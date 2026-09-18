//! Founding and hosting this node's shard groups (M11.5).
//!
//! **Why this is a task and not part of the driver.** Opening a shard means
//! opening two Bitcask instances and replaying or hint-loading both keydirs. A
//! node replicating 154 of 256 shards does that 308 times. On the driver loop
//! it would stall every group already ticking for the whole run — long enough
//! to lose leadership on all of them — so founding happens here, off that
//! loop, and a finished [`Group`] is handed over through a channel.
//!
//! **Why founding is a one-time act.** See [`crate::placement`]: every replica
//! of shard 7 derives the same voter set from the same map version, which is
//! what lets a shard group be founded with no conf change — and that argument
//! collapses the moment two nodes found from different versions. So a node
//! founds from version 1 and never again, and a shard moving to a node that
//! has never held it is a data move, which is M12's.

use std::collections::BTreeSet;
use std::time::Duration;

use kv_raft::RaftNode;
use kv_ring::{ShardId, ShardMap};
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::config::NodeConfig;
use crate::driver::{AdminOp, AdminReply, AdminRequest, Group};
use crate::meta::PublishedMap;
use crate::placement;
use crate::storage::BitcaskStorage;
use crate::transport::group::{self, GroupId};
use crate::transport::server::{GroupRegistry, Inbound};

/// How often the supervisor re-examines the map against what it hosts.
///
/// Slow by design, like the meta reconciler's: founding happens once, and
/// after that every pass is a set comparison that finds nothing.
pub const SUPERVISE_INTERVAL: Duration = Duration::from_millis(500);

pub struct ShardSupervisor {
    config: NodeConfig,
    published: PublishedMap,
    registry: GroupRegistry,
    /// The shard driver's inbox, registered under each group so the
    /// `RaftService` can route to it.
    inbox: mpsc::Sender<Inbound>,
    /// The shard driver's admin channel, for asking it to let a group go.
    ///
    /// The supervisor owns the disk; the driver owns the group. Neither can
    /// delete a shard alone, and that split is what makes the two-witness
    /// rule structural rather than a condition somebody can delete.
    shard_admin: mpsc::Sender<AdminRequest>,
    groups: mpsc::Sender<Group>,
    interval: Duration,
    /// Shards already handed to the driver.
    hosted: BTreeSet<ShardId>,
    /// Shards reported as having moved away, so the warning is said once
    /// rather than twice a second forever.
    reported: BTreeSet<ShardId>,
}

impl ShardSupervisor {
    pub fn new(
        config: NodeConfig,
        published: PublishedMap,
        registry: GroupRegistry,
        inbox: mpsc::Sender<Inbound>,
        shard_admin: mpsc::Sender<AdminRequest>,
        groups: mpsc::Sender<Group>,
        interval: Duration,
    ) -> Self {
        Self {
            config,
            published,
            registry,
            inbox,
            shard_admin,
            groups,
            interval,
            hosted: BTreeSet::new(),
            reported: BTreeSet::new(),
        }
    }

    pub async fn run(mut self) {
        // What is already on disk comes up first and without consulting the
        // map: a group whose log is here is one this node is a member of as
        // far as Raft is concerned, whatever placement has since decided.
        match placement::hosted_on_disk(&self.config) {
            Ok(shards) => {
                for shard in shards {
                    self.host(shard, Origin::Reopen).await;
                }
            }
            Err(e) => tracing::error!(error = %e, "cannot read the shards on disk"),
        }

        loop {
            self.pass().await;
            tokio::time::sleep(self.interval).await;
        }
    }

    async fn pass(&mut self) {
        let Some(map) = self.published.load_full() else {
            // No placement yet. Nothing to found and nothing to compare
            // against; the meta group is still electing.
            return;
        };

        match placement::founded_at(&self.config) {
            Ok(None) => self.found(&map).await,
            Ok(Some(_)) => {}
            Err(e) => {
                tracing::error!(error = %e, "cannot read the founding marker");
                return;
            }
        }

        // Founding is a one-time act; adoption is not. Every pass after the
        // first is the migration's receiving end: a shard the map has since
        // placed here is one whose leader is about to add us as a learner,
        // and a Raft message for a group this process does not host is
        // answered `not_found` and retried. Hosting first means the retry
        // lands.
        self.adopt(&map).await;
        self.release_departed(&map).await;
    }

    /// Lets go of every shard the map has moved away, if its group agrees.
    ///
    /// Three steps in this order and no other: ask the driver to drop the
    /// group, which closes both Bitcask instances; stop routing to it; then
    /// unlink. Unlinking while the driver still holds it removes files two
    /// open engines are reading, and a node that crashed between the unlink
    /// and the drop would come back up hosting a group with no data.
    async fn release_departed(&mut self, map: &ShardMap) {
        let departing: Vec<ShardId> =
            self.hosted.iter().copied().filter(|&s| !map.holds(self.config.id, s)).collect();
        for shard in departing {
            let (reply, wait) = oneshot::channel();
            let request = AdminRequest {
                group: group::shard(shard),
                op: AdminOp::ReleaseShard { shard },
                reply,
            };
            if self.shard_admin.send(request).await.is_err() {
                tracing::error!(shard, "the shard driver is gone; not releasing");
                return;
            }
            match wait.await {
                Ok(AdminReply::Released { released: true }) => {}
                // The group still names this node. Ordinary between a map
                // change and the conf change that carries it out — this node
                // has not been removed from the group yet — and the next pass
                // asks again.
                Ok(_) => continue,
                Err(_) => return,
            }

            self.registry.unregister(group::shard(shard));
            self.hosted.remove(&shard);
            self.reported.remove(&shard);
            match std::fs::remove_dir_all(self.config.shard_dir(shard)) {
                Ok(()) => tracing::info!(shard, version = map.version, "released a shard"),
                Err(e) => tracing::error!(shard, error = %e, "cannot remove a released shard"),
            }
        }
    }

    /// Hosts every shard the map places here that we are not already hosting
    /// (M12.1).
    async fn adopt(&mut self, map: &ShardMap) {
        let incoming: Vec<ShardId> =
            map.shards_of(self.config.id).filter(|s| !self.hosted.contains(s)).collect();
        for shard in incoming {
            tracing::info!(shard, version = map.version, "adopting a shard the map placed here");
            self.host(shard, Origin::Adopt).await;
        }

        for shard in self.hosted.clone() {
            if !map.holds(self.config.id, shard) && self.reported.insert(shard) {
                tracing::warn!(shard, "this node hosts a shard the map has moved away");
            }
        }
    }

    /// Creates this node's shard groups, once, from the first map it sees.
    async fn found(&mut self, map: &ShardMap) {
        let me = self.config.id;

        if self.config.initial_learner {
            // A joining node founds nothing: its shards arrive by migration,
            // as learners added to groups that already exist. Founding from
            // the map it happens to see would create a second configuration
            // for a shard the incumbents are already running.
            tracing::info!(
                version = map.version,
                "joined an existing cluster; shards arrive by migration (M12)"
            );
            self.mark(map.version);
            return;
        }

        if map.version != 1 {
            // A founder that missed bootstrap. Its shards were placed by a
            // map it never saw, and founding from the current one would put a
            // different voter set on the same shard as its co-replicas.
            tracing::error!(
                version = map.version,
                "this node founds no shards: it has no placement marker but the cluster is \
                 already past version 1, so its shards must arrive by migration (M12)"
            );
            self.mark(map.version);
            return;
        }

        let shards: Vec<ShardId> = map.shards_of(me).collect();
        tracing::info!(count = shards.len(), version = map.version, "founding shard groups");
        for shard in shards {
            self.host(shard, Origin::Found { voters: map.replicas(shard).to_vec() }).await;
        }
        self.mark(map.version);
    }

    fn mark(&self, version: u64) {
        if let Err(e) = placement::record_founding(&self.config, version) {
            // Not fatal, and not silent: without the marker the next boot
            // would found again from whatever map it sees. Retried next pass.
            tracing::error!(error = %e, "cannot record the founding map version");
        }
    }

    /// Opens one shard and hands it to the driver.
    ///
    /// See [`Origin`] for what the three cases mean: founding names the
    /// shard's replica set, reopening takes what founding recorded, and
    /// adopting deliberately names nobody.
    async fn host(&mut self, shard: ShardId, origin: Origin) {
        if !self.hosted.insert(shard) {
            return;
        }
        let config = self.config.clone();
        let group = group::shard(shard);

        // Blocking: two `open`s, each replaying or hint-loading a keydir.
        let opened = tokio::task::spawn_blocking(move || open(&config, shard, group, origin))
            .await
            .expect("opening a shard does not panic");
        let group_state = match opened {
            Ok(group_state) => group_state,
            Err(e) => {
                tracing::error!(shard, error = %e, "cannot open shard");
                self.hosted.remove(&shard);
                return;
            }
        };

        // Routable before the driver holds it: a peer that dials during this
        // node's first tick on the group must find an inbox, not a gap. The
        // inbox is the driver's, and a message arriving before the group does
        // is dropped and retried, which is what a lost Raft message already
        // is.
        self.registry.register(group, self.inbox.clone());
        if self.groups.send(group_state).await.is_err() {
            tracing::error!(shard, "the shard driver is gone; not hosting");
            self.registry.unregister(group);
            self.hosted.remove(&shard);
        }
    }
}

/// Why this group is being opened, which is what decides its bootstrap
/// membership.
///
/// Three cases and no default, because the wrong one is silent. `Found` is the
/// only one that may name voters; `Reopen` takes what founding recorded;
/// `Adopt` deliberately takes **nothing**, because the membership of a group
/// this node is being added to belongs to that group's leader and arrives by
/// snapshot or by the conf entry that admits us.
#[derive(Debug, Clone)]
pub(crate) enum Origin {
    Found { voters: Vec<kv_raft::NodeId> },
    Adopt,
    Reopen,
}

/// Opens one shard's log and state machine and builds its group. Synchronous
/// and blocking — the caller runs it on a blocking thread.
pub(crate) fn open(
    config: &NodeConfig,
    shard: ShardId,
    group: GroupId,
    origin: Origin,
) -> anyhow::Result<Group> {
    let raft_dir = config.shard_raft_dir(shard);
    let state_dir = config.shard_state_dir(shard);
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::create_dir_all(&state_dir)?;

    let mut raft = config.raft_config_for(group);
    match origin {
        // The shard's replica set is the group's membership, with ourselves
        // filtered out the way `--peer` is — and recorded, because no conf
        // entry carries it and a reopen would otherwise fall back to flags.
        Origin::Found { voters } => {
            crate::placement::record_voters(config, shard, &voters)?;
            raft.peers = voters.iter().copied().filter(|id| *id != config.id).collect();
        }
        // Whatever founding recorded, never `--peer`, which is the whole
        // cluster where this group is three of it. No record means the group
        // was adopted rather than founded, and an adopted group's membership
        // is its leader's to supply — so it reopens the way it arrived.
        Origin::Reopen => match crate::placement::founding_voters(config, shard)? {
            Some(voters) if !voters.is_empty() => {
                raft.peers = voters.iter().copied().filter(|id| *id != config.id).collect();
            }
            _ => {
                raft.peers = Vec::new();
                raft.initial_learner = true;
            }
        },
        // The one that matters. `peers` empty and `initial_learner` true give
        // `ClusterConfig { voters: {}, learners: {me} }`: this group cannot
        // campaign (`kv-raft`'s campaign returns early for a non-voter) and
        // cannot vote (granting requires `is_voter(self)`). It sits inert
        // until its leader contacts it, which is the only correct state for a
        // node that does not yet know this group's membership. Taking the
        // flags instead would make a founder a voter of a config nobody
        // agreed on, and a voter campaigns.
        Origin::Adopt => {
            raft.peers = Vec::new();
            raft.initial_learner = true;
        }
    }

    let node = RaftNode::new(raft, BitcaskStorage::open_with_policy(&raft_dir, config.log_fsync)?);
    let engine = Engine::open_with_config(&state_dir, config.engine_config())?;
    Ok(Group::new(config, group, node, engine))
}
