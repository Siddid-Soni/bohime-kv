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
use tokio::sync::mpsc;

use crate::config::NodeConfig;
use crate::driver::Group;
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
    groups: mpsc::Sender<Group>,
    interval: Duration,
    /// Shards already handed to the driver.
    hosted: BTreeSet<ShardId>,
    /// Placement disagreements already reported, so the warning is said once
    /// rather than twice a second forever.
    reported: BTreeSet<ShardId>,
}

impl ShardSupervisor {
    pub fn new(
        config: NodeConfig,
        published: PublishedMap,
        registry: GroupRegistry,
        inbox: mpsc::Sender<Inbound>,
        groups: mpsc::Sender<Group>,
        interval: Duration,
    ) -> Self {
        Self {
            config,
            published,
            registry,
            inbox,
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
                    self.host(shard, None).await;
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
            Ok(Some(_)) => self.report_divergence(&map),
            Err(e) => tracing::error!(error = %e, "cannot read the founding marker"),
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
            self.host(shard, Some(map)).await;
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

    /// Says once, per shard, where the map and this node disagree.
    ///
    /// Both directions are ordinary between a placement change and the
    /// migration that carries it out, and both are M12's to resolve — but an
    /// operator staring at a shard that is not where the map says needs to
    /// see it named.
    fn report_divergence(&mut self, map: &ShardMap) {
        for shard in map.shards_of(self.config.id) {
            if !self.hosted.contains(&shard) && self.reported.insert(shard) {
                tracing::warn!(shard, "the map places this shard here but it is not hosted (M12)");
            }
        }
        for &shard in &self.hosted {
            if !map.holds(self.config.id, shard) && self.reported.insert(shard) {
                tracing::warn!(shard, "this node hosts a shard the map has moved away (M12)");
            }
        }
    }

    /// Opens one shard and hands it to the driver.
    ///
    /// `map` is `Some` when founding — the group's initial voters are that
    /// shard's replica set — and `None` when reopening a shard already on
    /// disk, where the log and its snapshot own the membership and the
    /// bootstrap config is history.
    async fn host(&mut self, shard: ShardId, map: Option<&ShardMap>) {
        if !self.hosted.insert(shard) {
            return;
        }
        let voters: Vec<kv_raft::NodeId> =
            map.map(|m| m.replicas(shard).to_vec()).unwrap_or_default();
        let config = self.config.clone();
        let group = group::shard(shard);

        // Blocking: two `open`s, each replaying or hint-loading a keydir.
        let opened = tokio::task::spawn_blocking(move || open(&config, shard, group, &voters))
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

/// Opens one shard's log and state machine and builds its group. Synchronous
/// and blocking — the caller runs it on a blocking thread.
fn open(
    config: &NodeConfig,
    shard: ShardId,
    group: GroupId,
    voters: &[kv_raft::NodeId],
) -> anyhow::Result<Group> {
    let raft_dir = config.shard_raft_dir(shard);
    let state_dir = config.shard_state_dir(shard);
    std::fs::create_dir_all(&raft_dir)?;
    std::fs::create_dir_all(&state_dir)?;

    let mut raft = config.raft_config_for(group);
    if !voters.is_empty() {
        // Founding: the shard's replica set is the group's membership, with
        // ourselves filtered out the way `--peer` is.
        raft.peers = voters.iter().copied().filter(|id| *id != config.id).collect();
    }
    let node = RaftNode::new(raft, BitcaskStorage::open(&raft_dir)?);
    let engine = Engine::open_with_config(&state_dir, config.engine_config())?;
    Ok(Group::new(config, group, node, engine))
}
