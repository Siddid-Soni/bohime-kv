//! Moving a shard when placement changes (M12.1).
//!
//! **The map is the target; each group's own committed Raft config is the
//! state.** There is no migration plan recorded anywhere and no new
//! replicated structure: a shard is mid-migration exactly when its group's
//! config differs from `map.replicas(shard)`, which every node can already
//! see. The alternative — a `pending` list inside the `ShardMap`, which
//! `docs/DESIGN.md:290` anticipates — buys cluster-wide rate limiting and a
//! readable progress figure, at the cost of a second replicated structure
//! that has to agree with the first and a completion report from each shard's
//! leader to the meta leader, who is generally a different node. The
//! condition we need is derivable from two things every node already holds,
//! and deriving it is cheaper than replicating it.
//!
//! The four-step sequence §1.12 names is carried out by machinery M9 already
//! built: `AddNode` proposes `AddLearner`, `Group::maybe_promote` promotes it
//! once its `match_index` reaches the commit index, `RemoveNode` proposes
//! `RemoveVoter`, and a leader that removes itself hands off rather than
//! waiting out an election timeout. This module is the caller, not a second
//! implementation.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kv_raft::NodeId;
use kv_ring::{ShardId, ShardMap};
use tokio::sync::{mpsc, oneshot};

use crate::config::NodeConfig;
use crate::driver::{AdminOp, AdminReply, AdminRequest, ShardStatus};
use crate::meta::PublishedMap;
use crate::transport::group::{self, GroupId};

/// How often a node re-examines its shards against the map.
///
/// Slow by design, like the other two reconcilers': placement changes when an
/// operator adds a machine, and polling a driver hard is a cost every node
/// pays forever.
pub const MIGRATE_INTERVAL: Duration = Duration::from_millis(500);

/// One membership change on one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Add { shard: ShardId, node: NodeId },
    Remove { shard: ShardId, node: NodeId },
}

/// What this node should propose right now.
///
/// Pure, and deliberately so: the rules that make a rebalance safe are
/// properties of this decision rather than of the plumbing around it, and a
/// pure function lets a test state them without a cluster.
///
/// - **Only shards this node leads.** Every node runs a reconciler; one
///   acting on a shard it merely replicates would have two nodes proposing
///   the same conf change.
/// - **Add before remove, and only once every incoming replica votes.**
///   Removing first drops the group below its replication factor while the
///   replacement is still catching up — a rebalance manufacturing the
///   unavailability it exists to avoid.
/// - **At most `limit` shards, in shard-id order.** Divergence persists until
///   a move completes, so the same shards are chosen next pass and the
///   in-flight count is capped by construction: no bookkeeping of what is
///   outstanding, and a deterministic choice, which is what makes the limit
///   testable.
pub fn plan_step(map: &ShardMap, statuses: &[ShardStatus], me: NodeId, limit: usize) -> Vec<Step> {
    debug_assert!(
        statuses.iter().all(|s| !s.leading || s.leader == Some(me)),
        "a status claims to lead but names another leader"
    );

    // Sorted, because the limit below takes a prefix and "the first four" has
    // to mean the same four on every pass.
    let mut by_shard: BTreeMap<ShardId, &ShardStatus> = BTreeMap::new();
    for status in statuses {
        by_shard.insert(status.shard, status);
    }

    let mut steps = Vec::new();
    for (&shard, status) in &by_shard {
        if steps.len() >= limit {
            break;
        }
        if !status.leading || shard >= map.num_shards {
            continue;
        }
        let target = map.replicas(shard);

        let missing = target
            .iter()
            .copied()
            .find(|id| !status.replicas.contains(id) && !status.learners.contains(id));
        if let Some(node) = missing {
            steps.push(Step::Add { shard, node });
            continue;
        }

        // Every node the map names is present. Only once they all *vote* may
        // anything be taken out: a learner counts toward no quorum, so
        // removing a voter while one is still catching up shrinks the group
        // rather than replacing a member of it.
        if target.iter().any(|id| !status.replicas.contains(id)) {
            continue;
        }
        let surplus = status
            .replicas
            .iter()
            .chain(status.learners.iter())
            .copied()
            .find(|id| !target.contains(id));
        if let Some(node) = surplus {
            steps.push(Step::Remove { shard, node });
        }
    }
    steps
}

/// Shards this node leads whose group no longer matches the map — what an
/// operator watching a rebalance wants to see reach zero.
fn diverging(map: &ShardMap, statuses: &[ShardStatus]) -> usize {
    statuses
        .iter()
        .filter(|s| s.leading && s.shard < map.num_shards)
        .filter(|s| {
            let target: BTreeSet<NodeId> = map.replicas(s.shard).iter().copied().collect();
            let held: BTreeSet<NodeId> =
                s.replicas.iter().chain(s.learners.iter()).copied().collect();
            target != held
        })
        .count()
}

/// Closes the difference between the map and this node's shard groups.
///
/// One per node, like the meta reconciler and the shard supervisor. A node
/// acts only on shards it leads, so the work partitions itself across the
/// cluster with nothing to coordinate, and a node that leads nothing does
/// nothing.
pub struct MigrationDriver {
    config: NodeConfig,
    shard_admin: mpsc::Sender<AdminRequest>,
    /// The meta group's admin channel, for the address book. A shard group's
    /// own book holds that group's members, and the node being added is by
    /// definition not one of them yet.
    meta_admin: mpsc::Sender<AdminRequest>,
    published: PublishedMap,
    interval: Duration,
}

impl MigrationDriver {
    pub fn new(
        config: NodeConfig,
        shard_admin: mpsc::Sender<AdminRequest>,
        meta_admin: mpsc::Sender<AdminRequest>,
        published: PublishedMap,
        interval: Duration,
    ) -> Self {
        Self { config, shard_admin, meta_admin, published, interval }
    }

    /// `&self` rather than `self`: the admin service holds the same driver, to
    /// answer `Rebalance` with a pass run now, so both callers share an `Arc`.
    pub async fn run_loop(&self) {
        loop {
            self.pass().await;
            tokio::time::sleep(self.interval).await;
        }
    }

    /// One pass. Returns how many shards this node leads that differ from the
    /// map.
    pub async fn pass(&self) -> usize {
        let Some(map) = self.published.load_full() else { return 0 };
        let Some(statuses) = self.shard_statuses().await else { return 0 };
        let steps = plan_step(&map, &statuses, self.config.id, self.config.max_migrations);
        let outstanding = diverging(&map, &statuses);

        for step in steps {
            self.propose(step).await;
        }
        outstanding
    }

    async fn propose(&self, step: Step) {
        let (shard, op) = match step {
            Step::Add { shard, node } => {
                let Some(address) = self.address_of(node).await else {
                    // A cluster member with no address in the meta group's
                    // book. Said and skipped: committing a conf entry naming
                    // a node nobody can dial admits a member that can never
                    // be reached.
                    tracing::warn!(shard, node, "no address for an incoming replica; not adding");
                    return;
                };
                (shard, AdminOp::AddNode { id: node, address })
            }
            Step::Remove { shard, node } => (shard, AdminOp::RemoveNode { id: node }),
        };

        let (reply, wait) = oneshot::channel();
        let request = AdminRequest { group: group::shard(shard), op, reply };
        if self.shard_admin.send(request).await.is_err() {
            return;
        }
        match wait.await {
            Ok(AdminReply::Accepted { index }) => {
                tracing::info!(?step, index, "proposed a migration step");
            }
            // Lost leadership between the status and the proposal, or another
            // conf change is still in flight on this group. Both resolve
            // themselves, and the next pass looks again.
            Ok(AdminReply::NotLeader { .. }) | Ok(AdminReply::Rejected { .. }) => {}
            Ok(other) => {
                tracing::error!(?step, ?other, "the shard driver answered the wrong reply");
            }
            Err(_) => {}
        }
    }

    async fn shard_statuses(&self) -> Option<Vec<ShardStatus>> {
        match self.ask(&self.shard_admin, group::UNSET, AdminOp::ShardStatuses).await? {
            AdminReply::ShardStatuses(statuses) => Some(statuses),
            _ => None,
        }
    }

    async fn address_of(&self, node: NodeId) -> Option<String> {
        match self.ask(&self.meta_admin, group::META, AdminOp::Status).await? {
            AdminReply::Status(status) => status.endpoints.get(&node).cloned(),
            _ => None,
        }
    }

    async fn ask(
        &self,
        channel: &mpsc::Sender<AdminRequest>,
        group: GroupId,
        op: AdminOp,
    ) -> Option<AdminReply> {
        let (reply, wait) = oneshot::channel();
        channel.send(AdminRequest { group, op, reply }).await.ok()?;
        wait.await.ok()
    }
}
