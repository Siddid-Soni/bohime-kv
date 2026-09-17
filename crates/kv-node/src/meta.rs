//! The meta reconciler (M10.6): what keeps the shard map current, and what
//! publishes it.
//!
//! **It changes nothing in `Driver`.** Everything it needs already exists as a
//! channel: the meta group's `ClusterStatus` says who is in the cluster, its
//! `LocalShardMap` says what placement the cluster agreed on, and its request
//! channel takes a `Cas`. So this is a plain task composed over two senders,
//! and the driver stays the single-threaded loop M4's determinism rests on.
//!
//! Since M11 the cluster's membership *is* the meta group's membership. There
//! is no single data group to ask any more — there is one Raft group per shard
//! — so the small fixed group that records placement records who is in the
//! cluster too, and a node admitted after bootstrap joins it as a permanent
//! learner so that it replicates the map and can route.
//!
//! Every node runs one; only the meta leader's proposals land, because a
//! proposal to a follower comes back `NotLeader`. Two leaders' proposals
//! racing — which happens exactly at bootstrap, when every node sees the same
//! empty state at the same instant — are resolved by the compare-and-swap,
//! not by anyone holding a lock.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use kv_ring::ShardMap;
use tokio::sync::{mpsc, oneshot};

use crate::command::Mutation;
use crate::config::NodeConfig;
use crate::driver::{AdminOp, AdminReply, AdminRequest, ClientOp, ClientRequest};
use crate::shard_map::{self, SHARD_MAP_KEY};
use crate::transport::group;

/// How often a node re-examines the cluster against the map.
///
/// Placement changes when an operator adds a machine, so this is a slow loop
/// by design: the cost of noticing a second late is nothing, and the cost of
/// polling two drivers hard is paid on every node forever.
pub const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

/// The published map, shared with every request handler.
///
/// `ArcSwap` rather than `left-right` (§1.15): 256 entries replaced wholesale,
/// with no natural oplog to absorb. `None` until the meta group bootstraps.
pub type PublishedMap = Arc<ArcSwapOption<ShardMap>>;

pub struct MetaReconciler {
    config: NodeConfig,
    meta_admin: mpsc::Sender<AdminRequest>,
    meta_requests: mpsc::Sender<ClientRequest>,
    published: PublishedMap,
    interval: Duration,
}

impl MetaReconciler {
    pub fn new(
        config: NodeConfig,
        meta_admin: mpsc::Sender<AdminRequest>,
        meta_requests: mpsc::Sender<ClientRequest>,
        published: PublishedMap,
        interval: Duration,
    ) -> Self {
        Self { config, meta_admin, meta_requests, published, interval }
    }

    pub async fn run(self) {
        loop {
            self.reconcile().await;
            tokio::time::sleep(self.interval).await;
        }
    }

    /// One pass: read the agreed map, publish it, and propose a new one if the
    /// cluster has moved out from under it.
    async fn reconcile(&self) {
        let stored = match self.local_map().await {
            Some(bytes) => bytes,
            // The meta driver is gone, or its inbox is wedged. Nothing useful
            // to do but come back next interval.
            None => return,
        };

        match stored {
            None => self.bootstrap().await,
            Some(bytes) => match ShardMap::decode(&bytes) {
                Ok(map) => {
                    if let Err(mismatch) = shard_map::check_params(&self.config, &map) {
                        // Fatal on purpose. Serving with the wrong shard count
                        // would route every key to the wrong group and report
                        // nothing at all; stopping is the quieter failure.
                        tracing::error!(error = %mismatch, "refusing to serve a keyspace this node would misroute");
                        std::process::abort();
                    }
                    // Publish what the cluster agreed on *before* proposing
                    // anything: this node's handlers should serve the current
                    // placement even on a pass where it also decides the
                    // placement is out of date.
                    self.published.store(Some(Arc::new(map.clone())));
                    self.follow_cluster(&map, bytes).await;
                }
                Err(e) => tracing::error!(error = %e, "the replicated shard map does not decode"),
            },
        }
    }

    /// Republishes the map when the data cluster's voter set has moved.
    ///
    /// The comparison is on the node *set*, not on a diff: a node that missed
    /// a change while it was down must reach the same conclusion as one that
    /// watched every step, and a set comparison cannot drift the way a running
    /// tally can.
    ///
    /// Only when they differ — a reconciler that proposed on every pass would
    /// fill the meta log with versions that all say the same thing, and then
    /// snapshot them forever.
    async fn follow_cluster(&self, current: &ShardMap, current_bytes: Vec<u8>) {
        let Some(voters) = self.cluster_nodes().await else { return };
        let voters: std::collections::BTreeSet<_> = voters.into_iter().collect();
        if voters == current.nodes || voters.is_empty() {
            return;
        }

        let next = match current.with_nodes(voters) {
            Ok(next) => next,
            // Shrinking below the replication factor. Refused rather than
            // under-replicated: the old map stays published and correct, and
            // an operator sees why in the log.
            Err(e) => {
                tracing::warn!(error = %e, "cannot replace the shard map for the new cluster");
                return;
            }
        };

        let (from, to) = (current.version, next.version);
        if self.swap(Some(current_bytes), next.encode()).await {
            tracing::info!(from, to, "republished the shard map for a changed cluster");
        }
    }

    /// Creates version 1, if this node leads the meta group.
    ///
    /// `expected: None` is M7's "only if absent", which is what makes this
    /// safe to run on all three nodes at once: the first proposal to commit
    /// wins, and the rest see a value where they expected none and answer
    /// `Swapped(false)`.
    async fn bootstrap(&self) {
        let Some(nodes) = self.cluster_nodes().await else { return };
        if nodes.is_empty() {
            return;
        }

        let map = match ShardMap::build(
            1,
            nodes,
            self.config.num_shards,
            self.config.replication_factor,
            self.config.vnodes_per_node,
        ) {
            Ok(map) => map,
            // The cluster is smaller than the replication factor asks for.
            // Refused rather than under-replicated — the decision recorded in
            // the M10 plan — and retried, because a cluster that is still
            // starting will grow into it.
            Err(e) => {
                tracing::warn!(error = %e, "cannot place shards yet");
                return;
            }
        };

        let proposed = map.version;
        if self.swap(None, map.encode()).await {
            tracing::info!(version = proposed, "bootstrapped the shard map");
        }
    }

    /// Proposes `new` against `expected` through the meta group.
    ///
    /// Returns whether *this* proposal was the one that landed. `false` covers
    /// both losing the race and not being the leader, which are the same thing
    /// from here: someone else's version is in the log.
    async fn swap(&self, expected: Option<Vec<u8>>, new: Vec<u8>) -> bool {
        let op = ClientOp::Mutate {
            // No session context: the reconciler is idempotent by
            // construction — its `expected` already refuses a replay — and a
            // client id here would put a permanent entry in the session table
            // of every node, for a caller that never retries.
            ctx: None,
            op: Mutation::Cas { key: SHARD_MAP_KEY.to_vec(), expected, new_value: new },
        };
        let (reply, wait) = oneshot::channel();
        if self.meta_requests.send(ClientRequest { group: group::META, op, reply }).await.is_err() {
            return false;
        }
        matches!(wait.await, Ok(crate::driver::ClientReply::Swapped(true)))
    }

    /// This replica's applied copy of the map, as bytes.
    ///
    /// The outer `Option` is "could not ask"; the inner one is "no map yet".
    async fn local_map(&self) -> Option<Option<Vec<u8>>> {
        match self.ask_meta(AdminOp::LocalShardMap).await? {
            AdminReply::ShardMap(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Who is in the cluster — the set the ring is built from.
    ///
    /// The meta group's voters **and** its learners. Through M10 this was the
    /// data group's voters, and learners were excluded because a learner was
    /// still catching up on user data. Since M11 there is no data group, and a
    /// meta learner is a full cluster member that simply never votes on
    /// placement — excluding them would mean a node admitted after bootstrap
    /// never appeared in the map at all.
    async fn cluster_nodes(&self) -> Option<Vec<kv_raft::NodeId>> {
        let status = self.status().await?;
        Some(status.voters.into_iter().chain(status.learners).collect())
    }

    async fn status(&self) -> Option<crate::driver::ClusterStatus> {
        match self.ask_meta(AdminOp::Status).await? {
            AdminReply::Status(status) => Some(status),
            _ => None,
        }
    }

    async fn ask_meta(&self, op: AdminOp) -> Option<AdminReply> {
        let (reply, wait) = oneshot::channel();
        self.meta_admin.send(AdminRequest { group: group::META, op, reply }).await.ok()?;
        wait.await.ok()
    }
}
