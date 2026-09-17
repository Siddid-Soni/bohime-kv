//! The ring: which nodes hold a shard (M10.2).
//!
//! Physical nodes are placed on a 64-bit hash ring at several positions each
//! (virtual nodes). A shard hashes to its own position; walking clockwise and
//! collecting `replication_factor` **distinct physical** nodes gives that
//! shard's replica set, which is that shard's Raft group.
//!
//! **Why virtual nodes.** With one position per node the ring is carved into
//! N arcs of wildly uneven length — the largest is typically several times
//! the mean — so one node would hold several times its share of shards, and a
//! shard's replicas are where its Raft group's disk and network load lives.
//! ~128 positions per node averages that unevenness away.
//!
//! **Why this is a pure function of the node set.** Two nodes that learned of
//! the same membership through different sequences of conf changes must
//! compute the same placement, or they disagree about who owns a shard. So
//! the vnode table is rebuilt from scratch and sorted on every change rather
//! than patched incrementally, and ties are broken by node id. Rebuilding is
//! `O(N·V·log N·V)` over a few hundred entries, on an event that happens when
//! an operator adds a machine.

use std::collections::BTreeSet;

use kv_raft::NodeId;

use crate::hash::hash64;

/// Positions per physical node. The balance tests are written against this.
pub const DEFAULT_VNODES: u32 = 128;

/// Separate hash namespaces for the two kinds of ring position. Without them
/// a shard and a vnode derived from the same underlying integer would sit at
/// the same place, correlating placement with node id in a way that is hard
/// to see and harder to debug.
const VNODE_SALT: &[u8] = b"bohime/vnode/";
const SHARD_SALT: &[u8] = b"bohime/shard/";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// The cluster is too small for the requested replication factor. Refused
    /// rather than under-replicated: a config that cannot be satisfied should
    /// fail where an operator sees it, not degrade silently into a promise
    /// the cluster is not keeping.
    #[error(
        "replication factor {replication_factor} needs {replication_factor} nodes, ring has {nodes}"
    )]
    NotEnoughNodes { replication_factor: u8, nodes: usize },
    #[error("a replication factor of zero would place no replicas")]
    ZeroReplicationFactor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ring {
    vnodes_per_node: u32,
    nodes: BTreeSet<NodeId>,
    /// Every vnode as `(position, owner)`, sorted. Ties broken by node id so
    /// two colliding positions still order deterministically.
    vnodes: Vec<(u64, NodeId)>,
}

fn vnode_position(node: NodeId, index: u32) -> u64 {
    let mut bytes = VNODE_SALT.to_vec();
    bytes.extend_from_slice(&node.to_be_bytes());
    bytes.extend_from_slice(&index.to_be_bytes());
    hash64(&bytes)
}

fn shard_position(shard: u16) -> u64 {
    let mut bytes = SHARD_SALT.to_vec();
    bytes.extend_from_slice(&shard.to_be_bytes());
    hash64(&bytes)
}

impl Ring {
    pub fn new(vnodes_per_node: u32) -> Self {
        assert!(vnodes_per_node > 0, "a node with no vnodes can hold nothing");
        Self { vnodes_per_node, nodes: BTreeSet::new(), vnodes: Vec::new() }
    }

    pub fn with_nodes(nodes: impl IntoIterator<Item = NodeId>, vnodes_per_node: u32) -> Self {
        let mut ring = Self::new(vnodes_per_node);
        ring.nodes = nodes.into_iter().collect();
        ring.rebuild();
        ring
    }

    pub fn nodes(&self) -> &BTreeSet<NodeId> {
        &self.nodes
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn add_node(&mut self, id: NodeId) {
        if self.nodes.insert(id) {
            self.rebuild();
        }
    }

    pub fn remove_node(&mut self, id: NodeId) {
        if self.nodes.remove(&id) {
            self.rebuild();
        }
    }

    /// Replaces the membership wholesale. The reconciler uses this: it learns
    /// the data cluster's voter set as a set, not as a diff.
    pub fn set_nodes(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        let nodes: BTreeSet<_> = nodes.into_iter().collect();
        if nodes != self.nodes {
            self.nodes = nodes;
            self.rebuild();
        }
    }

    fn rebuild(&mut self) {
        self.vnodes.clear();
        self.vnodes.reserve(self.nodes.len() * self.vnodes_per_node as usize);
        for &node in &self.nodes {
            for index in 0..self.vnodes_per_node {
                self.vnodes.push((vnode_position(node, index), node));
            }
        }
        self.vnodes.sort_unstable();
    }

    /// The replica set for a shard, in ring order — the first entry is the
    /// one the ring would call primary, though Raft elects its own leader and
    /// does not consult this.
    pub fn place(&self, shard: u16, replication_factor: u8) -> Result<Vec<NodeId>, PlacementError> {
        if replication_factor == 0 {
            return Err(PlacementError::ZeroReplicationFactor);
        }
        if replication_factor as usize > self.nodes.len() {
            return Err(PlacementError::NotEnoughNodes {
                replication_factor,
                nodes: self.nodes.len(),
            });
        }

        let position = shard_position(shard);
        let start = self.vnodes.partition_point(|&(p, _)| p < position);

        let wanted = replication_factor as usize;
        let mut replicas = Vec::with_capacity(wanted);
        for offset in 0..self.vnodes.len() {
            let (_, node) = self.vnodes[(start + offset) % self.vnodes.len()];
            // Linear, but `wanted` is a single digit in every real config —
            // cheaper than a set, and it preserves ring order.
            if !replicas.contains(&node) {
                replicas.push(node);
                if replicas.len() == wanted {
                    break;
                }
            }
        }

        debug_assert_eq!(
            replicas.len(),
            wanted,
            "the walk covers every vnode, so it sees every node at least once"
        );
        Ok(replicas)
    }
}
