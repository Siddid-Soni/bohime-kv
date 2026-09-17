//! The versioned shard map (M10.3): the ring's materialized output.
//!
//! The [`Ring`] is a pure function of the node set, so in principle every node
//! could derive placement locally and this type would be redundant. It is not,
//! for one reason: nodes learn of a membership change at *different instants*.
//! During that window two nodes deriving locally compute different rings,
//! disagree about who owns a shard, and a client routed by one writes to a
//! group the other does not consider authoritative — split brain, but for
//! placement.
//!
//! So the ring computes and the meta Raft group *records*. Every node flips
//! from version 7 to version 8 in a Raft-ordered way, and version 8 is also
//! where M12 will be able to write down "shard 37 is mid-migration".
//!
//! `num_shards`, `replication_factor` and `vnodes_per_node` live *in* the map
//! rather than in each node's config. They are fixed at bootstrap: changing
//! `num_shards` would remap nearly every key (`hash % 256` and `hash % 512`
//! agree about almost nothing), so a node whose flags disagree with the
//! replicated map is misconfigured, not merely out of date.

use std::collections::BTreeSet;

use kv_raft::NodeId;
use serde::{Deserialize, Serialize};

use crate::hash::shard_for;
use crate::ring::{PlacementError, Ring};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MapError {
    #[error(transparent)]
    Placement(#[from] PlacementError),
    #[error("a cluster needs at least one shard")]
    ZeroShards,
}

#[derive(Debug, thiserror::Error)]
#[error("undecodable shard map: {0}")]
pub struct DecodeError(#[from] bincode::Error);

/// A placement decision, version-stamped. `Eq` compares every field, so two
/// maps are equal only if they would route every key identically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMap {
    /// Bumped on every rebuild. The meta group's `Cas` is what actually
    /// serializes concurrent proposals; this is what makes the ordering
    /// legible to an operator.
    pub version: u64,
    pub num_shards: u16,
    pub replication_factor: u8,
    pub vnodes_per_node: u32,
    /// The cluster this map was computed for. Held explicitly rather than
    /// recovered from `shards`, so that a node holding no shard is still
    /// visibly a member.
    pub nodes: BTreeSet<NodeId>,
    /// `shards[i]` is shard `i`'s replica set in ring order. Length is
    /// always `num_shards`.
    pub shards: Vec<Vec<NodeId>>,
}

impl ShardMap {
    pub fn build(
        version: u64,
        nodes: impl IntoIterator<Item = NodeId>,
        num_shards: u16,
        replication_factor: u8,
        vnodes_per_node: u32,
    ) -> Result<Self, MapError> {
        if num_shards == 0 {
            return Err(MapError::ZeroShards);
        }
        let ring = Ring::with_nodes(nodes, vnodes_per_node);
        let shards = (0..num_shards)
            .map(|shard| ring.place(shard, replication_factor))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            version,
            num_shards,
            replication_factor,
            vnodes_per_node,
            nodes: ring.nodes().clone(),
            shards,
        })
    }

    /// The next version of this map for a different cluster. The shard count,
    /// replication factor and vnode count come from `self`, never from the
    /// caller — they were settled at bootstrap.
    pub fn with_nodes(&self, nodes: impl IntoIterator<Item = NodeId>) -> Result<Self, MapError> {
        Self::build(
            self.version + 1,
            nodes,
            self.num_shards,
            self.replication_factor,
            self.vnodes_per_node,
        )
    }

    pub fn shard_for_key(&self, key: &[u8]) -> u16 {
        shard_for(key, self.num_shards)
    }

    pub fn replicas(&self, shard: u16) -> &[NodeId] {
        &self.shards[shard as usize]
    }

    pub fn replicas_for_key(&self, key: &[u8]) -> &[NodeId] {
        self.replicas(self.shard_for_key(key))
    }

    /// Whether `node` holds a replica of `shard` — the question M11's router
    /// asks on every request.
    pub fn holds(&self, node: NodeId, shard: u16) -> bool {
        self.replicas(shard).contains(&node)
    }

    /// Every shard `node` replicates.
    pub fn shards_of(&self, node: NodeId) -> impl Iterator<Item = u16> + '_ {
        (0..self.num_shards).filter(move |&shard| self.holds(node, shard))
    }

    /// Deterministic: this is the `expected` side of the meta group's
    /// compare-and-swap, so two encodings of equal maps must be equal bytes.
    /// Every field is either a fixed-width integer or an ordered collection,
    /// which is what makes that true.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("a ShardMap always serializes")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(bincode::deserialize(bytes)?)
    }
}
