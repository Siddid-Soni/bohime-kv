//! Where a key belongs, and where to send one this node does not hold
//! (M11.6).
//!
//! Two steps, deliberately different in kind. **Key → shard** is
//! `hash(key) % num_shards` and never touches the ring: that is what makes it
//! stable across restarts and across membership changes, which is M10's first
//! ✅. **Shard → nodes** is the replicated map, so every node routes by a
//! placement the cluster agreed on rather than one it computed locally — two
//! nodes computing their own rings during a membership change would disagree
//! about who owns shard 37, and a client routed by one would write to a group
//! the other does not consider authoritative.
//!
//! A pure function over the published map, so the decision is testable without
//! a cluster and cheap enough to make on every request.

use kv_raft::NodeId;
use kv_ring::{ShardId, ShardMap};

use crate::transport::group::{self, GroupId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// This node replicates the key's shard. Whether it *leads* that shard is
    /// a separate question, answered by the group itself.
    Local { shard: ShardId, group: GroupId },
    /// Somebody else's: the shard's replica set, in ring order, so a client
    /// has somewhere to go rather than a scan to perform.
    Elsewhere { shard: ShardId, replicas: Vec<NodeId> },
    /// No map has been published here yet. Distinct from `Elsewhere` because
    /// there is nowhere to point the client: this node does not know where the
    /// key belongs, and a cluster is only in this state between starting and
    /// its meta group's first election.
    NoMap,
}

/// Resolves `key` against the placement `me` currently believes in.
pub fn route(map: Option<&ShardMap>, me: NodeId, key: &[u8]) -> Route {
    let Some(map) = map else {
        return Route::NoMap;
    };
    let shard = map.shard_for_key(key);
    if map.holds(me, shard) {
        Route::Local { shard, group: group::shard(shard) }
    } else {
        Route::Elsewhere { shard, replicas: map.replicas(shard).to_vec() }
    }
}
