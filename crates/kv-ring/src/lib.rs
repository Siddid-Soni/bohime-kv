//! Consistent hashing + shard map (plan §1.11, built at M10). Keys hash onto
//! a fixed number of shards; the `shard -> [replica nodes]` map is versioned
//! and replicated by the meta Raft group, then published for reads via
//! `ArcSwap` (deliberately not `left-right` — see plan §1.15).

pub mod hash;
pub mod map;
pub mod ring;

/// A shard number, as it appears in the map and on the wire.
///
/// `u16` rather than `u32`: the shard count is fixed at cluster creation and
/// 65536 shards on one cluster is already far past the point where a shard's
/// Raft group costs more than it carries. Narrow enough that
/// `transport::group::shard` can promise no shard's group id collides with
/// another's.
pub type ShardId = u16;

pub use crate::hash::{hash64, shard_for};
pub use crate::map::{MapError, ShardMap};
pub use crate::ring::{DEFAULT_VNODES, PlacementError, Ring};

#[cfg(test)]
mod tests;
