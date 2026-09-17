//! Raft group ids (M10.4).
//!
//! From M10 a process hosts more than one `RaftNode`: the meta group, holding
//! the shard map, beside the data group. They share a process, a listener and
//! every peer address, so the group id on the wire is the only thing that
//! tells an AppendEntries for the shard map apart from one for user data.
//! Getting it wrong is not a dropped message — Raft tolerates those — but a
//! message applied to the wrong log.
//!
//! This is deliberately *not* in `kv-raft`. A `RaftNode` has no idea it is one
//! of several; hosting and routing are the shell's problem, which is the same
//! boundary that keeps the core simulatable.

use kv_ring::ShardId;

/// A group id as it travels on the wire.
pub type GroupId = u32;

/// Reserved, never routable.
///
/// proto3 gives an absent field its zero value, so a sender that forgot to
/// set the group is indistinguishable from one that meant group 0. Burning
/// the value turns that into a loud rejection instead of a plausible-looking
/// delivery to whichever group sorted first — the same trade M5 made when it
/// gave the conflict hints `optional` rather than a sentinel 0, and M9 made
/// when it reserved node id 0 for "no leader known".
pub const UNSET: GroupId = 0;

/// The meta group: one small Raft group whose state machine is the shard map.
///
/// Fixed rather than derived, because everything else needs to find it before
/// it has read anything — including the map that would otherwise name it.
pub const META: GroupId = 1;

/// The group replicating `shard`.
///
/// Offset by two so that neither [`UNSET`] nor [`META`] can collide with
/// shard 0. `u16::MAX + 2` fits a `u32` with room to spare, so no shard
/// number wraps onto another's group.
pub const fn shard(shard: u16) -> GroupId {
    shard as GroupId + 2
}

/// The shard a group replicates, or `None` for [`UNSET`] and [`META`].
///
/// The inverse of [`shard`]. A group id above the `u16` range cannot name a
/// shard, which is a malformed sender rather than a shard this process has
/// not heard of.
pub const fn shard_of(group: GroupId) -> Option<ShardId> {
    if group == UNSET || group == META {
        return None;
    }
    let shard = group - 2;
    if shard > ShardId::MAX as GroupId { None } else { Some(shard as ShardId) }
}

/// Shard 0's group.
///
/// Through M10 this was *the* data group, numbered as shard 0's so that M11
/// would inherit the numbering rather than rename it. From M11 it is an
/// ordinary shard group and nothing about it is special; the name survives
/// only where a test means "some shard's group", hence `cfg(test)`: a
/// production call site naming "the data group" would be naming something
/// that no longer exists.
#[cfg(test)]
pub const DATA: GroupId = shard(0);
