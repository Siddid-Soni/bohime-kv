//! The shard map where it lives on disk: inside the meta group's state
//! machine (M10.6).
//!
//! One key, one bincode blob. The meta group is a **versioned register**, not
//! a state machine that understands placement: the ring math is a pure
//! function in `kv-ring`, and what Raft replicates is its output. That keeps
//! `Driver::apply` unchanged — a map update is an ordinary `Cas` — and it
//! makes the ordering of concurrent proposals a property of M7's
//! compare-and-swap rather than of a lock nobody holds.
//!
//! The key sits in the reserved `\x00` space beside the session table and the
//! peer address book, so the service-boundary check that stops a client
//! forging a session entry also stops one rewriting cluster placement. Living
//! in the state machine is also what makes it survive: a snapshot carries it,
//! and a restart replays it, exactly as M7 argued for sessions and M9 for
//! addresses.

use kv_ring::ShardMap;
use kv_storage::Engine;

use crate::config::NodeConfig;

/// Reserved, and deliberately not under a `/` prefix: there is exactly one
/// shard map, not a family of them.
pub const SHARD_MAP_KEY: &[u8] = b"\x00shardmap";

/// The stored map's bytes, or `None` before the meta group has bootstrapped
/// one.
///
/// Bytes rather than a decoded `ShardMap` because every caller needs them: the
/// `Cas` that replaces the map needs them for `expected`, and comparing
/// decoded values there would be comparing something the log never saw.
pub fn encoded(engine: &Engine) -> anyhow::Result<Option<Vec<u8>>> {
    Ok(engine.get(SHARD_MAP_KEY)?)
}

/// This node's flags disagree with the cluster's stored placement.
///
/// Fatal rather than a warning. `--shards` and `--replication-factor` seed
/// version 1 and are owned by the map afterwards, so a node arriving with
/// different values is not out of date — it is wrong, and `hash % 256` and
/// `hash % 512` agree about almost no key, so serving anyway would misroute
/// the entire keyspace without a single error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "--{parameter} is {ours} on this node but the cluster's shard map was built with {stored}; \
     it is fixed at cluster creation and cannot be changed by restarting a node"
)]
pub struct ParamMismatch {
    pub parameter: &'static str,
    pub ours: u32,
    pub stored: u32,
}

/// Checks this node's placement flags against the map the cluster actually
/// agreed on.
pub fn check_params(config: &NodeConfig, map: &ShardMap) -> Result<(), ParamMismatch> {
    let checks = [
        ("shards", config.num_shards as u32, map.num_shards as u32),
        ("replication-factor", config.replication_factor as u32, map.replication_factor as u32),
        ("vnodes", config.vnodes_per_node, map.vnodes_per_node),
    ];
    for (parameter, ours, stored) in checks {
        if ours != stored {
            return Err(ParamMismatch { parameter, ours, stored });
        }
    }
    Ok(())
}
