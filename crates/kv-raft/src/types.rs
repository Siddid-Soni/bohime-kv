//! The vocabulary of the Raft core (M3.1). No behavior — these are the types
//! `RaftStorage`, `Message`, `Action`, and `Ready` are all expressed in.
//!
//! Log indices start at 1. Index 0 is the sentinel meaning "before the log
//! begins": `term(0)` is 0 and an empty log's `last_index()` is 0, so
//! `prev_log_index = 0` in an AppendEntries means "no predecessor" without a
//! special case anywhere.

use serde::{Deserialize, Serialize};

use crate::membership::ClusterConfig;

pub type Term = u64;
pub type LogIndex = u64;
pub type NodeId = u64;

/// One entry in the replicated log. `command` is opaque here — interpreting
/// it is the state machine's job, not Raft's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

/// The state that must survive a crash for Raft to stay safe (§1.5). Losing
/// `voted_for` lets a node vote twice in one term, which breaks election
/// safety — so this is written before any vote is granted, not after.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    pub term: Term,
    /// `None` means "has not voted this term", which is distinct from having
    /// voted for node 0. A sentinel value here would be a real bug.
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
}

/// A state machine snapshot replacing the log prefix up to and including
/// `last_included_index`. Produced and consumed at M8; the membership at M9.
///
/// `config` is the cluster as of the boundary: conf entries at or below it
/// are gone with the prefix, so without this a node that compacted past its
/// last conf change would forget who votes. A node that restarts replays the
/// log over this base the same way it replays state over `data`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub data: Vec<u8>,
    pub config: ClusterConfig,
}
