//! The wire vocabulary of the Raft core (M3.1). No behavior — `node.rs`
//! dispatches on these, `kv-node` converts `Message` to proto in `convert.rs`
//! (M5) so prost types never leak in here.

use serde::{Deserialize, Serialize};

use crate::types::{Entry, HardState, LogIndex, NodeId, Term};

/// Static configuration of one Raft group member. `seed` drives election
/// timeout randomization deterministically — never `thread_rng`, or M4's
/// reproducibility is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    /// Base election timeout in `tick()` units. The actual timeout is drawn
    /// uniformly from `[election_timeout, 2 * election_timeout)`.
    pub election_timeout: u64,
    /// Ticks between leader heartbeats. Must be well below `election_timeout`.
    pub heartbeat_interval: u64,
    pub seed: u64,
}

impl Config {
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    pub fn all_nodes(&self) -> Vec<NodeId> {
        let mut nodes = vec![self.id];
        nodes.extend(self.peers.iter().copied());
        nodes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Every RPC Raft sends, in-memory form. Conversion to proto lives in
/// `kv-node` (M5); bincode round-trip is tested here so the shape is pinned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteResp {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    },
    AppendEntriesResp {
        term: Term,
        success: bool,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
    },
    InstallSnapshotResp {
        term: Term,
        success: bool,
    },
}

impl Message {
    /// The sender's term — every handler routes through the term rules first.
    pub fn term(&self) -> Term {
        match *self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResp { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResp { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotResp { term, .. } => term,
        }
    }
}

/// One unit of work `tick`/`step`/`propose` asks its caller to perform.
/// The core never sends, fsyncs, or applies — it only describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send { to: NodeId, msg: Message },
    PersistEntries(Vec<Entry>),
    PersistHardState(HardState),
    ApplyEntries { up_to: LogIndex },
}

/// A read that cleared the leadership check (M7). Always empty at M3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadState {
    pub token: u64,
    pub index: LogIndex,
}

/// The drained accumulation of everything pending. `kv-node` executes it as:
/// persist entries, persist hard state, send messages, apply committed — in
/// that order (§1.5: disk before network).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Ready {
    pub messages: Vec<(NodeId, Message)>,
    pub entries: Vec<Entry>,
    pub hard_state: Option<HardState>,
    pub committed: Vec<Entry>,
    pub read_states: Vec<ReadState>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
            && self.entries.is_empty()
            && self.hard_state.is_none()
            && self.committed.is_empty()
            && self.read_states.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    /// Only the leader accepts proposals. Callers find the leader via
    /// `NotLeader` hints on the client path (M6) or `kv-sim`'s `leader()` (M4).
    NotLeader,
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NotLeader => write!(f, "not the leader"),
        }
    }
}

impl std::error::Error for ProposeError {}

#[cfg(test)]
#[path = "tests/message.rs"]
mod tests;
