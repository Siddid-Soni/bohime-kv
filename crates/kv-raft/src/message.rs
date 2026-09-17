//! The wire vocabulary of the Raft core (M3.1). No behavior — `node.rs`
//! dispatches on these, `kv-node` converts `Message` to proto in `convert.rs`
//! (M5) so prost types never leak in here.

use serde::{Deserialize, Serialize};

use crate::membership::ClusterConfig;
use crate::types::{Entry, HardState, LogIndex, NodeId, Snapshot, Term};

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
    /// True when this node joins an existing group as a learner (M9): it
    /// starts in `peers`' voters' shadow — replicating, never campaigning —
    /// until someone proposes promoting it. False for founding members.
    /// Only read on a fresh store; once the log or snapshot holds membership,
    /// they own it and this flag is history.
    pub initial_learner: bool,
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
        /// ReadIndex round (M7), echoed back by the follower. A leader
        /// confirming it still leads must count only acks to heartbeats sent
        /// *after* the read was recorded — an ack already in flight proves
        /// leadership at an earlier instant, and the leader could have been
        /// deposed in between. `None` when no read is outstanding.
        read_round: Option<u64>,
    },
    AppendEntriesResp {
        term: Term,
        success: bool,
        /// The highest index this follower has confirmed matches the leader's
        /// log, as of this reply. The leader must take this rather than infer
        /// it from its own last send: with several AppendEntries in flight to
        /// one peer, a reply to an older, shorter one would otherwise be
        /// credited with the newest, longest one's end index, and the leader
        /// would commit entries the follower never received. Meaningless when
        /// `success` is false.
        match_index: LogIndex,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
        /// Echoed from the request, untouched. See `AppendEntries::read_round`.
        read_round: Option<u64>,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        /// The membership as of the boundary (M9). Conf entries at or below
        /// it are gone with the prefix; without this the follower would keep
        /// its stale config and disagree about every quorum.
        config: ClusterConfig,
    },
    InstallSnapshotResp {
        term: Term,
        success: bool,
    },
    /// Leadership handoff (M9). A leader that has committed its own removal
    /// sends this to the most-caught-up voter, which campaigns at once instead
    /// of waiting out its election timeout. Without it, removing the leader
    /// costs an unavailability window the operator chose, not one the protocol
    /// needed.
    TimeoutNow {
        term: Term,
        leader_id: NodeId,
    },
    /// The transport-level acknowledgement of a `TimeoutNow`: the campaign it
    /// triggers travels as a separate `RequestVote`, so the RPC itself needs
    /// no Raft semantics — just an answer for the request/response plumbing.
    TimeoutNowResp {
        term: Term,
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
            | Message::InstallSnapshotResp { term, .. }
            | Message::TimeoutNow { term, .. }
            | Message::TimeoutNowResp { term } => term,
        }
    }
}

/// One unit of work `tick`/`step`/`propose` asks its caller to perform.
/// The core never sends, fsyncs, or applies — it only describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send {
        to: NodeId,
        msg: Message,
    },
    PersistEntries(Vec<Entry>),
    PersistHardState(HardState),
    ApplyEntries {
        up_to: LogIndex,
    },
    /// A received snapshot was installed into storage. The caller must restore
    /// its state machine from the snapshot data and treat everything through
    /// `last_included_index` as applied — none of it will arrive as entries.
    ApplySnapshot(Snapshot),
}

/// Why a leader cannot serve a read right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadIndexError {
    /// Only the leader can serve a linearizable read. The caller answers
    /// `NotLeader` and the client redirects.
    NotLeader,
    /// The leader has not yet committed an entry in its own term, so its
    /// commit index may not reflect everything it is required to hold — the
    /// figure-8 case. The no-op appended on election resolves this; until it
    /// commits, reads must wait.
    NoQuorumInTerm,
}

impl std::fmt::Display for ReadIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadIndexError::NotLeader => write!(f, "not the leader"),
            ReadIndexError::NoQuorumInTerm => {
                write!(f, "leader has not yet committed an entry in its own term")
            }
        }
    }
}

impl std::error::Error for ReadIndexError {}

/// A read that cleared the leadership check (M7).
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
    /// A snapshot the caller must restore its state machine from (M8). Set at
    /// most once per drain; the caller syncs storage before acting on it, like
    /// every other durable state in this struct.
    pub snapshot: Option<Snapshot>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
            && self.entries.is_empty()
            && self.hard_state.is_none()
            && self.committed.is_empty()
            && self.read_states.is_empty()
            && self.snapshot.is_none()
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
