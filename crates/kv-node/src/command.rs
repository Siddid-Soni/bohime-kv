//! What a committed log entry means to the state machine (M6, extended at M7).
//!
//! `Entry::command` is opaque to Raft by design, so the interpretation lives
//! here. The one subtlety is the empty command: a new leader appends a no-op
//! in its own term (the figure-8 rule, `kv-raft`'s `become_leader`), and that
//! entry is committed and applied like any other. It must mean "do nothing"
//! rather than be an error, or every election would poison the state machine.
//!
//! bincode tags an enum variant with a u32, so no real command can encode to
//! zero bytes and collide with the no-op. There is a named test for that —
//! the same collapse `Option<u64>` versus a sentinel 0 caused in M5's proto.
//!
//! M7 adds the request context. It rides in the log entry rather than beside
//! it because the session table it feeds is part of the replicated state
//! (§1.8) — a context that did not reach the log could not be replayed on the
//! node that takes over.

use serde::{Deserialize, Serialize};

use crate::session::RequestCtx;

/// A change to the state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Compare-and-swap. `expected: None` means "only if absent", which is how
    /// a create-if-not-exists is expressed without a separate operation.
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new_value: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    /// `None` when the client is not tracking retries. Not a sentinel id:
    /// client 0 is a real client, and conflating the two is the same trap a
    /// sentinel `leader_hint` or `conflict_index` would be.
    pub ctx: Option<RequestCtx>,
    pub op: Mutation,
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("undecodable command: {0}")]
    Encoding(#[from] bincode::Error),
}

impl Command {
    pub fn new(ctx: Option<RequestCtx>, op: Mutation) -> Self {
        Self { ctx, op }
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("a Command always serializes")
    }

    /// `Ok(None)` is the leader's no-op, not a failure.
    pub fn decode(bytes: &[u8]) -> Result<Option<Command>, CommandError> {
        if bytes.is_empty() {
            return Ok(None);
        }
        Ok(Some(bincode::deserialize(bytes)?))
    }
}
