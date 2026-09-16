//! What a committed log entry means to the state machine (M6).
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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("undecodable command: {0}")]
    Encoding(#[from] bincode::Error),
}

impl Command {
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
