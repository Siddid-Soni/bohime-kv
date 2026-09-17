//! Pure Raft consensus core (plan §1.5). No I/O, no async runtime, no
//! wall-clock reads — `tick`/`step` take logical time and return `Action`s
//! for the caller (`kv-node` for real I/O, `kv-sim` for simulated I/O) to
//! execute. See plan Part 2 for the `Action`/`Ready` interface and M3 for
//! the build sequence (types, election, replication, commitment, safety).

pub mod conformance;
pub mod election;
pub mod invariants;
pub mod log;
pub mod membership;
pub mod message;
pub mod node;
pub mod replication;
pub mod storage;
pub mod types;

#[cfg(test)]
mod tests;

pub use membership::{ClusterConfig, ConfChange, ConfError, ConfOp, ConfProposeError};
pub use message::{Action, Config, Message, ProposeError, ReadIndexError, ReadState, Ready, Role};
pub use node::RaftNode;
pub use types::{Entry, HardState, LogIndex, NodeId, Snapshot, Term};
