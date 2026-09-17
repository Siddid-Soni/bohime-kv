//! Cluster membership (M9): who votes, who learns, and how the set changes.
//!
//! One Raft group tolerates minority failure only while a majority agrees on
//! *who the majority is*. That set lives here as [`ClusterConfig`], changed
//! exclusively by [`ConfChange`] log entries — never by flags, RPC side
//! channels, or restart-time argv. The log is the source of truth: a node
//! rebuilds its config by replaying conf entries on open (plus the snapshot's
//! config for the compacted prefix), so two replicas that agree on the log
//! agree on the membership.
//!
//! [`Role::Learner`] is deliberately absent: learner-hood is a membership
//! property, not a Raft role. A learner is a follower that never campaigns,
//! never grants votes, and is never counted toward any quorum — but otherwise
//! runs the full follower path (log, snapshots, commit), which is what lets it
//! catch up and be promoted.
//!
//! Safety shape (single-server change, §1.7): every voter-set change alters
//! the voter set by exactly one node, takes effect on append, and at most one
//! change is uncommitted at a time. Any old majority then overlaps any new
//! majority, so no two leaders can be elected in the same term under different
//! configs. Joint consensus is the general alternative; single-server is the
//! standard choice for one-at-a-time operations work and all M9 needs.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Magic prefix distinguishing a conf entry's command bytes from data.
///
// A data command is either empty (the leader no-op) or a bincode `Command`
/// whose first encoded field is the `ctx: Option<RequestCtx>` tag — `0x00` or
/// `0x01`. `CONF_MAGIC` starts with `0x52`, so the two can never collide, and
/// the driver routes on the prefix before `Command::decode` ever sees the
/// bytes. Pinned by `membership_prefix_cannot_collide_with_commands`.
pub const CONF_MAGIC: [u8; 4] = [0x52, 0x43, 0x46, 0x39]; // "RCF9"

/// Who votes and who learns. Deterministic ordering (`BTreeSet`, never
/// `HashSet`): configs are compared, snapshotted, and exchanged across replicas,
/// and iteration order must not leak into any of that.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
}

impl ClusterConfig {
    /// The initial config: everybody the operator named votes, nobody learns.
    pub fn voting(members: impl IntoIterator<Item = NodeId>) -> Self {
        Self { voters: members.into_iter().collect(), learners: BTreeSet::new() }
    }

    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    pub fn is_learner(&self, id: NodeId) -> bool {
        self.learners.contains(&id)
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.is_learner(id)
    }

    /// Applies one change, validating the single-server shape: exactly one
    /// voter added or removed, learners untouched by voter ops, and the voter
    /// set never emptied. Deterministic in `(old, change)`, so every replica
    /// applying the same entry reaches the same config — including agreement
    /// to *reject* a malformed one.
    pub fn apply(&self, change: &ConfChange) -> Result<ClusterConfig, ConfError> {
        let mut next = self.clone();
        match change.op {
            ConfOp::AddLearner => {
                if next.is_voter(change.node) || next.is_learner(change.node) {
                    return Err(ConfError::AlreadyMember(change.node));
                }
                next.learners.insert(change.node);
            }
            ConfOp::RemoveLearner => {
                if !next.learners.remove(&change.node) {
                    return Err(ConfError::NotMember(change.node));
                }
            }
            ConfOp::Promote => {
                if !next.learners.remove(&change.node) {
                    return Err(ConfError::NotLearner(change.node));
                }
                next.voters.insert(change.node);
            }
            ConfOp::RemoveVoter => {
                if !next.voters.remove(&change.node) {
                    return Err(ConfError::NotVoter(change.node));
                }
                next.learners.remove(&change.node);
                if next.voters.is_empty() {
                    return Err(ConfError::EmptyVoters);
                }
            }
        }
        Ok(next)
    }
}

/// One membership operation. There is deliberately no raw "add voter": a cold
/// node made a voter on arrival stalls every quorum until it catches up. The
/// only path in is learner → catch-up → promote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfOp {
    AddLearner,
    RemoveLearner,
    Promote,
    RemoveVoter,
}

/// A membership change proposed as a log entry. `context` is opaque to the
/// core — the application stashes what *it* needs to reach the node (for
/// `kv-node`, the endpoint address) so a restart rebuilds dial state from the
/// log rather than from flags. The core never reads it; it only preserves it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfChange {
    pub op: ConfOp,
    pub node: NodeId,
    pub context: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfError {
    #[error("node {0} is already a member")]
    AlreadyMember(NodeId),
    #[error("node {0} is not a member")]
    NotMember(NodeId),
    #[error("node {0} is not a learner and cannot be promoted")]
    NotLearner(NodeId),
    #[error("node {0} is not a voter and cannot be removed")]
    NotVoter(NodeId),
    #[error("a config with no voters cannot elect or commit")]
    EmptyVoters,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConfProposeError {
    #[error("only the leader changes membership")]
    NotLeader,
    #[error("a membership change is already uncommitted")]
    ConfInFlight,
    #[error("invalid membership change: {0}")]
    Invalid(#[from] ConfError),
}

/// Serializes a conf change for a log entry's command bytes: magic prefix plus
/// the bincode body.
pub fn encode_conf(change: &ConfChange) -> Vec<u8> {
    let mut out = Vec::with_capacity(CONF_MAGIC.len() + 32);
    out.extend_from_slice(&CONF_MAGIC);
    out.extend_from_slice(&bincode::serialize(change).expect("a ConfChange always serializes"));
    out
}

/// Whether these command bytes carry a conf change. Checked before any data
/// decoding, so conf entries never reach the state machine.
pub fn is_conf_change(command: &[u8]) -> bool {
    command.starts_with(&CONF_MAGIC)
}

/// Decodes a conf entry. `None` means "not a conf entry" — including corrupt
/// bytes under a forged-or-torn magic, which every replica deterministically
/// ignores rather than half-applying.
pub fn decode_conf(command: &[u8]) -> Option<ConfChange> {
    let body = command.strip_prefix(&CONF_MAGIC)?;
    bincode::deserialize(body).ok()
}
