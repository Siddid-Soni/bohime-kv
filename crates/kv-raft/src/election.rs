//! Election rules (M3.3, M3.6). Pure functions: who may be granted a vote,
//! decided from terms and log freshness alone. `node.rs` handles persistence
//! and transport; this module answers the yes/no question.

use crate::types::{LogIndex, NodeId, Term};

/// What a candidate presents with its request.
pub struct CandidateInfo {
    pub term: Term,
    pub id: NodeId,
    pub last_term: Term,
    pub last_index: LogIndex,
}

/// What a voter decides from: its term, existing vote, and log freshness.
pub struct VoterState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub last_term: Term,
    pub last_index: LogIndex,
}

/// Whether the voter grants its vote to the candidate.
///
/// Denies when: the candidate's term is stale; we already voted for someone
/// else this term; or the candidate's log is not at least as up to date as
/// ours (higher last term, or same last term and longer log — §1.5, the
/// election restriction that gives Leader Completeness).
pub fn should_grant_vote(candidate: &CandidateInfo, voter: &VoterState) -> bool {
    if candidate.term < voter.current_term {
        return false;
    }
    if let Some(voted) = voter.voted_for
        && voted != candidate.id
    {
        return false;
    }
    candidate.last_term > voter.last_term
        || (candidate.last_term == voter.last_term && candidate.last_index >= voter.last_index)
}
