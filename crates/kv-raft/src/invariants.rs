//! The four Raft safety invariants (M3.7), checkable after *any* step.
//!
//! M4's simulator calls [`check_all`] after every single `tick`/`step`, so
//! these are snapshot checks over the whole cluster: no history is threaded
//! through, and nothing here allocates per node beyond the logs it is handed.
//!
//! A violation here is a real bug in the core, not a flaky test — every one of
//! these holds at every instant of a correct run, including mid-election and
//! mid-partition.

use std::collections::BTreeMap;

use crate::message::Role;
use crate::node::RaftNode;
use crate::storage::RaftStorage;
use crate::types::{Entry, LogIndex, NodeId, Term};

/// One node's externally visible state, which is all the invariants need.
///
/// Separated from `RaftNode` so tests can construct states a correct run never
/// reaches — a checker that has only ever been run against healthy clusters is
/// not known to detect anything.
#[derive(Debug, Clone)]
pub struct NodeView {
    pub id: NodeId,
    pub term: Term,
    pub role: Role,
    pub commit_index: LogIndex,
    pub log: Vec<Entry>,
}

impl NodeView {
    fn of<S: RaftStorage>(node: &RaftNode<S>) -> Self {
        Self {
            id: node.id(),
            term: node.current_term(),
            role: node.role(),
            commit_index: node.commit_index(),
            log: node.log_entries(),
        }
    }

    /// The entry at `index`, or `None` if this node's log does not reach it.
    fn at(&self, index: LogIndex) -> Option<&Entry> {
        self.log.iter().find(|e| e.index == index)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Violation {
    #[error("election safety: term {term} has {} leaders: {leaders:?}", leaders.len())]
    ElectionSafety { term: Term, leaders: Vec<NodeId> },

    #[error(
        "log matching: nodes {a} and {b} agree at index {index} (term {term}) \
         but their logs differ earlier, at index {diverged_at}"
    )]
    LogMatching { a: NodeId, b: NodeId, index: LogIndex, term: Term, diverged_at: LogIndex },

    #[error(
        "leader completeness: leader {leader} of term {leader_term} is missing \
         entry {expected:?}, committed in an earlier term"
    )]
    LeaderCompleteness { leader: NodeId, leader_term: Term, expected: Entry },

    #[error(
        "state machine safety: nodes {a} and {b} committed different entries at \
         index {index}: {a_entry:?} vs {b_entry:?}"
    )]
    StateMachineSafety { index: LogIndex, a: NodeId, b: NodeId, a_entry: Entry, b_entry: Entry },
}

/// Checks every invariant against a live cluster.
pub fn check_all<S: RaftStorage>(nodes: &[RaftNode<S>]) -> Result<(), Violation> {
    let views: Vec<NodeView> = nodes.iter().map(NodeView::of).collect();
    check_views(&views)
}

/// Checks every invariant against hand-built state. See [`NodeView`].
///
/// Ordered cheapest-first, and with state machine safety before leader
/// completeness: the latter reads the committed prefix as authoritative, which
/// is only meaningful once the nodes are known to agree on it.
pub fn check_views(views: &[NodeView]) -> Result<(), Violation> {
    election_safety(views)?;
    log_matching(views)?;
    state_machine_safety(views)?;
    leader_completeness(views)
}

/// At most one leader per term.
fn election_safety(views: &[NodeView]) -> Result<(), Violation> {
    let mut by_term: BTreeMap<Term, Vec<NodeId>> = BTreeMap::new();
    for v in views.iter().filter(|v| v.role == Role::Leader) {
        by_term.entry(v.term).or_default().push(v.id);
    }
    for (term, mut leaders) in by_term {
        if leaders.len() > 1 {
            leaders.sort_unstable();
            return Err(Violation::ElectionSafety { term, leaders });
        }
    }
    Ok(())
}

/// If two logs hold an entry with the same index *and* term, everything before
/// it is identical too. Divergence at differing terms is legal — that is just
/// an interrupted replication waiting to be repaired.
fn log_matching(views: &[NodeView]) -> Result<(), Violation> {
    for (i, a) in views.iter().enumerate() {
        for b in views.iter().skip(i + 1) {
            for ea in &a.log {
                let Some(eb) = b.at(ea.index) else { continue };
                if ea.term != eb.term {
                    continue;
                }
                if let Some(diverged_at) = first_divergence(a, b, ea.index) {
                    return Err(Violation::LogMatching {
                        a: a.id,
                        b: b.id,
                        index: ea.index,
                        term: ea.term,
                        diverged_at,
                    });
                }
            }
        }
    }
    Ok(())
}

/// The lowest index at or below `through` where two logs disagree.
fn first_divergence(a: &NodeView, b: &NodeView, through: LogIndex) -> Option<LogIndex> {
    (1..=through).find(|&index| match (a.at(index), b.at(index)) {
        (Some(x), Some(y)) => x != y,
        (None, None) => false,
        _ => true,
    })
}

/// No two nodes commit different entries at the same index. This is the one
/// that corresponds to losing an acknowledged write.
fn state_machine_safety(views: &[NodeView]) -> Result<(), Violation> {
    let highest = views.iter().map(|v| v.commit_index).max().unwrap_or(0);
    for index in 1..=highest {
        let mut first: Option<(&NodeView, &Entry)> = None;
        for v in views {
            if v.commit_index < index {
                continue;
            }
            let Some(e) = v.at(index) else { continue };
            match first {
                None => first = Some((v, e)),
                Some((seen, seen_entry)) if seen_entry != e => {
                    return Err(Violation::StateMachineSafety {
                        index,
                        a: seen.id,
                        b: v.id,
                        a_entry: seen_entry.clone(),
                        b_entry: e.clone(),
                    });
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// An entry committed in some term is present in every leader of a later term.
/// Leaders at or below the committing term are exempt: a deposed leader that
/// has not yet heard about the entry is not a violation.
fn leader_completeness(views: &[NodeView]) -> Result<(), Violation> {
    let mut committed: BTreeMap<LogIndex, Entry> = BTreeMap::new();
    for v in views {
        for e in v.log.iter().filter(|e| e.index <= v.commit_index) {
            committed.insert(e.index, e.clone());
        }
    }

    for l in views.iter().filter(|v| v.role == Role::Leader) {
        for e in committed.values() {
            if e.term >= l.term {
                continue;
            }
            if l.at(e.index) != Some(e) {
                return Err(Violation::LeaderCompleteness {
                    leader: l.id,
                    leader_term: l.term,
                    expected: e.clone(),
                });
            }
        }
    }
    Ok(())
}
