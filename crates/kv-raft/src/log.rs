//! Log arithmetic for the Raft core. Pure functions over `RaftStorage`:
//! index/term reads, the consistency check (M3.4), and commit-index math (M3.5).

use crate::storage::RaftStorage;
use crate::types::{LogIndex, Term};

/// The last log entry's `(index, term)`, or `(0, 0)` for an empty log.
/// Storage errors are fatal (see the M3 plan: `MemStorage` cannot fail, and a
/// `BitcaskStorage` IO error means the disk is gone).
pub(crate) fn last_log<S: RaftStorage>(storage: &S) -> (LogIndex, Term) {
    let index = storage.last_index().expect("raft storage");
    let term = storage.term(index).expect("raft storage").unwrap_or(0);
    (index, term)
}

/// The outcome of the Log Matching check for one `AppendEntries`.
pub(crate) enum Consistency {
    Match,
    Mismatch { conflict_term: Option<Term>, conflict_index: Option<LogIndex> },
}

/// Whether `prev_log_index`/`prev_log_term` agree with our log. A mismatch
/// carries the conflict hint that lets the leader skip a whole term (M3.4):
/// our conflicting term plus its first index, or — when our log is simply
/// shorter — the index where entries must start.
pub(crate) fn check_consistency<S: RaftStorage>(
    storage: &S,
    prev_index: LogIndex,
    prev_term: Term,
) -> Consistency {
    if prev_index == 0 {
        return Consistency::Match;
    }
    let last = storage.last_index().expect("raft storage");
    if prev_index > last {
        return Consistency::Mismatch { conflict_term: None, conflict_index: Some(last + 1) };
    }
    match storage.term(prev_index).expect("raft storage") {
        Some(term) if term == prev_term => Consistency::Match,
        Some(term) => Consistency::Mismatch {
            conflict_term: Some(term),
            conflict_index: Some(first_index_of_term(storage, term, prev_index)),
        },
        None => Consistency::Mismatch { conflict_term: None, conflict_index: Some(last + 1) },
    }
}

/// The first index holding `term`, scanning back from `known_at`. Our log is
/// append-only per term run, so the first occurrence at or before `known_at`
/// is the term's start.
fn first_index_of_term<S: RaftStorage>(storage: &S, term: Term, known_at: LogIndex) -> LogIndex {
    let mut first = known_at;
    while first > 1 {
        if storage.term(first - 1).expect("raft storage") != Some(term) {
            break;
        }
        first -= 1;
    }
    first
}
