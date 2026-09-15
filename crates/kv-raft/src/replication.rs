//! Replication bookkeeping (M3.4). Pure functions the leader calls when an
//! `AppendEntriesResp` arrives: where to resume sending after a rejection.

use crate::storage::RaftStorage;
use crate::types::{LogIndex, Term};

/// Where the leader should resume sending to a follower that rejected with
/// the given conflict hint (`conflict_term`/`conflict_index` from the
/// rejection):
///
/// - If the follower's conflicting term exists in our log, resume just after
///   its last entry — skipping the whole term in one round trip instead of
///   decrementing past it entry by entry.
/// - Else if the follower named an index (log too short), resume there.
/// - Else (no hint at all) fall back to one step back. This arm should be
///   unreachable against our own follower code, which always hints.
pub(crate) fn backtrack<S: RaftStorage>(
    current_next: LogIndex,
    conflict_term: Option<Term>,
    conflict_index: Option<LogIndex>,
    storage: &S,
) -> LogIndex {
    if let Some(term) = conflict_term {
        let last = storage.last_index().expect("raft storage");
        let mut found = None;
        for idx in 1..=last {
            if storage.term(idx).expect("raft storage") == Some(term) {
                found = Some(idx);
            }
        }
        if let Some(idx) = found {
            return idx + 1;
        }
    }
    if let Some(index) = conflict_index {
        return index.max(1);
    }
    current_next.saturating_sub(1).max(1)
}
