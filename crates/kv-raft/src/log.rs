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
