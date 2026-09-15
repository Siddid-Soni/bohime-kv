//! The persistence interface Raft needs, and an in-memory implementation.
//!
//! The trait is deliberately narrow: Raft needs to append to a log, read a
//! range back, ask a single entry's term, discard a divergent suffix, and
//! persist the hard state. Everything else — where the bytes live, whether
//! they are fsynced, how they are encoded — is the implementor's problem.
//!
//! `MemStorage` is what M3 and M4 run against: fast, deterministic, and with
//! no filesystem anywhere near the pure core. The production Bitcask-backed
//! impl lives in `kv-node`, because `kv-storage` must not know Raft exists.

use std::collections::BTreeMap;

use crate::types::{Entry, HardState, LogIndex, Snapshot, Term};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("append would leave a gap: expected first index {expected}, got {got}")]
    Gap { expected: LogIndex, got: LogIndex },
    #[error("entries are not contiguous at index {at}")]
    NotContiguous { at: LogIndex },
    #[error("backing store error: {0}")]
    Backend(String),
}

pub trait RaftStorage {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persists the state that must survive a crash for Raft to stay safe:
    /// current term, the vote in it, and the commit index. Callers must
    /// complete this before acting on the state it records — granting a vote
    /// before persisting it is how a node votes twice in one term.
    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error>;
    fn hard_state(&self) -> Result<HardState, Self::Error>;

    /// Appends contiguous entries starting at `last_index() + 1`.
    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error>;

    /// Entries in the half-open range `[lo, hi)`. Indices outside the log are
    /// silently skipped rather than erroring, so a caller can ask for more
    /// than exists.
    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error>;

    /// The term at `idx`, or `None` if no such entry exists. `term(0)` is
    /// always `Some(0)` — the before-the-log sentinel.
    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Discards every entry with index `>= from`. A no-op if `from` is past
    /// the end. This is what M3.4's conflict resolution calls when a
    /// follower's log diverges from the leader's.
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error>;

    /// The highest index in the log, or 0 if the log is empty.
    fn last_index(&self) -> Result<LogIndex, Self::Error>;

    /// The most recent snapshot, if any. Written at M8; `None` until then.
    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error>;
}

#[derive(Debug, Default)]
pub struct MemStorage {
    entries: BTreeMap<LogIndex, Entry>,
    hard_state: HardState,
    snapshot: Option<Snapshot>,
}

impl RaftStorage for MemStorage {
    type Error = StorageError;

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error> {
        self.hard_state = *hs;
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState, Self::Error> {
        Ok(self.hard_state)
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let expected = self.last_index()? + 1;
        if first.index != expected {
            return Err(StorageError::Gap { expected, got: first.index });
        }
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(StorageError::NotContiguous { at: pair[1].index });
            }
        }
        for entry in entries {
            self.entries.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error> {
        Ok(self.entries.range(lo..hi).map(|(_, e)| e.clone()).collect())
    }

    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error> {
        if idx == 0 {
            return Ok(Some(0));
        }
        Ok(self.entries.get(&idx).map(|e| e.term))
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        self.entries.retain(|&idx, _| idx < from);
        Ok(())
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.entries.keys().next_back().copied().unwrap_or(0))
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(self.snapshot.clone())
    }
}
