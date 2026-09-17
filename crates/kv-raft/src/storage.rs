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

    /// Persists a snapshot replacing the log prefix up to and including
    /// `snapshot.last_included_index` (M8). Does not delete any entries —
    /// `truncate_prefix` does that — so a crash between the two leaves the
    /// prefix redundantly stored, never lost.
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error>;

    /// The smallest available log index: 1 on a fresh store, otherwise one
    /// past the truncated prefix. A fully-compacted log reports
    /// `last_index() + 1`, so the next append continues the sequence instead
    /// of reusing an index the snapshot already covers.
    fn first_index(&self) -> Result<LogIndex, Self::Error>;

    /// Discards every entry with index `<= up_to`. A no-op below the current
    /// base. The snapshot (if any) is untouched — this only drops the log
    /// prefix it already describes.
    fn truncate_prefix(&mut self, up_to: LogIndex) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct MemStorage {
    entries: BTreeMap<LogIndex, Entry>,
    hard_state: HardState,
    snapshot: Option<Snapshot>,
    /// One past the highest truncated prefix index. Stays 1 until the first
    /// `truncate_prefix`; the snapshot alone never moves it, because the
    /// entries it describes are still stored until they are truncated.
    base: LogIndex,
}

impl Default for MemStorage {
    fn default() -> Self {
        Self { entries: BTreeMap::new(), hard_state: HardState::default(), snapshot: None, base: 1 }
    }
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
        Ok(self.entries.range(lo.max(self.base)..hi).map(|(_, e)| e.clone()).collect())
    }

    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error> {
        if idx == 0 {
            return Ok(Some(0));
        }
        if let Some(entry) = self.entries.get(&idx) {
            return Ok(Some(entry.term));
        }
        // The boundary term survives the prefix it describes; below it the
        // log is gone and only the snapshot knows anything.
        if let Some(snap) = &self.snapshot
            && idx == snap.last_included_index
        {
            return Ok(Some(snap.last_included_term));
        }
        Ok(None)
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        self.entries.retain(|&idx, _| idx < from);
        Ok(())
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        let entries_last = self.entries.keys().next_back().copied().unwrap_or(0);
        let snap_last = self.snapshot.as_ref().map(|s| s.last_included_index).unwrap_or(0);
        Ok(entries_last.max(snap_last))
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(self.snapshot.clone())
    }

    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }

    fn first_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.base)
    }

    fn truncate_prefix(&mut self, up_to: LogIndex) -> Result<(), Self::Error> {
        if up_to < self.base {
            return Ok(());
        }
        self.entries.retain(|&idx, _| idx > up_to);
        self.base = up_to + 1;
        Ok(())
    }
}
