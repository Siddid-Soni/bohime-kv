//! A conformance suite for `RaftStorage`, shared by every implementation.
//!
//! Behind the `testing` feature so it never reaches a release build. The point
//! is that `MemStorage` (which M3 and M4 run against) and `BitcaskStorage`
//! (which production runs against) answer identical questions identically —
//! a divergence between them would show up as a Raft bug that only reproduces
//! on real hardware, which is the worst kind to debug.

use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, LogIndex, Term};

pub trait StorageHarness {
    type Storage: RaftStorage;
    /// A fresh, empty store.
    fn create(&mut self) -> Self::Storage;
    /// Drop `s` and reopen the same backing store. For in-memory impls this
    /// is the identity; for durable ones it must actually round-trip disk.
    fn reopen(&mut self, s: Self::Storage) -> Self::Storage;
}

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: format!("cmd-{index}").into_bytes() }
}

/// Runs every `RaftStorage` requirement against `harness`. Panics on the first
/// violation, naming which requirement failed.
pub fn assert_storage_conformance<H: StorageHarness>(harness: &mut H) {
    empty_log_conventions(harness);
    append_and_range_reads(harness);
    contiguity_is_enforced(harness);
    truncate_suffix_leaves_no_ghosts(harness);
    hard_state_survives_reopen(harness);
    log_survives_reopen(harness);
    truncation_survives_reopen(harness);
}

fn empty_log_conventions<H: StorageHarness>(h: &mut H) {
    let s = h.create();
    assert_eq!(s.last_index().unwrap(), 0, "empty log's last_index must be 0");
    assert_eq!(s.term(0).unwrap(), Some(0), "index 0 is the before-the-log sentinel");
    assert_eq!(s.term(1).unwrap(), None, "no entry at 1 in an empty log");
    assert!(s.entries(1, 100).unwrap().is_empty());
    assert!(s.snapshot().unwrap().is_none());
}

fn append_and_range_reads<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 2)]).unwrap();

    assert_eq!(s.last_index().unwrap(), 3);
    assert_eq!(s.term(3).unwrap(), Some(2));
    assert_eq!(s.entries(1, 3).unwrap(), vec![entry(1, 1), entry(2, 1)], "entries is half-open");
    assert_eq!(s.entries(3, 4).unwrap(), vec![entry(3, 2)]);
    assert_eq!(s.entries(1, 999).unwrap().len(), 3, "reading past the end must not error");
    assert!(s.entries(5, 5).unwrap().is_empty(), "an empty range is empty, not an error");
}

fn contiguity_is_enforced<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1)]).unwrap();
    assert!(s.append(&[entry(3, 1)]).is_err(), "a gap in the log must be rejected");
    assert_eq!(s.last_index().unwrap(), 1, "a rejected append must change nothing");
}

fn truncate_suffix_leaves_no_ghosts<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 1), entry(4, 1)]).unwrap();

    s.truncate_suffix(3).unwrap();
    assert_eq!(s.last_index().unwrap(), 2);
    assert_eq!(s.term(3).unwrap(), None);
    assert_eq!(s.term(4).unwrap(), None);

    s.append(&[entry(3, 9)]).unwrap();
    assert_eq!(s.last_index().unwrap(), 3, "the truncated index 4 must not reappear");
    assert_eq!(s.term(3).unwrap(), Some(9));
    assert_eq!(s.term(4).unwrap(), None, "ghost entry from before the truncation");
    assert_eq!(s.entries(1, 999).unwrap().len(), 3);

    s.truncate_suffix(999).unwrap();
    assert_eq!(s.last_index().unwrap(), 3, "truncating past the end is a no-op");
}

fn hard_state_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    assert_eq!(s.hard_state().unwrap(), HardState::default());

    let hs = HardState { term: 11, voted_for: Some(4), commit_index: 7 };
    s.save_hard_state(&hs).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.hard_state().unwrap(), hs, "a lost vote lets a node vote twice in one term");
}

fn log_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    let written: Vec<Entry> = (1..=50).map(|i| entry(i, 1 + i / 10)).collect();
    s.append(&written).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.last_index().unwrap(), 50);
    assert_eq!(s.entries(1, 51).unwrap(), written);
    assert_eq!(s.term(50).unwrap(), Some(6));
}

fn truncation_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&(1..=20).map(|i| entry(i, 1)).collect::<Vec<_>>()).unwrap();
    s.truncate_suffix(11).unwrap();
    s.append(&[entry(11, 7)]).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.last_index().unwrap(), 11, "truncated entries must not come back on reopen");
    assert_eq!(s.term(11).unwrap(), Some(7));
    assert_eq!(s.term(12).unwrap(), None);
}
