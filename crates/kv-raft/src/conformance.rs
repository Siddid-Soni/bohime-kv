//! The shared `RaftStorage` conformance suite.
//!
//! `MemStorage` (which M3 and M4 run against) and `BitcaskStorage` (which
//! production runs against) must answer identical questions identically — a
//! divergence between them would surface as a Raft bug that only reproduces on
//! real hardware, the worst kind to debug. So both run these same assertions.
//!
//! Shipped API, not test code: `kv-node` is a different crate and has to reach
//! it. It is not feature-gated — every item is generic over `H`, so a release
//! build that never instantiates one emits no code for it. It must stay
//! dependency-free, since `kv-raft` is the pure core; if it ever needs
//! `proptest`, move it to its own crate rather than add that dependency here.
//!
//! Use it through [`raft_storage_conformance!`], which generates one named
//! `#[test]` per requirement, so a failure names the requirement that broke
//! instead of reporting a single opaque suite failure.

use crate::membership::ClusterConfig;
use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, LogIndex, Snapshot, Term};

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

pub fn empty_log_conventions<H: StorageHarness>(h: &mut H) {
    let s = h.create();
    assert_eq!(s.last_index().unwrap(), 0, "empty log's last_index must be 0");
    assert_eq!(s.term(0).unwrap(), Some(0), "index 0 is the before-the-log sentinel");
    assert_eq!(s.term(1).unwrap(), None, "no entry at 1 in an empty log");
    assert!(s.entries(1, 100).unwrap().is_empty());
    assert!(s.snapshot().unwrap().is_none());
}

pub fn append_and_range_reads<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 2)]).unwrap();

    assert_eq!(s.last_index().unwrap(), 3);
    assert_eq!(s.term(3).unwrap(), Some(2));
    assert_eq!(s.entries(1, 3).unwrap(), vec![entry(1, 1), entry(2, 1)], "entries is half-open");
    assert_eq!(s.entries(3, 4).unwrap(), vec![entry(3, 2)]);
    assert_eq!(s.entries(1, 999).unwrap().len(), 3, "reading past the end must not error");
    assert!(s.entries(5, 5).unwrap().is_empty(), "an empty range is empty, not an error");
}

pub fn contiguity_is_enforced<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1)]).unwrap();
    assert!(s.append(&[entry(3, 1)]).is_err(), "a gap in the log must be rejected");
    assert_eq!(s.last_index().unwrap(), 1, "a rejected append must change nothing");
}

pub fn truncate_suffix_leaves_no_ghosts<H: StorageHarness>(h: &mut H) {
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

pub fn hard_state_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    assert_eq!(s.hard_state().unwrap(), HardState::default());

    let hs = HardState { term: 11, voted_for: Some(4), commit_index: 7 };
    s.save_hard_state(&hs).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.hard_state().unwrap(), hs, "a lost vote lets a node vote twice in one term");
}

pub fn log_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    let written: Vec<Entry> = (1..=50).map(|i| entry(i, 1 + i / 10)).collect();
    s.append(&written).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.last_index().unwrap(), 50);
    assert_eq!(s.entries(1, 51).unwrap(), written);
    assert_eq!(s.term(50).unwrap(), Some(6));
}

pub fn snapshot_persists_and_term_covers_the_boundary<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 2)]).unwrap();

    let snap = Snapshot {
        last_included_index: 2,
        last_included_term: 1,
        data: vec![7; 8],
        config: ClusterConfig::default(),
    };
    s.save_snapshot(&snap).unwrap();
    assert_eq!(s.snapshot().unwrap(), Some(snap.clone()));

    s.truncate_prefix(2).unwrap();
    assert_eq!(s.first_index().unwrap(), 3);
    assert_eq!(s.term(2).unwrap(), Some(1), "boundary term survives its prefix");
    assert_eq!(s.term(1).unwrap(), None, "below the snapshot the log is gone");
    assert_eq!(s.snapshot().unwrap(), Some(snap));
}

pub fn append_continues_after_a_fully_compacted_prefix<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1)]).unwrap();
    s.save_snapshot(&Snapshot {
        last_included_index: 2,
        last_included_term: 1,
        data: vec![],
        config: ClusterConfig::default(),
    })
    .unwrap();
    s.truncate_prefix(2).unwrap();

    assert_eq!(s.last_index().unwrap(), 2);
    assert_eq!(s.first_index().unwrap(), 3);
    s.append(&[entry(3, 1)]).unwrap();
    assert_eq!(s.entries(1, 10).unwrap(), vec![entry(3, 1)]);
}

pub fn snapshot_and_truncation_survive_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&(1..=10).map(|i| entry(i, 1)).collect::<Vec<_>>()).unwrap();
    let snap = Snapshot {
        last_included_index: 7,
        last_included_term: 1,
        data: vec![9; 4],
        config: ClusterConfig::default(),
    };
    s.save_snapshot(&snap).unwrap();
    s.truncate_prefix(7).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.snapshot().unwrap(), Some(snap));
    assert_eq!(s.first_index().unwrap(), 8);
    assert_eq!(s.last_index().unwrap(), 10);
    assert_eq!(s.term(7).unwrap(), Some(1));
    assert_eq!(s.term(6).unwrap(), None);
    assert_eq!(s.entries(1, 11).unwrap().len(), 3);
}

pub fn truncation_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&(1..=20).map(|i| entry(i, 1)).collect::<Vec<_>>()).unwrap();
    s.truncate_suffix(11).unwrap();
    s.append(&[entry(11, 7)]).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.last_index().unwrap(), 11, "truncated entries must not come back on reopen");
    assert_eq!(s.term(11).unwrap(), Some(7));
    assert_eq!(s.term(12).unwrap(), None);
}

/// Generates one `#[test]` per `RaftStorage` requirement, each against a fresh
/// harness.
///
/// `$harness` is re-evaluated for every generated test, so it must be an
/// expression that builds a new harness rather than a shared one:
///
/// ```ignore
/// raft_storage_conformance!(mem_storage, Harness);
/// ```
///
/// The generated tests land in a module named `$name`, so one crate can run the
/// suite against several implementations without the names colliding.
#[macro_export]
macro_rules! raft_storage_conformance {
    ($name:ident, $harness:expr) => {
        mod $name {
            #[allow(unused_imports)]
            use super::*;

            #[test]
            fn empty_log_conventions() {
                $crate::conformance::empty_log_conventions(&mut $harness);
            }

            #[test]
            fn append_and_range_reads() {
                $crate::conformance::append_and_range_reads(&mut $harness);
            }

            #[test]
            fn contiguity_is_enforced() {
                $crate::conformance::contiguity_is_enforced(&mut $harness);
            }

            #[test]
            fn truncate_suffix_leaves_no_ghosts() {
                $crate::conformance::truncate_suffix_leaves_no_ghosts(&mut $harness);
            }

            #[test]
            fn hard_state_survives_reopen() {
                $crate::conformance::hard_state_survives_reopen(&mut $harness);
            }

            #[test]
            fn log_survives_reopen() {
                $crate::conformance::log_survives_reopen(&mut $harness);
            }

            #[test]
            fn truncation_survives_reopen() {
                $crate::conformance::truncation_survives_reopen(&mut $harness);
            }

            #[test]
            fn snapshot_persists_and_term_covers_the_boundary() {
                $crate::conformance::snapshot_persists_and_term_covers_the_boundary(&mut $harness);
            }

            #[test]
            fn append_continues_after_a_fully_compacted_prefix() {
                $crate::conformance::append_continues_after_a_fully_compacted_prefix(&mut $harness);
            }

            #[test]
            fn snapshot_and_truncation_survive_reopen() {
                $crate::conformance::snapshot_and_truncation_survive_reopen(&mut $harness);
            }
        }
    };
}
