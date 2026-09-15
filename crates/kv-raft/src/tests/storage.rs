use crate::storage::{MemStorage, RaftStorage, StorageError};
use crate::types::{Entry, HardState, LogIndex, Term};

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: vec![index as u8] }
}

#[test]
fn empty_log_reports_index_zero_and_term_zero() {
    let s = MemStorage::default();
    assert_eq!(s.last_index().unwrap(), 0);
    assert_eq!(s.term(0).unwrap(), Some(0), "index 0 is the before-the-log sentinel");
    assert_eq!(s.term(1).unwrap(), None);
    assert_eq!(s.entries(1, 1).unwrap(), vec![]);
}

#[test]
fn append_then_read_back_is_half_open() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 2)]).unwrap();

    assert_eq!(s.last_index().unwrap(), 3);
    assert_eq!(s.entries(1, 3).unwrap(), vec![entry(1, 1), entry(2, 1)]);
    assert_eq!(s.entries(2, 4).unwrap(), vec![entry(2, 1), entry(3, 2)]);
    assert_eq!(s.term(3).unwrap(), Some(2));
}

#[test]
fn append_with_a_gap_is_rejected() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1)]).unwrap();

    assert!(matches!(s.append(&[entry(3, 1)]), Err(StorageError::Gap { expected: 2, got: 3 })));
}

#[test]
fn truncate_suffix_then_append_leaves_no_ghost_entries() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]).unwrap();

    s.truncate_suffix(2).unwrap();
    assert_eq!(s.last_index().unwrap(), 1);
    assert_eq!(s.term(2).unwrap(), None);

    s.append(&[entry(2, 5)]).unwrap();
    assert_eq!(s.last_index().unwrap(), 2, "the old index 3 must not reappear");
    assert_eq!(s.term(2).unwrap(), Some(5));
    assert_eq!(s.term(3).unwrap(), None);
    assert_eq!(
        s.entries(1, 10).unwrap(),
        vec![entry(1, 1), Entry { term: 5, index: 2, command: vec![2] }]
    );
}

#[test]
fn truncate_past_the_end_is_a_noop() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1)]).unwrap();
    s.truncate_suffix(99).unwrap();
    assert_eq!(s.last_index().unwrap(), 1);
}

#[test]
fn hard_state_round_trips() {
    let mut s = MemStorage::default();
    assert_eq!(s.hard_state().unwrap(), HardState::default());

    let hs = HardState { term: 4, voted_for: Some(2), commit_index: 1 };
    s.save_hard_state(&hs).unwrap();
    assert_eq!(s.hard_state().unwrap(), hs);
}

#[cfg(feature = "testing")]
#[test]
fn mem_storage_satisfies_the_conformance_suite() {
    struct Harness;
    impl crate::testing::StorageHarness for Harness {
        type Storage = MemStorage;
        fn create(&mut self) -> MemStorage {
            MemStorage::default()
        }
        fn reopen(&mut self, s: MemStorage) -> MemStorage {
            s
        }
    }
    crate::testing::assert_storage_conformance(&mut Harness);
}
