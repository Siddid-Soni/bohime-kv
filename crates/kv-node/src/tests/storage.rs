use super::*;
use kv_raft::types::{Entry, HardState};

#[test]
fn bitcask_storage_satisfies_the_conformance_suite() {
    struct Harness {
        dir: tempfile::TempDir,
        generation: usize,
    }
    impl kv_raft::testing::StorageHarness for Harness {
        type Storage = BitcaskStorage;

        fn create(&mut self) -> BitcaskStorage {
            self.generation += 1;
            let path = self.dir.path().join(format!("gen-{}", self.generation));
            BitcaskStorage::open(path).unwrap()
        }

        fn reopen(&mut self, s: BitcaskStorage) -> BitcaskStorage {
            let path = s.path().to_path_buf();
            drop(s);
            BitcaskStorage::open(path).unwrap()
        }
    }

    let mut harness = Harness { dir: tempfile::tempdir().unwrap(), generation: 0 };
    kv_raft::testing::assert_storage_conformance(&mut harness);
}

#[test]
fn entries_are_readable_after_a_reopen_without_rewriting_hard_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = BitcaskStorage::open(dir.path()).unwrap();
        s.append(&[Entry { term: 3, index: 1, command: b"x".to_vec() }]).unwrap();
    }

    let s = BitcaskStorage::open(dir.path()).unwrap();
    assert_eq!(s.last_index().unwrap(), 1);
    assert_eq!(s.term(1).unwrap(), Some(3));
    assert_eq!(s.hard_state().unwrap(), HardState::default());
}
