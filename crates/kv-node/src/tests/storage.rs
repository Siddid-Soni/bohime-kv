use crate::storage::BitcaskStorage;
use kv_raft::storage::RaftStorage;
use kv_raft::types::{Entry, HardState};

struct ConformanceHarness {
    dir: tempfile::TempDir,
    generation: usize,
}

impl kv_raft::conformance::StorageHarness for ConformanceHarness {
    type Storage = BitcaskStorage;

    /// A fresh directory per store, so `create` never reopens a previous one.
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

kv_raft::raft_storage_conformance!(
    bitcask,
    ConformanceHarness { dir: tempfile::tempdir().unwrap(), generation: 0 }
);

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
