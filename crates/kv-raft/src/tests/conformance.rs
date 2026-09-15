//! `MemStorage` against the shared `RaftStorage` conformance suite — the same
//! assertions `kv-node` runs against `BitcaskStorage`.

use crate::conformance::StorageHarness;
use crate::storage::MemStorage;

struct Harness;

impl StorageHarness for Harness {
    type Storage = MemStorage;

    fn create(&mut self) -> MemStorage {
        MemStorage::default()
    }

    /// In-memory storage has nothing to round-trip, so a reopen is identity.
    fn reopen(&mut self, s: MemStorage) -> MemStorage {
        s
    }
}

crate::raft_storage_conformance!(mem_storage, Harness);
