//! The keydir abstraction (plan §1.11/M1.3, M11.5). A plain `HashMap` today;
//! this trait is the seam `left-right` swaps into at M11.5 without touching
//! any call site in `engine.rs`.

use std::collections::HashMap;

pub(crate) type SegmentId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueLoc {
    pub(crate) segment_id: SegmentId,
    pub(crate) offset: u64,
    pub(crate) len: u32,
}

pub(crate) trait KeyDirIndex {
    fn get(&self, key: &[u8]) -> Option<ValueLoc>;
    fn insert(&mut self, key: Vec<u8>, loc: ValueLoc);
    fn remove(&mut self, key: &[u8]) -> Option<ValueLoc>;

    /// Applies a compaction-driven move, but only if the entry still points
    /// at `old` — if the key was overwritten since compaction read it, the
    /// newer entry must win. Returns whether the relocation was applied.
    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool;

    /// Enumerates all live entries; compaction uses this to find what's
    /// still live across the segments it is about to retire.
    fn iter(&self) -> Vec<(Vec<u8>, ValueLoc)>;
}

#[derive(Debug, Default)]
pub(crate) struct HashMapIndex {
    map: HashMap<Vec<u8>, ValueLoc>,
}

impl KeyDirIndex for HashMapIndex {
    fn get(&self, key: &[u8]) -> Option<ValueLoc> {
        self.map.get(key).copied()
    }

    fn insert(&mut self, key: Vec<u8>, loc: ValueLoc) {
        self.map.insert(key, loc);
    }

    fn remove(&mut self, key: &[u8]) -> Option<ValueLoc> {
        self.map.remove(key)
    }

    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool {
        match self.map.get(key) {
            Some(current) if *current == old => {
                self.map.insert(key.to_vec(), new);
                true
            }
            _ => false,
        }
    }

    fn iter(&self) -> Vec<(Vec<u8>, ValueLoc)> {
        self.map.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }
}
