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
    /// Not called by production code until M1.5's compaction step.
    #[allow(dead_code)]
    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool;

    /// Enumerates all live entries. Not called by production code until
    /// M1.5's compaction step needs to find what's still live in a segment.
    #[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_after_insert_returns_loc() {
        let mut index = HashMapIndex::default();
        let loc = ValueLoc { segment_id: 0, offset: 10, len: 5 };

        index.insert(b"k".to_vec(), loc);

        assert_eq!(index.get(b"k"), Some(loc));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let index = HashMapIndex::default();
        assert_eq!(index.get(b"missing"), None);
    }

    #[test]
    fn remove_deletes_entry_and_returns_it() {
        let mut index = HashMapIndex::default();
        let loc = ValueLoc { segment_id: 0, offset: 10, len: 5 };
        index.insert(b"k".to_vec(), loc);

        let removed = index.remove(b"k");

        assert_eq!(removed, Some(loc));
        assert_eq!(index.get(b"k"), None);
    }

    #[test]
    fn relocate_applies_when_old_loc_matches() {
        let mut index = HashMapIndex::default();
        let old = ValueLoc { segment_id: 0, offset: 10, len: 5 };
        let new = ValueLoc { segment_id: 0, offset: 100, len: 5 };
        index.insert(b"k".to_vec(), old);

        let applied = index.relocate(b"k", old, new);

        assert!(applied);
        assert_eq!(index.get(b"k"), Some(new));
    }

    #[test]
    fn relocate_is_noop_when_old_loc_is_stale() {
        // Simulates: compaction read `old`, but the key was overwritten to
        // `current` in the meantime. The relocation must not clobber that.
        let mut index = HashMapIndex::default();
        let old = ValueLoc { segment_id: 0, offset: 10, len: 5 };
        let current = ValueLoc { segment_id: 0, offset: 200, len: 7 };
        let compaction_target = ValueLoc { segment_id: 0, offset: 100, len: 5 };
        index.insert(b"k".to_vec(), current);

        let applied = index.relocate(b"k", old, compaction_target);

        assert!(!applied);
        assert_eq!(index.get(b"k"), Some(current));
    }

    #[test]
    fn iter_yields_all_entries() {
        let mut index = HashMapIndex::default();
        index.insert(b"a".to_vec(), ValueLoc { segment_id: 0, offset: 0, len: 1 });
        index.insert(b"b".to_vec(), ValueLoc { segment_id: 0, offset: 1, len: 2 });

        let mut entries = index.iter();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(
            entries,
            vec![
                (b"a".to_vec(), ValueLoc { segment_id: 0, offset: 0, len: 1 }),
                (b"b".to_vec(), ValueLoc { segment_id: 0, offset: 1, len: 2 }),
            ]
        );
    }
}
