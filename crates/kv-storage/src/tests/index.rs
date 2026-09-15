use crate::index::{HashMapIndex, KeyDirIndex, ValueLoc};

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
