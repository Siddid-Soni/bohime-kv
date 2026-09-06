use super::*;

#[test]
fn put_then_get_returns_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();

    engine.put(b"k", b"v").unwrap();

    assert_eq!(engine.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn overwrite_returns_latest_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();

    engine.put(b"k", b"v1").unwrap();
    engine.put(b"k", b"v2").unwrap();

    assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn reopen_replays_existing_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open(path).unwrap();
    engine.put(b"k1", b"v1").unwrap();
    engine.put(b"k2", b"v2").unwrap();
    drop(engine);

    let mut reopened = Engine::open(path).unwrap();
    assert_eq!(reopened.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(reopened.get(b"k2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn reopen_replays_overwrite_as_latest_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open(path).unwrap();
    engine.put(b"k", b"v1").unwrap();
    engine.put(b"k", b"v2").unwrap();
    drop(engine);

    let mut reopened = Engine::open(path).unwrap();
    assert_eq!(reopened.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn reopen_after_delete_stays_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open(path).unwrap();
    engine.put(b"k", b"v").unwrap();
    engine.delete(b"k").unwrap();
    drop(engine);

    let mut reopened = Engine::open(path).unwrap();
    assert_eq!(reopened.get(b"k").unwrap(), None);
}

#[test]
fn writes_after_reopen_append_correctly() {
    // Guards against replay leaving write_offset pointing at the wrong
    // place, which would silently corrupt the first post-reopen write.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open(path).unwrap();
    engine.put(b"k1", b"v1").unwrap();
    drop(engine);

    let mut reopened = Engine::open(path).unwrap();
    reopened.put(b"k2", b"v2").unwrap();

    assert_eq!(reopened.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(reopened.get(b"k2").unwrap(), Some(b"v2".to_vec()));

    drop(reopened);
    let mut reopened_again = Engine::open(path).unwrap();
    assert_eq!(reopened_again.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(reopened_again.get(b"k2").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn write_10k_keys_drop_reopen_all_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open(path).unwrap();
    for i in 0..10_000u32 {
        let key = format!("key-{i}").into_bytes();
        let value = format!("value-{i}").into_bytes();
        engine.put(&key, &value).unwrap();
    }
    drop(engine);

    let mut reopened = Engine::open(path).unwrap();
    for i in 0..10_000u32 {
        let key = format!("key-{i}").into_bytes();
        let expected = format!("value-{i}").into_bytes();
        assert_eq!(reopened.get(&key).unwrap(), Some(expected));
    }
}

fn segment_file_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir).unwrap().count()
}

#[test]
fn rotates_when_size_threshold_exceeded() {
    let dir = tempfile::tempdir().unwrap();
    // Each record here is a few dozen bytes; a 64-byte cap forces a
    // rotation well before all of them fit in one segment.
    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();

    for i in 0..20u32 {
        engine.put(format!("key-{i}").as_bytes(), format!("value-{i}").as_bytes()).unwrap();
    }

    assert!(
        segment_file_count(dir.path()) > 1,
        "expected multiple segment files, found {}",
        segment_file_count(dir.path())
    );
}

#[test]
fn reads_resolve_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();

    for i in 0..20u32 {
        engine.put(format!("key-{i}").as_bytes(), format!("value-{i}").as_bytes()).unwrap();
    }
    assert!(segment_file_count(dir.path()) > 1, "test setup should span multiple segments");

    for i in 0..20u32 {
        let expected = format!("value-{i}").into_bytes();
        assert_eq!(engine.get(format!("key-{i}").as_bytes()).unwrap(), Some(expected));
    }
}

#[test]
fn reopen_replays_across_multiple_segments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open_with_max_segment_size(path, 64).unwrap();
    for i in 0..20u32 {
        engine.put(format!("key-{i}").as_bytes(), format!("value-{i}").as_bytes()).unwrap();
    }
    assert!(segment_file_count(path) > 1, "test setup should span multiple segments");
    drop(engine);

    let mut reopened = Engine::open_with_max_segment_size(path, 64).unwrap();
    for i in 0..20u32 {
        let expected = format!("value-{i}").into_bytes();
        assert_eq!(reopened.get(format!("key-{i}").as_bytes()).unwrap(), Some(expected));
    }
}

#[test]
fn delete_in_later_segment_overrides_put_in_earlier_segment() {
    // If segments were replayed in the wrong order, this earlier put
    // would win over the later tombstone. Only correct if replay walks
    // segments in ascending SegmentId order.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    let mut engine = Engine::open_with_max_segment_size(path, 64).unwrap();
    engine.put(b"k", b"v").unwrap();
    for i in 0..20u32 {
        engine.put(format!("filler-{i}").as_bytes(), format!("filler-{i}").as_bytes()).unwrap();
    }
    assert!(segment_file_count(path) > 1, "test setup should span multiple segments");
    engine.delete(b"k").unwrap();
    assert_eq!(engine.get(b"k").unwrap(), None);
    drop(engine);

    let mut reopened = Engine::open_with_max_segment_size(path, 64).unwrap();
    assert_eq!(reopened.get(b"k").unwrap(), None);
}

#[test]
fn delete_then_get_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();

    engine.put(b"k", b"v").unwrap();
    engine.delete(b"k").unwrap();

    assert_eq!(engine.get(b"k").unwrap(), None);
}

#[test]
fn get_missing_key_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();

    assert_eq!(engine.get(b"never put").unwrap(), None);
}

// --- M1.5: hint files ---

#[test]
fn hint_file_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let entries = vec![
        (b"a".to_vec(), ValueLoc { segment_id: 3, offset: 0, len: 10 }),
        (b"bb".to_vec(), ValueLoc { segment_id: 3, offset: 10, len: 20 }),
    ];

    write_hint_file(dir.path(), 3, &entries).unwrap();
    let read_back = read_hint_file(dir.path(), 3).unwrap().unwrap();

    assert_eq!(read_back, entries);
}

#[test]
fn read_hint_file_returns_none_when_missing() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(read_hint_file(dir.path(), 7).unwrap(), None);
}

// --- M1.5: compaction ---

fn count_records_in_file(path: &std::path::Path) -> usize {
    let data = std::fs::read(path).unwrap();
    let mut count = 0;
    let mut rest = &data[..];
    while !rest.is_empty() {
        let (_, consumed) = Record::decode(rest).unwrap();
        rest = &rest[consumed..];
        count += 1;
    }
    count
}

#[test]
fn compact_with_only_active_segment_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    engine.put(b"k", b"v").unwrap();

    engine.compact().unwrap();

    assert_eq!(engine.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn compact_reduces_dead_records_to_live_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open_with_max_segment_size(dir.path(), 512).unwrap();

    // 100 overwrites each of 100 keys: 10k puts total, but only 100 keys
    // are ever live at once.
    for round in 0..100u32 {
        for key_idx in 0..100u32 {
            let key = format!("key-{key_idx}").into_bytes();
            let value = format!("value-{round}-{key_idx}").into_bytes();
            engine.put(&key, &value).unwrap();
        }
    }

    engine.compact().unwrap();

    for key_idx in 0..100u32 {
        let key = format!("key-{key_idx}").into_bytes();
        let expected = format!("value-99-{key_idx}").into_bytes();
        assert_eq!(engine.get(&key).unwrap(), Some(expected));
    }

    let total_records: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
        .map(|e| count_records_in_file(&e.path()))
        .sum();
    assert!(
        total_records < 300,
        "expected compaction to shrink to ~100 live records, found {total_records}"
    );
}

#[test]
fn reopen_after_compaction_uses_hint_and_matches_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut engine = Engine::open_with_max_segment_size(path, 256).unwrap();

    for round in 0..50u32 {
        for key_idx in 0..20u32 {
            let key = format!("key-{key_idx}").into_bytes();
            let value = format!("value-{round}-{key_idx}").into_bytes();
            engine.put(&key, &value).unwrap();
        }
    }
    engine.compact().unwrap();
    drop(engine);

    // Reopen normally: this exercises the hint-file fast path for whatever
    // segment(s) compaction produced.
    let mut reopened = Engine::open_with_max_segment_size(path, 256).unwrap();
    for key_idx in 0..20u32 {
        let key = format!("key-{key_idx}").into_bytes();
        let expected = format!("value-49-{key_idx}").into_bytes();
        assert_eq!(reopened.get(&key).unwrap(), Some(expected));
    }
    drop(reopened);

    // Delete every hint file so the next open() has no choice but to fall
    // back to a full segment scan, then confirm it lands on the same data.
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "hint") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    let mut scanned = Engine::open_with_max_segment_size(path, 256).unwrap();
    for key_idx in 0..20u32 {
        let key = format!("key-{key_idx}").into_bytes();
        let expected = format!("value-49-{key_idx}").into_bytes();
        assert_eq!(scanned.get(&key).unwrap(), Some(expected));
    }
}

#[test]
fn compact_does_not_resurrect_deleted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let mut engine = Engine::open_with_max_segment_size(path, 64).unwrap();

    engine.put(b"k", b"v").unwrap();
    for i in 0..20u32 {
        engine.put(format!("filler-{i}").as_bytes(), format!("filler-{i}").as_bytes()).unwrap();
    }
    engine.delete(b"k").unwrap();

    engine.compact().unwrap();
    assert_eq!(engine.get(b"k").unwrap(), None);

    drop(engine);
    let mut reopened = Engine::open_with_max_segment_size(path, 64).unwrap();
    assert_eq!(reopened.get(b"k").unwrap(), None);
}

#[test]
fn overwrite_during_compaction_keeps_new_value() {
    // Simulates the race compaction must survive: a write lands on a key
    // after compaction has already snapshotted its old location, but before
    // the relocation is applied to the index.
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();

    engine.put(b"k", b"old").unwrap();
    for i in 0..20u32 {
        engine.put(format!("filler-{i}").as_bytes(), format!("filler-{i}").as_bytes()).unwrap();
    }

    let plan = engine.plan_compaction().unwrap().expect("closed segments exist to compact");
    engine.put(b"k", b"new").unwrap();
    engine.apply_compaction(plan).unwrap();

    assert_eq!(engine.get(b"k").unwrap(), Some(b"new".to_vec()));
}

use proptest::strategy::Strategy;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

fn small_bytes() -> impl proptest::strategy::Strategy<Value = Vec<u8>> {
    proptest::collection::vec(proptest::prelude::any::<u8>(), 0..8)
}

// Keys are drawn from a small fixed alphabet so puts/deletes collide with
// each other often, rather than every op landing on a distinct key.
fn small_key() -> impl proptest::strategy::Strategy<Value = Vec<u8>> {
    proptest::sample::select(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
}

fn op_strategy() -> impl proptest::strategy::Strategy<Value = Op> {
    proptest::prop_oneof![
        (small_key(), small_bytes()).prop_map(|(k, v)| Op::Put(k, v)),
        small_key().prop_map(Op::Delete),
    ]
}

proptest::proptest! {
    #[test]
    fn matches_btreemap_model(ops in proptest::collection::vec(op_strategy(), 0..50)) {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(dir.path()).unwrap();
        let mut model: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = std::collections::BTreeMap::new();

        for op in &ops {
            match op {
                Op::Put(k, v) => {
                    engine.put(k, v).unwrap();
                    model.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    engine.delete(k).unwrap();
                    model.remove(k);
                }
            }
        }

        let touched_keys: std::collections::BTreeSet<&Vec<u8>> = ops
            .iter()
            .map(|op| match op {
                Op::Put(k, _) => k,
                Op::Delete(k) => k,
            })
            .collect();

        for key in touched_keys {
            let expected = model.get(key).cloned();
            let actual = engine.get(key).unwrap();
            proptest::prop_assert_eq!(actual, expected, "mismatch for key {:?}", key);
        }
    }
}
