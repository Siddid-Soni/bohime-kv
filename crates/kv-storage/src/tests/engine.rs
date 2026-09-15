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
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".seg"))
        .count()
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

    write_hint_tmp(dir.path(), 3, &entries).unwrap();
    std::fs::rename(
        dir.path().join(tmp_name(&hint_file_name(3))),
        dir.path().join(hint_file_name(3)),
    )
    .unwrap();
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

// --- M1.6: crash safety — torn tail (Task 1), tests only ---

use proptest::strategy::Strategy;

fn segment_path(dir: &std::path::Path, id: u32) -> std::path::PathBuf {
    dir.join(format!("{id:020}.seg"))
}

fn truncate_to(path: &std::path::Path, new_len: u64) {
    std::fs::OpenOptions::new().write(true).open(path).unwrap().set_len(new_len).unwrap();
}

fn file_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

#[test]
fn reopen_after_torn_tail_recovers_prior_records() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
    }

    let path = segment_path(dir.path(), 0);
    truncate_to(&path, file_len(&path) - 1);

    let mut engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), None, "torn record must not be resurrected");
}

#[test]
fn every_truncation_offset_in_the_final_record_recovers() {
    // The final record's encoded length: HEADER_LEN(21) + key + value.
    for cut in 1..=(21 + 1 + 2) {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut engine = Engine::open(dir.path()).unwrap();
            engine.put(b"a", b"11").unwrap();
            engine.put(b"b", b"22").unwrap();
        }

        let path = segment_path(dir.path(), 0);
        let intact_len = file_len(&path) - (21 + 1 + 2);
        truncate_to(&path, file_len(&path) - cut as u64);

        let mut engine = Engine::open(dir.path()).unwrap();
        assert_eq!(engine.get(b"a").unwrap(), Some(b"11".to_vec()), "cut={cut}");
        assert_eq!(engine.get(b"b").unwrap(), None, "cut={cut}");
        assert_eq!(file_len(&path), intact_len, "torn tail must be truncated away, cut={cut}");
    }
}

#[test]
fn write_after_torn_tail_recovery_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
    }

    let path = segment_path(dir.path(), 0);
    truncate_to(&path, file_len(&path) - 3);

    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(b"c", b"3").unwrap();
    }

    let mut engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn torn_tombstone_leaves_the_key_present() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.delete(b"a").unwrap();
    }

    let path = segment_path(dir.path(), 0);
    truncate_to(&path, file_len(&path) - 1);

    // The delete never became durable, so the key must still be there. A
    // half-written tombstone taking effect would be the engine inventing a
    // deletion the caller was never told had succeeded.
    let mut engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
}

#[test]
fn corrupt_byte_in_final_record_is_treated_as_a_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
    }

    let path = segment_path(dir.path(), 0);
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    let mut engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), None);
}

#[test]
fn trailing_garbage_is_truncated_away() {
    let dir = tempfile::tempdir().unwrap();
    let intact_len = {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(b"a", b"1").unwrap();
        file_len(&segment_path(dir.path(), 0))
    };

    let path = segment_path(dir.path(), 0);
    let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    std::io::Write::write_all(&mut file, &[0xde, 0xad, 0xbe, 0xef]).unwrap();
    drop(file);

    let mut engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(file_len(&path), intact_len);
}

#[test]
fn torn_closed_segment_is_rejected_rather_than_silently_truncated() {
    let dir = tempfile::tempdir().unwrap();
    {
        // A 64-byte cap forces rotation, so segment 0 closes.
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        for i in 0..20u32 {
            engine.put(format!("key-{i}").as_bytes(), format!("value-{i}").as_bytes()).unwrap();
        }
    }

    // Segment 0 is closed by construction. A crash cannot tear a closed
    // segment, so a tear here is real corruption of data that was already
    // durable — discarding it silently would be data loss dressed up as
    // recovery.
    let path = segment_path(dir.path(), 0);
    truncate_to(&path, file_len(&path) - 1);

    let err = match Engine::open_with_max_segment_size(dir.path(), 64) {
        Ok(_) => panic!("open should reject torn closed segment"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("segment 0"), "error must name the segment: {err}");
}

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

// --- M1.6 Task 2: compaction commit manifest ---

#[test]
fn crash_after_compaction_commit_point_completes_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        for _ in 0..3 {
            for i in 0..5u32 {
                engine.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        }
        engine.put(b"k0", b"final").unwrap();
    }

    let manifest = {
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        let plan = engine.plan_compaction().unwrap().expect("something to compact");
        let manifest = CompactionManifest {
            new_id: Some(plan.new_segment_id),
            old_ids: plan.old_segment_ids.clone(),
        };
        engine.apply_compaction(plan).unwrap();
        manifest
    };
    write_manifest(dir.path(), &manifest).unwrap();

    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
    assert_eq!(engine.get(b"k0").unwrap(), Some(b"final".to_vec()));
    for i in 1..5u32 {
        let key = format!("k{i}").into_bytes();
        assert_eq!(engine.get(&key).unwrap(), Some(format!("v{i}").into_bytes()));
    }
    assert!(!dir.path().join(MANIFEST_NAME).exists(), "manifest must be cleared once replayed");
}

#[test]
fn stale_retired_segment_left_by_a_crash_cannot_win_over_the_merge() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        for i in 0..8u32 {
            engine.put(b"k", format!("v{i}").as_bytes()).unwrap();
        }
        engine.put(b"k", b"newest").unwrap();
    }

    let before: Vec<(std::path::PathBuf, Vec<u8>)> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .map(|p| (p.clone(), std::fs::read(&p).unwrap()))
        .collect();

    let manifest = {
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        let plan = engine.plan_compaction().unwrap().expect("something to compact");
        let manifest = CompactionManifest {
            new_id: Some(plan.new_segment_id),
            old_ids: plan.old_segment_ids.clone(),
        };
        engine.apply_compaction(plan).unwrap();
        manifest
    };
    for (path, bytes) in before {
        if !path.exists() {
            std::fs::write(&path, &bytes).unwrap();
        }
    }
    write_manifest(dir.path(), &manifest).unwrap();

    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
    assert_eq!(
        engine.get(b"k").unwrap(),
        Some(b"newest".to_vec()),
        "restored stale segments must not replay over the merged segment"
    );
}

#[test]
fn crash_before_compaction_commit_point_leaves_the_pre_compaction_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        for i in 0..8u32 {
            engine.put(b"k", format!("v{i}").as_bytes()).unwrap();
        }
    }

    std::fs::write(dir.path().join("00000000000000000000.seg.tmp"), b"garbage").unwrap();
    std::fs::write(dir.path().join("00000000000000000000.hint.tmp"), b"garbage").unwrap();

    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(b"v7".to_vec()));
    assert!(!dir.path().join("00000000000000000000.seg.tmp").exists(), "orphans cleared");
    assert!(!dir.path().join("00000000000000000000.hint.tmp").exists(), "orphans cleared");
}

#[test]
fn malformed_hint_file_falls_back_to_full_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
        for i in 0..8u32 {
            engine.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
        }
        engine.compact().unwrap();
    }

    let hint = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "hint"))
        .expect("compaction writes a hint file");
    std::fs::write(&hint, b"\x00\x01\x02").unwrap();

    let mut engine = Engine::open_with_max_segment_size(dir.path(), 64).unwrap();
    for i in 0..8u32 {
        let key = format!("k{i}").into_bytes();
        assert_eq!(
            engine.get(&key).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "a bad hint must degrade to a full replay, not a failed open"
        );
    }
}
