//! Live-entry enumeration for M8 snapshots: the whole state image is a scan
//! of what the keydir still calls live.

use crate::Engine;

#[test]
fn scan_returns_all_live_pairs_and_skips_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    engine.put(b"a", b"1").unwrap();
    engine.put(b"b", b"2").unwrap();
    engine.put(b"\x00reserved", b"r").unwrap();
    engine.delete(b"b").unwrap();

    let mut pairs = engine.scan().unwrap();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![(b"\x00reserved".to_vec(), b"r".to_vec()), (b"a".to_vec(), b"1".to_vec()),],
        "tombstones stay out; reserved keys stay in — the session table rides along"
    );
}

#[test]
fn scan_sees_each_overwritten_key_once_with_its_latest_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    engine.put(b"k", b"old").unwrap();
    engine.put(b"k", b"new").unwrap();

    assert_eq!(engine.scan().unwrap(), vec![(b"k".to_vec(), b"new".to_vec())]);
}
