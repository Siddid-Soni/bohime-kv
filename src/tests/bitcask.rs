use crate::bitcask::Bitcask;
use std::io::Write;

#[test]
fn put_get_delete() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Bitcask::open(&dir.path().join("db")).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"a", b"2").unwrap();
    db.put(b"b", b"").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(vec![]));
    db.delete(b"a").unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    assert_eq!(db.get(b"missing").unwrap(), None);
}

#[test]
fn reopen_replays_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = Bitcask::open(&path).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.delete(b"b").unwrap();
    db.sync().unwrap();
    drop(db);
    let db = Bitcask::open(&path).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
}

#[test]
fn a_torn_tail_is_dropped_and_writing_resumes_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = Bitcask::open(&path).unwrap();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    drop(db);
    // Half a record, as a crash mid-write leaves it.
    std::fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&[7; 9]).unwrap();
    let mut db = Bitcask::open(&path).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();
    drop(db);
    let db = Bitcask::open(&path).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn puts_are_buffered_until_flushed_and_readable_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = Bitcask::open(&path).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0, "nothing written before flush");
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    db.flush().unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
    db.put(b"c", b"3").unwrap();
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
    db.sync().unwrap();
    drop(db);
    let db = Bitcask::open(&path).unwrap();
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}
