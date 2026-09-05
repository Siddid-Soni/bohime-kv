//! Append-only Bitcask engine (M1.2/M1.3). Single active file; the keydir is
//! rebuilt on open by replaying the log, and deletes persist as tombstones
//! so they survive a reopen. No rotation (M1.4) or compaction (M1.5) yet.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::index::{HashMapIndex, KeyDirIndex, ValueLoc};
use crate::record::Record;

pub struct Engine {
    file: File,
    write_offset: u64,
    index: HashMapIndex,
}

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Rebuilds the keydir by decoding every record from the start of the file.
/// A tombstone removes its key from the index instead of inserting it;
/// whichever record for a key comes last in the file wins, which is exactly
/// what a fresh in-order replay produces naturally. Returns the offset one
/// past the last record, i.e. where the next `put`/`delete` should append.
fn replay(mut file: &File, index: &mut HashMapIndex) -> io::Result<u64> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let mut offset = 0u64;
    let mut rest = &buf[..];
    while !rest.is_empty() {
        let (record, consumed) =
            Record::decode(rest).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        if record.is_tombstone {
            index.remove(&record.key);
        } else {
            index.insert(record.key.clone(), ValueLoc { offset, len: consumed as u32 });
        }

        offset += consumed as u64;
        rest = &rest[consumed..];
    }

    Ok(offset)
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).read(true).append(true).open(path)?;

        let mut index = HashMapIndex::default();
        let write_offset = replay(&file, &mut index)?;

        Ok(Self { file, write_offset, index })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let record = Record::create(now_millis(), key.to_vec(), value.to_vec());
        let encoded = record.encode();
        let len = encoded.len() as u32;

        self.file.write_all(&encoded)?;

        self.index.insert(key.to_vec(), ValueLoc { offset: self.write_offset, len });
        self.write_offset += len as u64;
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let Some(loc) = self.index.get(key) else {
            return Ok(None);
        };

        let mut buf = vec![0u8; loc.len as usize];
        self.file.seek(SeekFrom::Start(loc.offset))?;
        self.file.read_exact(&mut buf)?;

        let (record, _) =
            Record::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(record.value))
    }

    /// Writes a tombstone record so the deletion survives reopen, then
    /// removes `key` from the in-memory index.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let record = Record::tombstone(now_millis(), key.to_vec());
        let encoded = record.encode();
        self.file.write_all(&encoded)?;
        self.write_offset += encoded.len() as u64;

        self.index.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(dir.path().join("data.log")).unwrap();

        engine.put(b"k", b"v").unwrap();

        assert_eq!(engine.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn overwrite_returns_latest_value() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(dir.path().join("data.log")).unwrap();

        engine.put(b"k", b"v1").unwrap();
        engine.put(b"k", b"v2").unwrap();

        assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn reopen_replays_existing_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.log");

        let mut engine = Engine::open(&path).unwrap();
        engine.put(b"k1", b"v1").unwrap();
        engine.put(b"k2", b"v2").unwrap();
        drop(engine);

        let mut reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(reopened.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn reopen_replays_overwrite_as_latest_value() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.log");

        let mut engine = Engine::open(&path).unwrap();
        engine.put(b"k", b"v1").unwrap();
        engine.put(b"k", b"v2").unwrap();
        drop(engine);

        let mut reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn reopen_after_delete_stays_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.log");

        let mut engine = Engine::open(&path).unwrap();
        engine.put(b"k", b"v").unwrap();
        engine.delete(b"k").unwrap();
        drop(engine);

        let mut reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.get(b"k").unwrap(), None);
    }

    #[test]
    fn writes_after_reopen_append_correctly() {
        // Guards against replay leaving write_offset pointing at the wrong
        // place, which would silently corrupt the first post-reopen write.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.log");

        let mut engine = Engine::open(&path).unwrap();
        engine.put(b"k1", b"v1").unwrap();
        drop(engine);

        let mut reopened = Engine::open(&path).unwrap();
        reopened.put(b"k2", b"v2").unwrap();

        assert_eq!(reopened.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(reopened.get(b"k2").unwrap(), Some(b"v2".to_vec()));

        drop(reopened);
        let mut reopened_again = Engine::open(&path).unwrap();
        assert_eq!(reopened_again.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(reopened_again.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn write_10k_keys_drop_reopen_all_readable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.log");

        let mut engine = Engine::open(&path).unwrap();
        for i in 0..10_000u32 {
            let key = format!("key-{i}").into_bytes();
            let value = format!("value-{i}").into_bytes();
            engine.put(&key, &value).unwrap();
        }
        drop(engine);

        let mut reopened = Engine::open(&path).unwrap();
        for i in 0..10_000u32 {
            let key = format!("key-{i}").into_bytes();
            let expected = format!("value-{i}").into_bytes();
            assert_eq!(reopened.get(&key).unwrap(), Some(expected));
        }
    }

    #[test]
    fn delete_then_get_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(dir.path().join("data.log")).unwrap();

        engine.put(b"k", b"v").unwrap();
        engine.delete(b"k").unwrap();

        assert_eq!(engine.get(b"k").unwrap(), None);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::open(dir.path().join("data.log")).unwrap();

        assert_eq!(engine.get(b"never put").unwrap(), None);
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
            let mut engine = Engine::open(dir.path().join("data.log")).unwrap();
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
}
