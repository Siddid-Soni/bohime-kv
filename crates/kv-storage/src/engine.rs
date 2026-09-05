//! Append-only Bitcask engine (M1.2). Single active file, in-memory index —
//! no rebuild-on-reopen (M1.3), rotation (M1.4), or compaction (M1.5) yet.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::record::Record;

struct ValueLoc {
    offset: u64,
    len: u32,
}

pub struct Engine {
    file: File,
    write_offset: u64,
    index: HashMap<Vec<u8>, ValueLoc>,
}

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).read(true).append(true).open(path)?;
        let write_offset = file.metadata()?.len();
        Ok(Self { file, write_offset, index: HashMap::new() })
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

    /// Removes `key` from the in-memory index. Note: this does not yet
    /// persist a tombstone to the log, so a deleted key reappears after
    /// reopen — that gap closes in M1.3, where replay-on-reopen needs it.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
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
