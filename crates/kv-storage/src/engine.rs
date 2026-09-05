//! Append-only Bitcask engine (M1.2-M1.4). `Engine` owns a directory of
//! numbered segment files; the keydir is rebuilt on open by replaying every
//! segment in order, and deletes persist as tombstones so they survive a
//! reopen. No compaction (M1.5) yet, so old segments' garbage just
//! accumulates until then.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::index::{HashMapIndex, KeyDirIndex, SegmentId, ValueLoc};
use crate::record::Record;

/// Arbitrary default; production tuning is out of scope for this milestone.
/// Tests that need to force rotation use `open_with_max_segment_size`.
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn segment_file_name(id: SegmentId) -> String {
    format!("{id:020}.seg")
}

fn parse_segment_id(file_name: &str) -> Option<SegmentId> {
    file_name.strip_suffix(".seg")?.parse().ok()
}

fn open_segment(dir: &Path, id: SegmentId) -> io::Result<File> {
    OpenOptions::new().create(true).read(true).append(true).open(dir.join(segment_file_name(id)))
}

/// Rebuilds the keydir by decoding every record in one segment file, in
/// order. A tombstone removes its key from the index instead of inserting
/// it; whichever record for a key is replayed last wins. Callers must
/// replay segments in ascending `SegmentId` order for that to be correct
/// across the whole log, not just within one file.
fn replay(mut file: &File, segment_id: SegmentId, index: &mut HashMapIndex) -> io::Result<()> {
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
            index.insert(record.key.clone(), ValueLoc { segment_id, offset, len: consumed as u32 });
        }

        offset += consumed as u64;
        rest = &rest[consumed..];
    }

    Ok(())
}

pub struct Engine {
    dir: PathBuf,
    max_segment_size: u64,
    active_id: SegmentId,
    active_offset: u64,
    segments: BTreeMap<SegmentId, File>,
    index: HashMapIndex,
}

impl Engine {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_max_segment_size(dir, DEFAULT_MAX_SEGMENT_SIZE)
    }

    pub fn open_with_max_segment_size(
        dir: impl AsRef<Path>,
        max_segment_size: u64,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut ids: Vec<SegmentId> = fs::read_dir(&dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().to_str().and_then(parse_segment_id))
            .collect();
        ids.sort_unstable();
        if ids.is_empty() {
            ids.push(0);
        }

        let mut segments = BTreeMap::new();
        for &id in &ids {
            segments.insert(id, open_segment(&dir, id)?);
        }

        let mut index = HashMapIndex::default();
        for &id in &ids {
            replay(&segments[&id], id, &mut index)?;
        }

        let active_id = *ids.last().expect("ids always has at least one entry");
        let active_offset = segments[&active_id].metadata()?.len();

        Ok(Self { dir, max_segment_size, active_id, active_offset, segments, index })
    }

    /// Writes `encoded` to the active segment, rotating to a fresh one first
    /// if it wouldn't fit under `max_segment_size`. Never rotates an empty
    /// active segment, so a single record larger than the threshold is still
    /// written rather than looping forever.
    fn append(&mut self, encoded: &[u8]) -> io::Result<ValueLoc> {
        let len = encoded.len() as u32;

        if self.active_offset > 0 && self.active_offset + len as u64 > self.max_segment_size {
            self.rotate()?;
        }

        let file = self.segments.get_mut(&self.active_id).expect("active segment always present");
        file.write_all(encoded)?;

        let loc = ValueLoc { segment_id: self.active_id, offset: self.active_offset, len };
        self.active_offset += len as u64;
        Ok(loc)
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.active_id += 1;
        let file = open_segment(&self.dir, self.active_id)?;
        self.segments.insert(self.active_id, file);
        self.active_offset = 0;
        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let record = Record::create(now_millis(), key.to_vec(), value.to_vec());
        let loc = self.append(&record.encode())?;
        self.index.insert(key.to_vec(), loc);
        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let Some(loc) = self.index.get(key) else {
            return Ok(None);
        };

        let file = self.segments.get_mut(&loc.segment_id).expect("indexed segment must be open");
        let mut buf = vec![0u8; loc.len as usize];
        file.seek(SeekFrom::Start(loc.offset))?;
        file.read_exact(&mut buf)?;

        let (record, _) =
            Record::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(record.value))
    }

    /// Writes a tombstone record so the deletion survives reopen, then
    /// removes `key` from the in-memory index.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let record = Record::tombstone(now_millis(), key.to_vec());
        self.append(&record.encode())?;
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
}
