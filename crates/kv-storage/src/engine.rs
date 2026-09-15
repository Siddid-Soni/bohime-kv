//! Append-only Bitcask engine (M1.2-M1.5). `Engine` owns a directory of
//! numbered segment files; the keydir is rebuilt on open by replaying every
//! segment (or, where a hint file exists, loading it directly) in ascending
//! id order, and deletes persist as tombstones so they survive a reopen.
//! `compact()` merges every closed segment's still-live data into one new
//! segment plus a hint file, so a future reopen of that segment doesn't have
//! to re-read every value to rebuild the keydir.

use std::collections::{BTreeMap, HashSet};
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

fn hint_file_name(id: SegmentId) -> String {
    format!("{id:020}.hint")
}

/// A hint file's decoded contents: one `(key, location)` pair per live
/// record in the segment it describes.
type HintEntries = Vec<(Vec<u8>, ValueLoc)>;

/// Hint entries describe exactly what a compacted segment holds on disk —
/// `offset(8) | len(4) | key_len(4) | key` back to back, no crc/timestamp/
/// value — so a reopen can rebuild the keydir for that segment without
/// reading a single value.
fn write_hint_file(dir: &Path, id: SegmentId, entries: &[(Vec<u8>, ValueLoc)]) -> io::Result<()> {
    let mut buf = Vec::new();
    for (key, loc) in entries {
        buf.extend_from_slice(&loc.offset.to_be_bytes());
        buf.extend_from_slice(&loc.len.to_be_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
    }
    fs::write(dir.join(hint_file_name(id)), buf)
}

/// Returns `None` if segment `id` has no hint file (nothing written for it
/// yet, or it has never been compacted) — callers fall back to `replay`.
fn read_hint_file(dir: &Path, id: SegmentId) -> io::Result<Option<HintEntries>> {
    let path = dir.join(hint_file_name(id));
    if !path.exists() {
        return Ok(None);
    }

    let data = fs::read(path)?;
    let mut entries = Vec::new();
    let mut rest = &data[..];
    while !rest.is_empty() {
        if rest.len() < 16 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated hint entry header"));
        }
        let offset = u64::from_be_bytes(rest[0..8].try_into().unwrap());
        let len = u32::from_be_bytes(rest[8..12].try_into().unwrap());
        let key_len = u32::from_be_bytes(rest[12..16].try_into().unwrap()) as usize;

        if rest.len() < 16 + key_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated hint entry key"));
        }
        let key = rest[16..16 + key_len].to_vec();

        entries.push((key, ValueLoc { segment_id: id, offset, len }));
        rest = &rest[16 + key_len..];
    }
    Ok(Some(entries))
}

/// Rebuilds the keydir by decoding records from one segment file, in order,
/// and returns the byte length of the prefix that decoded cleanly.
///
/// Decoding stops at the first record that fails rather than erroring,
/// because a torn tail is the expected state of the active segment after a
/// crash. A tombstone removes its key from the index instead of inserting
/// it; whichever record for a key is replayed last wins. Callers must
/// replay segments in ascending `SegmentId` order for that to be correct
/// across the whole log, not just within one file.
fn replay(mut file: &File, segment_id: SegmentId, index: &mut HashMapIndex) -> io::Result<u64> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let mut offset = 0u64;
    let mut rest = &buf[..];
    while !rest.is_empty() {
        if let Ok((record, consumed)) = Record::decode(rest) {
            if record.is_tombstone {
                index.remove(&record.key);
            } else {
                index.insert(
                    record.key.clone(),
                    ValueLoc { segment_id, offset, len: consumed as u32 },
                );
            }

            offset += consumed as u64;
            rest = &rest[consumed..];
        } else {
            break;
        }
    }

    Ok(offset)
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

        let active_id = *ids.last().expect("ids always has at least one entry");

        let mut index = HashMapIndex::default();
        for &id in &ids {
            if id != active_id
                && let Some(hints) = read_hint_file(&dir, id)?
            {
                for (key, loc) in hints {
                    index.insert(key, loc);
                }
                continue;
            }

            let valid_len = replay(&segments[&id], id, &mut index)?;
            let on_disk_len = segments[&id].metadata()?.len();
            if valid_len == on_disk_len {
                continue;
            }

            if id != active_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment {id} is corrupt at offset {valid_len} ({} trailing bytes do not decode)",
                        on_disk_len - valid_len
                    ),
                ));
            }

            tracing::warn!(
                segment = id,
                valid_len,
                discarded = on_disk_len - valid_len,
                "discarding torn tail of active segment after unclean shutdown"
            );
            segments
                .get_mut(&id)
                .expect("segment just replayed must be open")
                .set_len(valid_len)?;
        }

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
        Ok(Some(self.read_value_at(loc)?))
    }

    fn read_value_at(&mut self, loc: ValueLoc) -> io::Result<Vec<u8>> {
        let file = self.segments.get_mut(&loc.segment_id).expect("indexed segment must be open");
        let mut buf = vec![0u8; loc.len as usize];
        file.seek(SeekFrom::Start(loc.offset))?;
        file.read_exact(&mut buf)?;

        let (record, _) =
            Record::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(record.value)
    }

    /// Writes a tombstone record so the deletion survives reopen, then
    /// removes `key` from the in-memory index.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let record = Record::tombstone(now_millis(), key.to_vec());
        self.append(&record.encode())?;
        self.index.remove(key);
        Ok(())
    }

    /// Merges every closed (non-active) segment's still-live data into one
    /// new segment plus a hint file, and deletes the old segments. A no-op
    /// if there is nothing but the active segment.
    pub fn compact(&mut self) -> io::Result<()> {
        if let Some(plan) = self.plan_compaction()? {
            self.apply_compaction(plan)?;
        }
        Ok(())
    }

    /// Snapshots which keys are currently live in closed segments and reads
    /// their values, without mutating any state. Split out from
    /// `apply_compaction` so a caller can force an overwrite to land between
    /// the snapshot and the relocation it drives — the exact race compaction
    /// must not lose to once a real concurrent writer exists (M11.5).
    fn plan_compaction(&mut self) -> io::Result<Option<CompactionPlan>> {
        let old_segment_ids: Vec<SegmentId> =
            self.segments.keys().copied().filter(|&id| id != self.active_id).collect();
        if old_segment_ids.is_empty() {
            return Ok(None);
        }

        let old_set: HashSet<SegmentId> = old_segment_ids.iter().copied().collect();
        let mut live: Vec<(Vec<u8>, ValueLoc)> = self
            .index
            .iter()
            .into_iter()
            .filter(|(_, loc)| old_set.contains(&loc.segment_id))
            .collect();
        live.sort_by(|a, b| a.0.cmp(&b.0));

        let mut entries = Vec::with_capacity(live.len());
        for (key, loc) in live {
            let value = self.read_value_at(loc)?;
            entries.push((key, loc, value));
        }

        let new_segment_id = *old_segment_ids.iter().min().expect("checked non-empty above");
        Ok(Some(CompactionPlan { old_segment_ids, new_segment_id, entries }))
    }

    /// Writes the plan's live entries into one new segment (reusing the
    /// smallest retired segment id so it still sorts before the active
    /// segment) plus its hint file, relocates the index — skipping any key
    /// overwritten since the plan was taken — then deletes the other old
    /// segment files.
    fn apply_compaction(&mut self, plan: CompactionPlan) -> io::Result<()> {
        let CompactionPlan { old_segment_ids, new_segment_id, entries } = plan;

        if entries.is_empty() {
            for id in &old_segment_ids {
                self.segments.remove(id);
                let _ = fs::remove_file(self.dir.join(segment_file_name(*id)));
                let _ = fs::remove_file(self.dir.join(hint_file_name(*id)));
            }
            return Ok(());
        }

        let mut hint_entries = Vec::with_capacity(entries.len());
        let mut relocations = Vec::with_capacity(entries.len());
        {
            let file = self
                .segments
                .get_mut(&new_segment_id)
                .expect("new_segment_id must be an open segment");
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;

            let mut offset = 0u64;
            for (key, old_loc, value) in &entries {
                let record = Record::create(now_millis(), key.clone(), value.clone());
                let encoded = record.encode();
                file.write_all(&encoded)?;

                let new_loc =
                    ValueLoc { segment_id: new_segment_id, offset, len: encoded.len() as u32 };
                hint_entries.push((key.clone(), new_loc));
                relocations.push((key.clone(), *old_loc, new_loc));
                offset += encoded.len() as u64;
            }
        }

        for (key, old_loc, new_loc) in relocations {
            self.index.relocate(&key, old_loc, new_loc);
        }
        write_hint_file(&self.dir, new_segment_id, &hint_entries)?;

        for id in old_segment_ids {
            if id != new_segment_id {
                self.segments.remove(&id);
                let _ = fs::remove_file(self.dir.join(segment_file_name(id)));
                let _ = fs::remove_file(self.dir.join(hint_file_name(id)));
            }
        }

        Ok(())
    }
}

struct CompactionPlan {
    old_segment_ids: Vec<SegmentId>,
    new_segment_id: SegmentId,
    entries: Vec<(Vec<u8>, ValueLoc, Vec<u8>)>,
}

#[cfg(test)]
#[path = "tests/engine.rs"]
mod tests;
