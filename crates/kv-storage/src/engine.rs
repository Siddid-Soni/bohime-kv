//! Append-only Bitcask engine (M1.2-M1.6). `Engine` owns a directory of
//! numbered segment files; the keydir is rebuilt on open by replaying every
//! segment (or, where a hint file exists, loading it directly) in ascending
//! id order, and deletes persist as tombstones so they survive a reopen.
//! `compact()` merges every closed segment's still-live data into one new
//! segment plus a hint file, so a future reopen of that segment doesn't have
//! to re-read every value to rebuild the keydir.
//!
//! Crash recovery (M1.6): a torn tail on the active segment is truncated away
//! on open, and an interrupted compaction is either completed or discarded
//! wholesale depending on whether it reached its manifest commit point. A
//! torn *closed* segment is rejected rather than truncated — a crash cannot
//! produce one, so it means real corruption.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
// pread. Linux-only by design: the project targets Linux and CI runs it there.
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::EngineConfig;
use crate::index::{Index, KeyDirIndex, KeyDirRead, KeyDirReadFactory, SegmentId, ValueLoc};
use crate::record::Record;

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

pub(crate) fn hint_file_name(id: SegmentId) -> String {
    format!("{id:020}.hint")
}

/// A hint file's decoded contents: one `(key, location)` pair per live
/// record in the segment it describes.
type HintEntries = Vec<(Vec<u8>, ValueLoc)>;

/// Writes a segment's hint entries to `{id}.hint.tmp`. The rename into place
/// is `finish_compaction`'s job, so a hint file never exists describing a
/// segment that was not also renamed into place.
pub(crate) fn write_hint_tmp(
    dir: &Path,
    id: SegmentId,
    entries: &[(Vec<u8>, ValueLoc)],
) -> io::Result<()> {
    let mut buf = Vec::new();
    for (key, loc) in entries {
        buf.extend_from_slice(&loc.offset.to_be_bytes());
        buf.extend_from_slice(&loc.len.to_be_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
    }
    fs::write(dir.join(tmp_name(&hint_file_name(id))), buf)
}

/// Returns `None` if segment `id` has no hint file, or if the one it has does
/// not parse — callers fall back to `replay`, which rebuilds the same keydir
/// from the segment itself. A hint file is a cache of information the segment
/// already contains, so a bad one is a performance problem, never a
/// correctness one, and must not fail the open.
pub(crate) fn read_hint_file(dir: &Path, id: SegmentId) -> io::Result<Option<HintEntries>> {
    let path = dir.join(hint_file_name(id));
    if !path.exists() {
        return Ok(None);
    }

    let data = fs::read(path)?;
    match parse_hint_entries(&data, id) {
        Some(entries) => Ok(Some(entries)),
        None => {
            tracing::warn!(segment = id, "hint file did not parse; falling back to full replay");
            Ok(None)
        }
    }
}

fn parse_hint_entries(data: &[u8], id: SegmentId) -> Option<HintEntries> {
    let mut entries = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        if rest.len() < 16 {
            return None;
        }
        let offset = u64::from_be_bytes(rest[0..8].try_into().unwrap());
        let len = u32::from_be_bytes(rest[8..12].try_into().unwrap());
        let key_len = u32::from_be_bytes(rest[12..16].try_into().unwrap()) as usize;

        if rest.len() < 16 + key_len {
            return None;
        }
        let key = rest[16..16 + key_len].to_vec();

        entries.push((key, ValueLoc { segment_id: id, offset, len }));
        rest = &rest[16 + key_len..];
    }
    Some(entries)
}

/// Names a compaction that has passed its commit point. Its presence in the
/// directory means the merged segment is fully written under a `.tmp` name
/// and the retired segments may be unlinked; `open` replays the remaining
/// steps before touching anything else, which is what makes an interrupted
/// compaction safe rather than corrupting.
pub(crate) const MANIFEST_NAME: &str = "compaction.manifest";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct CompactionManifest {
    /// `None` when every retired segment was entirely dead, so there is no
    /// merged output at all — only deletions to finish.
    pub(crate) new_id: Option<SegmentId>,
    pub(crate) old_ids: Vec<SegmentId>,
}

pub(crate) fn tmp_name(name: &str) -> String {
    format!("{name}.tmp")
}

/// Writes the manifest to a temporary file and renames it into place, so it
/// appears atomically. This rename is the compaction's commit point.
pub(crate) fn write_manifest(dir: &Path, manifest: &CompactionManifest) -> io::Result<()> {
    let encoded =
        bincode::serialize(manifest).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp = dir.join(tmp_name(MANIFEST_NAME));
    fs::write(&tmp, encoded)?;
    fs::rename(tmp, dir.join(MANIFEST_NAME))
}

fn read_manifest(dir: &Path) -> io::Result<Option<CompactionManifest>> {
    let path = dir.join(MANIFEST_NAME);
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read(&path)?;
    match bincode::deserialize(&data) {
        Ok(manifest) => Ok(Some(manifest)),
        Err(e) => {
            tracing::warn!(error = %e, "ignoring unreadable compaction manifest");
            let _ = fs::remove_file(&path);
            Ok(None)
        }
    }
}

/// Completes a committed compaction. Idempotent: every step is skipped if it
/// has already happened, so it is safe to run on every open and safe to be
/// interrupted and run again.
fn finish_compaction(dir: &Path, manifest: &CompactionManifest) -> io::Result<()> {
    if let Some(new_id) = manifest.new_id {
        let seg = segment_file_name(new_id);
        let seg_tmp = dir.join(tmp_name(&seg));
        if seg_tmp.exists() {
            fs::rename(seg_tmp, dir.join(&seg))?;
        }
        let hint = hint_file_name(new_id);
        let hint_tmp = dir.join(tmp_name(&hint));
        if hint_tmp.exists() {
            fs::rename(hint_tmp, dir.join(&hint))?;
        }
    }

    for &id in &manifest.old_ids {
        if Some(id) != manifest.new_id {
            let _ = fs::remove_file(dir.join(segment_file_name(id)));
            let _ = fs::remove_file(dir.join(hint_file_name(id)));
        }
    }

    fs::remove_file(dir.join(MANIFEST_NAME))
}

/// Removes `.tmp` files left by a compaction that never reached its commit
/// point. Called only when no manifest exists, so these are always orphans.
fn clear_orphan_tmp_files(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().ends_with(".tmp") {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
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
fn replay(mut file: &File, segment_id: SegmentId, index: &mut Index) -> io::Result<u64> {
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

/// The engine's open segment files. Held behind an `Arc` so a reader can take
/// a snapshot of it without locking; `File` needs no `&mut` to be read from,
/// since reads use pread.
///
/// One handle per segment. A previous revision opened each segment several
/// times so concurrent readers would not share a kernel `struct file` — the
/// `f_count` that `fget` bumps on every syscall. That was a workaround for the
/// blocking-read model and is gone with it: `io_uring` submissions against
/// registered files never take that path. The measurements that motivated it
/// are in `docs/superpowers/plans/2026-09-17-read-path-handoff.md`, and remain
/// relevant to the pread fallback if it ever becomes the common path.
pub(crate) type SegmentMap = BTreeMap<SegmentId, Arc<File>>;

/// A read that has been resolved against the keydir but not yet performed.
///
/// This is the seam that lets a value read leave the thread owning the
/// engine. The keydir lookup — the part that races with writes — stays on the
/// owner; what crosses the boundary is an offset and a snapshot of the open
/// segment handles, neither of which any writer mutates in place. A segment
/// retired by compaction after this was handed out is unlinked but still
/// open, so the bytes remain readable.
///
/// `Send`, deliberately: handing it to a blocking pool is the entire point.
pub struct ValueRef {
    loc: ValueLoc,
    segments: Arc<SegmentMap>,
}

impl ValueRef {
    /// The descriptor holding this value.
    ///
    /// Valid only while this `ValueRef` lives: it keeps the segment map alive,
    /// and that is what holds the descriptor open. An I/O engine that submits
    /// against this fd must therefore keep the `ValueRef` until the read
    /// completes, not merely until it is submitted. A segment retired by
    /// compaction in the meantime is unlinked but still open, so the read
    /// still returns the right bytes.
    pub fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.segments.get(&self.loc.segment_id).expect("indexed segment must be open").as_raw_fd()
    }

    /// Byte offset of the record within its segment.
    pub fn offset(&self) -> u64 {
        self.loc.offset
    }

    /// Exact byte length to read. The record is self-delimiting, so a short
    /// read cannot be detected by decoding alone — the caller must read this
    /// many bytes.
    pub fn len(&self) -> usize {
        self.loc.len as usize
    }

    /// Whether this value is zero-length. Present because clippy asks for it
    /// alongside `len`; a stored record always has a header, so this is never
    /// true in practice.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Turns bytes read at `(raw_fd, offset, len)` into the record's value.
    /// Separate from `read` so an external I/O engine can own the transfer and
    /// still share the decoding — including the CRC check, which is the reason
    /// this must not be reimplemented by callers.
    pub fn decode(&self, buf: &[u8]) -> io::Result<Vec<u8>> {
        let (record, _) =
            Record::decode(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(record.value)
    }

    /// Performs the disk read. Blocking, and meant to be: callers on an async
    /// runtime should run it somewhere that tolerates blocking.
    pub fn read(&self) -> io::Result<Vec<u8>> {
        read_value_at(&self.segments, self.loc)
    }
}

/// Shared by `Engine::get` and `ValueRef::read` so both resolve a location the
/// same way.
///
/// `read_exact_at` (pread), not `seek` + `read_exact`: the offset travels with
/// the call instead of living in the file's cursor, so this needs only `&File`
/// and any number of threads can be inside it at once. A seek here would make
/// two concurrent readers move each other's cursor and read each other's bytes.
///
/// Safe against a concurrent append because the active segment is opened
/// `O_APPEND`: writes go to the end regardless of the cursor, and pread never
/// touches it.
fn read_value_at(segments: &SegmentMap, loc: ValueLoc) -> io::Result<Vec<u8>> {
    let file = segments.get(&loc.segment_id).expect("indexed segment must be open");
    let mut buf = vec![0u8; loc.len as usize];
    file.read_exact_at(&mut buf, loc.offset)?;

    let (record, _) =
        Record::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(record.value)
}

pub struct Engine {
    dir: PathBuf,
    config: EngineConfig,
    active_id: SegmentId,
    active_offset: u64,
    /// Shared immutably so a resolved read can be handed to another thread
    /// (see `locate`). Only this engine mutates it, and it does so
    /// copy-on-write via `Arc::make_mut` — which clones the map only while
    /// a reader still holds the previous snapshot. `ArcSwap` would buy
    /// nothing here: there is exactly one writer of the pointer.
    segments: Arc<SegmentMap>,
    index: Index,
    unsynced: usize,
    last_sync: std::time::Instant,
    /// Shared rather than a plain counter so a caller can keep watching after
    /// the engine has been handed off — `kv-node` moves its state machines
    /// into a `Group` and still has to be able to say whether one was fsynced.
    /// One relaxed increment next to an fsync costs nothing measurable.
    syncs: Arc<AtomicU64>,
}

impl Engine {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_config(dir, EngineConfig::default())
    }

    /// Retained so M1.4-M1.6 tests keep working unchanged; equivalent to
    /// `open_with_config` with the default fsync policy.
    pub fn open_with_max_segment_size(
        dir: impl AsRef<Path>,
        max_segment_size: u64,
    ) -> io::Result<Self> {
        Self::open_with_config(dir, EngineConfig { max_segment_size, ..Default::default() })
    }

    pub fn open_with_config(dir: impl AsRef<Path>, config: EngineConfig) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        match read_manifest(&dir)? {
            Some(manifest) => finish_compaction(&dir, &manifest)?,
            None => clear_orphan_tmp_files(&dir)?,
        }

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
            segments.insert(id, Arc::new(open_segment(&dir, id)?));
        }

        let active_id = *ids.last().expect("ids always has at least one entry");

        let mut index = Index::new(config.index);
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
            segments.get(&id).expect("segment just replayed must be open").set_len(valid_len)?;
        }

        let active_offset = segments[&active_id].metadata()?.len();

        // Everything replay just inserted is a write like any other, so it is
        // pending until published. A freshly opened engine whose readers could
        // not see its own contents would look empty to every `Get` until the
        // first entry was applied.
        let segments = Arc::new(segments);
        index.set_segments(Arc::clone(&segments));
        index.publish();

        Ok(Self {
            dir,
            config,
            active_id,
            active_offset,
            segments,
            index,
            unsynced: 0,
            last_sync: std::time::Instant::now(),
            syncs: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Writes `encoded` to the active segment, rotating to a fresh one first
    /// if it wouldn't fit under `max_segment_size`. Never rotates an empty
    /// active segment, so a single record larger than the threshold is still
    /// written rather than looping forever.
    fn append(&mut self, encoded: &[u8]) -> io::Result<ValueLoc> {
        let len = encoded.len() as u32;

        if self.active_offset > 0 && self.active_offset + len as u64 > self.config.max_segment_size
        {
            self.rotate()?;
        }

        let mut file: &File =
            self.segments.get(&self.active_id).expect("active segment always present");
        file.write_all(encoded)?;

        let loc = ValueLoc { segment_id: self.active_id, offset: self.active_offset, len };
        self.active_offset += len as u64;
        self.unsynced += 1;
        Ok(loc)
    }

    /// Flushes the active segment to stable storage regardless of policy, and
    /// resets the group-commit window. A no-op when nothing is pending, so
    /// calling it defensively costs nothing.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.unsynced == 0 {
            return Ok(());
        }
        let file = self.segments.get(&self.active_id).expect("active segment always present");
        file.sync_data()?;
        self.unsynced = 0;
        self.last_sync = std::time::Instant::now();
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// How many times this engine has actually flushed to stable storage.
    ///
    /// Public, and not `cfg(test)`, because it is the only honest way to say
    /// what a durability policy costs: under `GroupCommit` the number of
    /// writes tells you nothing about the number of fsyncs, and the whole
    /// point of the policy is the gap between them. `kv-node` asserts on it
    /// across the crate boundary, and a metrics endpoint would want it too.
    pub fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// A handle on [`Self::sync_count`] that outlives a move of the engine.
    pub fn sync_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.syncs)
    }

    /// Flushes if the configured policy says this write should be the one
    /// that triggers it. Called after every append.
    fn maybe_sync(&mut self) -> io::Result<()> {
        if self.config.fsync_policy.should_sync(self.unsynced, self.last_sync.elapsed()) {
            self.sync()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.sync()?;
        self.active_id += 1;
        let opened = open_segment(&self.dir, self.active_id)?;
        let id = self.active_id;
        self.update_segments(|map| {
            map.insert(id, Arc::new(opened));
        });
        self.active_offset = 0;
        Ok(())
    }

    /// Changes which segments are open, copy-on-write, and tells readers.
    ///
    /// The previous `Arc` stays valid for whoever holds it, which is what
    /// makes a `ValueRef` handed out before the change still readable — and
    /// what keeps a retired segment's descriptor alive for a reader still on
    /// the keydir copy that names it.
    fn update_segments(&mut self, edit: impl FnOnce(&mut SegmentMap)) {
        edit(Arc::make_mut(&mut self.segments));
        self.index.set_segments(Arc::clone(&self.segments));
    }

    /// Makes every write since the last publish visible to readers, and says
    /// whether it managed to.
    ///
    /// The caller stores its own "visible up to" index only when this returns
    /// `true`. `false` is not an error: it means a reader is still inside the
    /// copy the swap would overwrite, and the engine will not block a writer
    /// on a reader. Retry on the next pass.
    ///
    /// A no-op that always succeeds for [`crate::IndexKind::Locked`], where a
    /// write is visible the moment it returns.
    pub fn publish(&mut self) -> bool {
        self.index.publish()
    }

    /// Whether any write is applied but not yet visible to readers.
    pub fn has_unpublished(&self) -> bool {
        self.index.has_unpublished()
    }

    /// Mints reader-side views of this engine. `Send + Sync`, so it belongs in
    /// a service struct; each reading task turns it into a [`ReadView`] of its
    /// own, because the underlying `ReadHandle` is `!Sync`.
    pub fn read_view_factory(&self) -> ReadViewFactory {
        ReadViewFactory { keydir: self.index.read_factory() }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let record = Record::create(now_millis(), key.to_vec(), value.to_vec());
        let loc = self.append(&record.encode())?;
        self.index.insert(key.to_vec(), loc);
        self.maybe_sync()?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let Some(loc) = self.index.get(key) else {
            return Ok(None);
        };
        Ok(Some(read_value_at(&self.segments, loc)?))
    }

    /// Resolves `key` against the keydir without touching the disk, handing
    /// back everything the actual read needs. The caller can then perform it
    /// anywhere — notably off the task that owns this engine — while this
    /// engine goes on taking writes.
    pub fn locate(&self, key: &[u8]) -> Option<ValueRef> {
        let loc = self.index.get(key)?;
        Some(ValueRef { loc, segments: Arc::clone(&self.segments) })
    }

    /// Every live `(key, value)` pair — the state image M8 snapshots are a
    /// scan of. Tombstones are already out of the keydir, so deleted keys do
    /// not appear; reserved (`\x00`) keys do, which is how the session table
    /// rides along inside the snapshot instead of beside it.
    pub fn scan(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for (key, loc) in self.index.iter() {
            out.push((key, read_value_at(&self.segments, loc)?));
        }
        Ok(out)
    }

    /// Writes a tombstone record so the deletion survives reopen, then
    /// removes `key` from the in-memory index.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let record = Record::tombstone(now_millis(), key.to_vec());
        self.append(&record.encode())?;
        self.index.remove(key);
        self.maybe_sync()?;
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
    pub(crate) fn plan_compaction(&mut self) -> io::Result<Option<CompactionPlan>> {
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
            let value = read_value_at(&self.segments, loc)?;
            entries.push((key, loc, value));
        }

        let new_segment_id = *old_segment_ids.iter().min().expect("checked non-empty above");
        Ok(Some(CompactionPlan { old_segment_ids, new_segment_id, entries }))
    }

    /// Writes the plan's live entries into a temporary segment (named for the
    /// smallest retired id, so the merged data still sorts before the active
    /// segment on the next replay) plus its hint file, relocates the index —
    /// skipping any key overwritten since the plan was taken — then commits
    /// with a manifest and finishes the renames and unlinks.
    ///
    /// Nothing before `write_manifest` is visible to a reopen; everything
    /// after it is replayable by `finish_compaction`. That is what keeps a
    /// crash mid-compaction from either losing the merge or letting a stale
    /// retired segment replay over it.
    pub(crate) fn apply_compaction(&mut self, plan: CompactionPlan) -> io::Result<()> {
        let CompactionPlan { old_segment_ids, new_segment_id, entries } = plan;

        let manifest = if entries.is_empty() {
            CompactionManifest { new_id: None, old_ids: old_segment_ids.clone() }
        } else {
            let mut hint_entries = Vec::with_capacity(entries.len());
            let mut relocations = Vec::with_capacity(entries.len());

            let seg_tmp = self.dir.join(tmp_name(&segment_file_name(new_segment_id)));
            {
                let mut file = File::create(&seg_tmp)?;
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
                file.sync_all()?;
            }

            write_hint_tmp(&self.dir, new_segment_id, &hint_entries)?;

            for (key, old_loc, new_loc) in relocations {
                self.index.relocate(&key, old_loc, new_loc);
            }

            CompactionManifest { new_id: Some(new_segment_id), old_ids: old_segment_ids.clone() }
        };

        write_manifest(&self.dir, &manifest)?;
        finish_compaction(&self.dir, &manifest)?;

        // A reader on the keydir copy this is replacing still points into the
        // segments being retired — and the merged segment deliberately reuses
        // the smallest retired id, so their descriptors cannot simply be
        // replaced in place. The old map stays alive for as long as that copy
        // does, because it is *inside* that copy; this builds a new one.
        let reopened = match manifest.new_id {
            Some(new_id) => Some((new_id, Arc::new(open_segment(&self.dir, new_id)?))),
            None => None,
        };
        self.update_segments(|map| {
            for id in &old_segment_ids {
                map.remove(id);
            }
            if let Some((new_id, file)) = reopened {
                map.insert(new_id, file);
            }
        });

        Ok(())
    }
}

pub(crate) struct CompactionPlan {
    pub(crate) old_segment_ids: Vec<SegmentId>,
    pub(crate) new_segment_id: SegmentId,
    entries: Vec<(Vec<u8>, ValueLoc, Vec<u8>)>,
}

/// Mints [`ReadView`]s. Cheap to clone and safe to share.
#[derive(Debug, Clone)]
pub struct ReadViewFactory {
    keydir: KeyDirReadFactory,
}

impl ReadViewFactory {
    pub fn view(&self) -> ReadView {
        ReadView { keydir: self.keydir.handle() }
    }

    /// Parks a reader inside the published copy and keeps it there until the
    /// returned value is dropped.
    ///
    /// Test support, and it tests something real: while a reader is inside the
    /// copy a swap would overwrite, [`Engine::publish`] cannot proceed. That
    /// is left-right's documented cost — "a slow reader stalls the writer" —
    /// and holding one is the only way to make the window between *applied*
    /// and *visible* deterministic instead of a race that passes by luck.
    ///
    /// Only defers a publish if it is taken before the publish that records
    /// its epoch: left-right waits for readers that entered before the last
    /// swap, not for ones that arrived after it.
    pub fn hold_read_copy(&self) -> ReadHold {
        let keydir = self.keydir.clone();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (parked, entered) = std::sync::mpsc::channel::<()>();
        // A thread rather than a guard held by the caller: a `ReadGuard`
        // borrows its handle, so a value owning both would be
        // self-referential. This thread owns both and parks.
        let thread = std::thread::Builder::new()
            .name("kv-read-hold".into())
            .spawn(move || {
                let handle = keydir.handle();
                let _guard = match &handle {
                    KeyDirRead::LeftRight(handle) => handle.enter(),
                    // Nothing to park behind: a locked write is visible
                    // immediately, so there is no window to hold open.
                    KeyDirRead::Locked(_) => None,
                };
                let _ = parked.send(());
                // Until the `ReadHold` is dropped, which closes this channel.
                let _ = wait.recv();
            })
            .expect("spawning a test hold thread");
        // Inside before this returns, so the caller can rely on the next
        // publish seeing it.
        let _ = entered.recv();
        ReadHold { release: Some(release), thread: Some(thread) }
    }
}

/// One task's reader-side view of the engine: the published keydir plus the
/// open segment handles.
///
/// This is the half of `Engine` that needs no `&mut` and no lock. What it can
/// see is whatever the last [`Engine::publish`] made visible, which is why a
/// linearizable read has to wait on the published index rather than the
/// applied one.
#[derive(Debug)]
pub struct ReadView {
    keydir: KeyDirRead,
}

impl ReadView {
    /// Resolves `key` against the published keydir, handing back everything
    /// the disk read needs. The same contract as [`Engine::locate`], from a
    /// thread that does not own the engine.
    pub fn locate(&self, key: &[u8]) -> Option<ValueRef> {
        let (loc, segments) = self.keydir.locate(key)?;
        Some(ValueRef { loc, segments })
    }

    /// Performs the whole read here, disk included. The blocking form, for
    /// callers with nowhere better to put the transfer.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self.locate(key) {
            Some(located) => located.read().map(Some),
            None => Ok(None),
        }
    }
}

/// A reader parked inside the published copy. See
/// [`ReadViewFactory::hold_read_copy`].
#[derive(Debug)]
pub struct ReadHold {
    release: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ReadHold {
    fn drop(&mut self) {
        drop(self.release.take());
        // Joined, so that when this returns the reader has really left and
        // the next publish cannot be deferred by it.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
