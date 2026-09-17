//! The wait-free keydir (M11.5, plan §1.15).
//!
//! `left-right` keeps two copies of the map. Readers read whichever one is
//! published, with no lock and no writer coordination — only an epoch
//! increment. The writer describes each change as a [`KvOp`], appends it to an
//! operational log, and `publish()` swaps the copies and replays the log onto
//! the one the readers just left.
//!
//! **What §1.15 does not say, and it matters.** The design argues that Raft
//! supplies left-right's three requirements for free: one writer, a
//! deterministic oplog, ops replayable twice. All true. But it misses that the
//! Raft apply loop is a *read-modify-write* loop — `Cas` reads the current
//! value, the session table reads the last response for a `(client, seq)`, the
//! membership handler reads the address book — and that left-right's writer
//! **cannot read its own unpublished writes at all**. `WriteHandle` derefs to
//! a `ReadHandle`, which is the published copy; the write copy does not even
//! see an appended op until the next `publish`, because `append` only pushes
//! onto the oplog.
//!
//! Publishing per op would fix that and throw away the batching §1.15 asks
//! for. Mutating the write copy directly through `raw_write_handle` is
//! unsound — readers may still be inside it after a swap, which is precisely
//! what `publish`'s wait exists to prevent.
//!
//! So this keeps a third, small thing: an **overlay** of the keys touched
//! since the last publish. The writer's own reads consult it first and fall
//! through to the published copy; it is cleared on publish. That costs the
//! delta, not a third full copy, so §1.15's "memory doubles" survives with an
//! asterisk rather than becoming "memory triples".

use std::collections::HashMap;
use std::sync::Arc;

use left_right::{Absorb, WriteHandle};

use crate::engine::SegmentMap;
use crate::index::{KeyDirIndex, KeyDirReadFactory, ValueLoc};

/// What a reader resolves a key against: the keydir, **and** the open segment
/// handles that its locations name.
///
/// The two travel together, and that is not an optimisation. Compaction gives
/// the merged segment the smallest *retired* id, so it deliberately reuses a
/// number the outgoing keydir copy still points at. A reader on the old copy
/// that looked up the *current* segment map would resolve an old offset
/// against the merged file and read whatever happens to live there. Keeping
/// the map inside the published structure makes every reader's (keydir,
/// segments) pair consistent by construction — the segment map changes through
/// the same oplog as everything else.
#[derive(Debug, Default)]
pub(crate) struct KeyDir {
    entries: HashMap<Vec<u8>, ValueLoc>,
    segments: Arc<SegmentMap>,
}

impl KeyDir {
    /// Resolves a key to its location and the segment map that location is
    /// valid in.
    pub(crate) fn locate(&self, key: &[u8]) -> Option<(ValueLoc, Arc<SegmentMap>)> {
        let loc = self.entries.get(key).copied()?;
        Some((loc, Arc::clone(&self.segments)))
    }

    pub(crate) fn entries(&self) -> Vec<(Vec<u8>, ValueLoc)> {
        self.entries.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<ValueLoc> {
        self.entries.get(key).copied()
    }

    /// The three mutations, applied directly. Used by the `RwLock<HashMap>`
    /// arm, which needs no oplog because its writes are visible as soon as it
    /// releases the lock.
    pub(crate) fn insert(&mut self, key: Vec<u8>, loc: ValueLoc) {
        self.entries.insert(key, loc);
    }

    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<ValueLoc> {
        self.entries.remove(key)
    }

    pub(crate) fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool {
        if self.entries.get(key) != Some(&old) {
            return false;
        }
        self.entries.insert(key.to_vec(), new);
        true
    }

    pub(crate) fn set_segments(&mut self, segments: Arc<SegmentMap>) {
        self.segments = segments;
    }

    /// Direct access to the map, for the tests that build a copy by hand to
    /// check `sync_with`.
    #[cfg(test)]
    pub(crate) fn entries_mut(&mut self) -> &mut HashMap<Vec<u8>, ValueLoc> {
        &mut self.entries
    }

    /// The op applied to one copy, borrowing the key.
    fn apply_ref(&mut self, op: &KvOp) {
        match op {
            KvOp::Insert { key, loc } => {
                self.entries.insert(key.clone(), *loc);
            }
            KvOp::Remove { key } => {
                self.entries.remove(key);
            }
            KvOp::Relocate { key, old, new } => self.apply_relocate(key, *old, *new),
            KvOp::Segments(segments) => self.segments = Arc::clone(segments),
        }
    }

    /// The same op, consuming it — so the second copy does not pay for a key
    /// clone it does not need.
    fn apply_owned(&mut self, op: KvOp) {
        match op {
            KvOp::Insert { key, loc } => {
                self.entries.insert(key, loc);
            }
            KvOp::Remove { key } => {
                self.entries.remove(&key);
            }
            KvOp::Relocate { key, old, new } => self.apply_relocate(&key, old, new),
            KvOp::Segments(segments) => self.segments = segments,
        }
    }

    /// Compaction's conditional move. Both copies evaluate the condition
    /// against their own contents, and reach the same answer because both have
    /// absorbed exactly the same ops in exactly the same order.
    fn apply_relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) {
        if self.entries.get(key) == Some(&old) {
            self.entries.insert(key.to_vec(), new);
        }
    }
}

/// One keydir mutation, as a value.
///
/// `Relocate` is the reason §1.15 insisted the keydir sit behind a trait from
/// M1: compaction cannot rewrite offsets behind the writer's back, so it
/// describes its move as an op that flows through the same single writer and
/// is applied conditionally.
#[derive(Debug, Clone)]
pub(crate) enum KvOp {
    Insert {
        key: Vec<u8>,
        loc: ValueLoc,
    },
    Remove {
        key: Vec<u8>,
    },
    Relocate {
        key: Vec<u8>,
        old: ValueLoc,
        new: ValueLoc,
    },
    /// A rotation or a compaction changed which segments are open. Carried
    /// through the oplog like any other change so that no reader ever sees a
    /// location from one generation of the keydir against the file map of
    /// another.
    Segments(Arc<SegmentMap>),
}

impl Absorb<KvOp> for KeyDir {
    fn absorb_first(&mut self, op: &mut KvOp, _other: &Self) {
        self.apply_ref(op);
    }

    fn absorb_second(&mut self, op: KvOp, _other: &Self) {
        self.apply_owned(op);
    }

    /// Called once, on the second publish, to bring the copy that missed
    /// everything written before the *first* publish up to date. A wholesale
    /// replacement: anything less loses the state a reopened engine starts in.
    fn sync_with(&mut self, first: &Self) {
        self.entries.clone_from(&first.entries);
        self.segments = Arc::clone(&first.segments);
    }
}

/// What the writer sees for a key it has touched since the last publish.
/// `None` is a removal — distinct from "not in the overlay", which falls
/// through to the published copy.
type Pending = HashMap<Vec<u8>, Option<ValueLoc>>;

pub(crate) struct LeftRightIndex {
    writer: WriteHandle<KeyDir, KvOp>,
    /// Keys written since the last publish. See the module comment: the apply
    /// loop reads its own writes, and left-right's writer cannot.
    pending: Pending,
    /// Whether a `KvOp::Segments` is waiting too. Not a key, so the overlay
    /// cannot represent it, and it still has to make `has_unpublished` true:
    /// a rotation with no write after it must still reach readers.
    segments_pending: bool,
}

impl std::fmt::Debug for LeftRightIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeftRightIndex").field("pending", &self.pending.len()).finish()
    }
}

impl Default for LeftRightIndex {
    fn default() -> Self {
        let (mut writer, _reader) = left_right::new::<KeyDir, KvOp>();
        // Leave the "first" regime immediately. Before its first publish a
        // `WriteHandle` applies appends straight to the write copy and keeps
        // no oplog, so `has_pending_operations` lies and the two copies are
        // out of step in a way nothing else here has to reason about. One
        // swap of two empty maps costs nothing and removes that special case.
        writer.publish();
        Self { writer, pending: HashMap::new(), segments_pending: false }
    }
}

impl LeftRightIndex {
    /// Swaps the copies so every write since the last publish becomes
    /// visible, and says whether it happened.
    ///
    /// `try_publish`, not `publish`: `publish` waits for readers to leave the
    /// copy it is about to overwrite, and the only caller is the driver's
    /// `select!` loop, which must not block on a reader. A deferred publish
    /// leaves the writes applied but invisible, which is exactly the state
    /// `published_index` exists to describe; the driver retries next drain.
    pub(crate) fn publish(&mut self) -> bool {
        if !self.writer.try_publish() {
            return false;
        }
        self.pending.clear();
        self.segments_pending = false;
        true
    }

    pub(crate) fn has_unpublished(&self) -> bool {
        !self.pending.is_empty() || self.segments_pending
    }

    pub(crate) fn read_factory(&self) -> KeyDirReadFactory {
        KeyDirReadFactory::LeftRight(self.writer.factory())
    }

    /// Publishes a new set of open segment handles, paired with whatever
    /// keydir state is published alongside it.
    pub(crate) fn set_segments(&mut self, segments: Arc<SegmentMap>) {
        self.writer.append(KvOp::Segments(segments));
        // Not a key, so it cannot go in the overlay — but it does mean there
        // is something to publish.
        self.segments_pending = true;
    }

    /// What the published copy holds for `key`, ignoring the overlay.
    fn published(&self, key: &[u8]) -> Option<ValueLoc> {
        self.writer.enter().and_then(|keydir| keydir.locate(key)).map(|(loc, _)| loc)
    }
}

impl KeyDirIndex for LeftRightIndex {
    fn get(&self, key: &[u8]) -> Option<ValueLoc> {
        match self.pending.get(key) {
            Some(pending) => *pending,
            None => self.published(key),
        }
    }

    fn insert(&mut self, key: Vec<u8>, loc: ValueLoc) {
        self.writer.append(KvOp::Insert { key: key.clone(), loc });
        self.pending.insert(key, Some(loc));
    }

    fn remove(&mut self, key: &[u8]) -> Option<ValueLoc> {
        let previous = self.get(key);
        self.writer.append(KvOp::Remove { key: key.to_vec() });
        self.pending.insert(key.to_vec(), None);
        previous
    }

    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool {
        // Decided here against the writer's own view, and again inside
        // `absorb_*` against each copy's. The three agree because all three
        // have seen the same ops in the same order — which is the invariant
        // that makes a conditional op safe to replay twice.
        if self.get(key) != Some(old) {
            return false;
        }
        self.writer.append(KvOp::Relocate { key: key.to_vec(), old, new });
        self.pending.insert(key.to_vec(), Some(new));
        true
    }

    fn iter(&self) -> Vec<(Vec<u8>, ValueLoc)> {
        let mut entries: HashMap<Vec<u8>, ValueLoc> = self
            .writer
            .enter()
            .map(|keydir| keydir.entries())
            .unwrap_or_default()
            .into_iter()
            .collect();
        for (key, pending) in &self.pending {
            match pending {
                Some(loc) => entries.insert(key.clone(), *loc),
                None => entries.remove(key),
            };
        }
        entries.into_iter().collect()
    }
}
