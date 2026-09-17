//! The keydir abstraction (plan §1.11/M1.3, M11.5).
//!
//! `KeyDirIndex` is the seam M1.3 established so that M11.5 could swap the
//! implementation without touching a call site in `engine.rs`. It held: the
//! six index call sites in `engine.rs` (`put`, `get`, `locate`, `scan`,
//! `delete`, and `compact`'s relocate loop) are unchanged by this milestone.
//!
//! Two implementations live behind it, chosen by [`crate::IndexKind`]:
//!
//! - [`LockedIndex`] — `Arc<RwLock<HashMap>>`. The comparison arm the M11.5
//!   benchmark needs, and the fallback if left-right ever misbehaves in
//!   production. Every reader takes the same lock, so reads serialize against
//!   each other under a writer.
//! - [`left_right::LeftRightIndex`] — wait-free reads. Readers never take a
//!   lock; they increment an epoch counter and read whichever copy is
//!   published.
//!
//! **The difference that leaks.** A `LockedIndex` write is visible the instant
//! it returns. A left-right write is visible only after `publish()`. Anything
//! that waits for a write to be *readable* must therefore wait on the
//! published index rather than the applied one — see `Group::drain` in
//! kv-node, and §1.15's "correctness trap".

pub(crate) mod left_right;

use std::sync::{Arc, RwLock};

use crate::config::IndexKind;
use crate::engine::SegmentMap;
use crate::index::left_right::KeyDir;

pub(crate) type SegmentId = u32;

/// Where one record lives: which segment, at what offset, for how many bytes.
///
/// `pub` because it travels inside [`left_right::KvOp`], which the keydir's
/// `Absorb` implementation is written over and which a benchmark outside this
/// crate has to be able to construct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueLoc {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub len: u32,
}

pub(crate) trait KeyDirIndex {
    fn get(&self, key: &[u8]) -> Option<ValueLoc>;
    fn insert(&mut self, key: Vec<u8>, loc: ValueLoc);
    fn remove(&mut self, key: &[u8]) -> Option<ValueLoc>;

    /// Applies a compaction-driven move, but only if the entry still points
    /// at `old` — if the key was overwritten since compaction read it, the
    /// newer entry must win. Returns whether the relocation was applied.
    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool;

    /// Enumerates all live entries; compaction uses this to find what's
    /// still live across the segments it is about to retire.
    fn iter(&self) -> Vec<(Vec<u8>, ValueLoc)>;
}

/// The keydir the engine actually holds: whichever implementation the config
/// asked for, dispatched by an enum rather than a `Box<dyn>` so the common
/// case is a branch the predictor gets right every time rather than an
/// indirect call.
#[derive(Debug)]
pub(crate) enum Index {
    Locked(LockedIndex),
    LeftRight(left_right::LeftRightIndex),
}

impl Index {
    pub(crate) fn new(kind: IndexKind) -> Self {
        match kind {
            IndexKind::Locked => Self::Locked(LockedIndex::default()),
            IndexKind::LeftRight => Self::LeftRight(left_right::LeftRightIndex::default()),
        }
    }

    /// Makes every write since the last publish visible to readers, and says
    /// whether it managed to.
    ///
    /// `false` means a reader is still parked inside the copy that is about to
    /// be overwritten, so the swap would have to block to be safe. The engine
    /// does not block: it leaves the writes pending and the caller retries.
    /// That is why visibility is a separate counter from application.
    pub(crate) fn publish(&mut self) -> bool {
        match self {
            // Nothing to publish: a locked write is visible the moment it
            // returns, which is exactly the property left-right trades away.
            Self::Locked(_) => true,
            Self::LeftRight(index) => index.publish(),
        }
    }

    /// Whether any write is applied but not yet visible to readers.
    pub(crate) fn has_unpublished(&self) -> bool {
        match self {
            Self::Locked(_) => false,
            Self::LeftRight(index) => index.has_unpublished(),
        }
    }

    pub(crate) fn read_factory(&self) -> KeyDirReadFactory {
        match self {
            Self::Locked(index) => KeyDirReadFactory::Locked(Arc::clone(&index.keydir)),
            Self::LeftRight(index) => index.read_factory(),
        }
    }

    /// Tells readers which segment files are open.
    ///
    /// Not part of [`KeyDirIndex`]: that trait is M1.3's five-method seam and
    /// this is not a keydir operation. It is here because the keydir seam
    /// turned out not to cover the reader's whole view — a location is only
    /// meaningful together with the segment map it was recorded in, and
    /// compaction reuses segment ids. The two have to become visible in the
    /// same step.
    pub(crate) fn set_segments(&mut self, segments: Arc<SegmentMap>) {
        match self {
            Self::Locked(index) => {
                index.keydir.write().expect("keydir lock is never poisoned").set_segments(segments)
            }
            Self::LeftRight(index) => index.set_segments(segments),
        }
    }
}

impl KeyDirIndex for Index {
    fn get(&self, key: &[u8]) -> Option<ValueLoc> {
        match self {
            Self::Locked(index) => index.get(key),
            Self::LeftRight(index) => index.get(key),
        }
    }

    fn insert(&mut self, key: Vec<u8>, loc: ValueLoc) {
        match self {
            Self::Locked(index) => index.insert(key, loc),
            Self::LeftRight(index) => index.insert(key, loc),
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<ValueLoc> {
        match self {
            Self::Locked(index) => index.remove(key),
            Self::LeftRight(index) => index.remove(key),
        }
    }

    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool {
        match self {
            Self::Locked(index) => index.relocate(key, old, new),
            Self::LeftRight(index) => index.relocate(key, old, new),
        }
    }

    fn iter(&self) -> Vec<(Vec<u8>, ValueLoc)> {
        match self {
            Self::Locked(index) => index.iter(),
            Self::LeftRight(index) => index.iter(),
        }
    }
}

/// Mints reader-side handles on the keydir. `Send + Sync`, so it can live in a
/// service struct; the handle it produces is per-task.
#[derive(Debug, Clone)]
pub(crate) enum KeyDirReadFactory {
    Locked(Arc<RwLock<KeyDir>>),
    LeftRight(::left_right::ReadHandleFactory<KeyDir>),
}

impl KeyDirReadFactory {
    pub(crate) fn handle(&self) -> KeyDirRead {
        match self {
            Self::Locked(keydir) => KeyDirRead::Locked(Arc::clone(keydir)),
            Self::LeftRight(factory) => KeyDirRead::LeftRight(factory.handle()),
        }
    }
}

/// One task's read handle on the keydir.
///
/// `!Sync` in the left-right case (`ReadHandle` is), which is the constraint
/// that decides the shape of the read path: the factory is shared, the handle
/// is not.
#[derive(Debug)]
pub(crate) enum KeyDirRead {
    Locked(Arc<RwLock<KeyDir>>),
    LeftRight(::left_right::ReadHandle<KeyDir>),
}

impl KeyDirRead {
    /// Resolves a key against what is published, together with the segment map
    /// that location belongs to.
    pub(crate) fn locate(&self, key: &[u8]) -> Option<(ValueLoc, Arc<SegmentMap>)> {
        match self {
            Self::Locked(keydir) => {
                keydir.read().expect("keydir lock is never poisoned").locate(key)
            }
            // `None` only once the writer has been dropped, which means the
            // engine is gone; a read racing shutdown answers "not found"
            // rather than panicking on the way out.
            Self::LeftRight(handle) => handle.enter().and_then(|keydir| keydir.locate(key)),
        }
    }

    /// Everything this reader can currently see. Test and benchmark support:
    /// the read path itself never needs the whole map.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> Vec<(Vec<u8>, ValueLoc)> {
        match self {
            Self::Locked(keydir) => keydir.read().expect("keydir lock is never poisoned").entries(),
            Self::LeftRight(handle) => {
                handle.enter().map(|keydir| keydir.entries()).unwrap_or_default()
            }
        }
    }
}

/// `Arc<RwLock<HashMap>>`: the comparison arm, and what every version of this
/// engine before M11.5 used (minus the lock, which it did not need while the
/// only reader was the writer).
#[derive(Debug, Default)]
pub(crate) struct LockedIndex {
    keydir: Arc<RwLock<KeyDir>>,
}

impl LockedIndex {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, KeyDir> {
        self.keydir.read().expect("keydir lock is never poisoned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, KeyDir> {
        self.keydir.write().expect("keydir lock is never poisoned")
    }
}

impl KeyDirIndex for LockedIndex {
    fn get(&self, key: &[u8]) -> Option<ValueLoc> {
        self.read().get(key)
    }

    fn insert(&mut self, key: Vec<u8>, loc: ValueLoc) {
        self.write().insert(key, loc);
    }

    fn remove(&mut self, key: &[u8]) -> Option<ValueLoc> {
        self.write().remove(key)
    }

    fn relocate(&mut self, key: &[u8], old: ValueLoc, new: ValueLoc) -> bool {
        self.write().relocate(key, old, new)
    }

    fn iter(&self) -> Vec<(Vec<u8>, ValueLoc)> {
        self.read().entries()
    }
}
