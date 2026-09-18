//! Bitcask-backed `RaftStorage` (M2).
//!
//! This lives in `kv-node`, not `kv-storage`, on purpose: `kv-storage` must
//! not know Raft exists, and putting the impl there would invert the layering.
//! `kv-node` already depends on both, so it is the natural seam.
//!
//! Bitcask has no range scan, so the key space carries the structure:
//! `\x00hard_state` and `\x00log_meta` hold the two singletons, and each entry
//! lives at `e{index:020}`. `last_index` is persisted rather than derived —
//! and that is also the more crash-correct choice, since a crash between the
//! entry appends and the meta update simply hides entries that were never
//! acknowledged.

use std::cell::RefCell;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use kv_raft::storage::RaftStorage;
use kv_raft::types::{Entry, HardState, LogIndex, Snapshot, Term};
use kv_storage::{Engine, EngineConfig, FsyncPolicy};
use serde::{Deserialize, Serialize};

const HARD_STATE_KEY: &[u8] = b"\x00hard_state";
const LOG_META_KEY: &[u8] = b"\x00log_meta";
const SNAPSHOT_KEY: &[u8] = b"\x00snapshot";

fn entry_key(index: LogIndex) -> Vec<u8> {
    format!("e{index:020}").into_bytes()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct LogMeta {
    last_index: LogIndex,
    /// One past the highest truncated prefix index. `#[serde(default)]` keeps
    /// directories written before M8 readable — they simply have no prefix.
    #[serde(default = "default_base")]
    base_index: LogIndex,
}

fn default_base() -> LogIndex {
    1
}

#[derive(Debug, thiserror::Error)]
pub enum BitcaskStorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encoding: {0}")]
    Encoding(#[from] bincode::Error),
    #[error("append would leave a gap: expected first index {expected}, got {got}")]
    Gap { expected: LogIndex, got: LogIndex },
    #[error("entries are not contiguous at index {at}")]
    NotContiguous { at: LogIndex },
}

pub struct BitcaskStorage {
    // Single-threaded by construction: one BitcaskStorage per Raft group,
    // owned by its RaftNode. `Engine::get` takes `&mut self` while the trait's
    // read methods take `&self`, hence interior mutability here rather than
    // `&mut` threaded through every M3 call site.
    engine: RefCell<Engine>,
    /// Only read by the conformance harness, which reopens the same directory
    /// to prove state survives a restart.
    #[cfg(test)]
    path: PathBuf,
    last_index: LogIndex,
    base: LogIndex,
    snapshot: Option<Snapshot>,
    /// Publishes [`Engine::sync_count`] on every explicit [`Self::sync`], so a
    /// test holding nothing but this `Arc` can tell whether the log had been
    /// fsynced at the instant some *other* component did something — which is
    /// how `tests::group_commit` pins disk-before-network without reaching
    /// inside the driver.
    #[cfg(test)]
    syncs: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl BitcaskStorage {
    /// Opens a log under the engine's default durability, one fsync per write.
    ///
    /// `cfg(test)` since the node started choosing its log's policy: every
    /// real call site goes through [`Self::open_with_policy`] with what
    /// `--log-fsync` asked for, and a shorthand that silently means
    /// `EveryWrite` is how a group-commit build ends up with a
    /// fsync-per-entry log on one path.
    #[cfg(test)]
    pub(crate) fn open(dir: impl AsRef<Path>) -> Result<Self, BitcaskStorageError> {
        Self::open_with_policy(dir, EngineConfig::default().fsync_policy)
    }

    /// Opens a log under an explicit fsync policy.
    ///
    /// `FsyncPolicy::GroupCommit` is what makes N appends cost one fsync
    /// instead of N+1, and it is safe **only** because `Group::drain` calls
    /// [`Self::sync`] before it puts anything on the wire. See the module
    /// header on `sync` and `tests::group_commit`.
    pub fn open_with_policy(
        dir: impl AsRef<Path>,
        fsync_policy: FsyncPolicy,
    ) -> Result<Self, BitcaskStorageError> {
        let path = dir.as_ref().to_path_buf();
        let engine =
            Engine::open_with_config(&path, EngineConfig { fsync_policy, ..Default::default() })?;
        let (last_index, base) = match engine.get(LOG_META_KEY)? {
            Some(bytes) => {
                let meta: LogMeta = bincode::deserialize(&bytes)?;
                (meta.last_index, meta.base_index.max(1))
            }
            None => (0, 1),
        };
        let snapshot: Option<Snapshot> = match engine.get(SNAPSHOT_KEY)? {
            Some(bytes) => Some(bincode::deserialize(&bytes)?),
            None => None,
        };
        Ok(Self {
            engine: RefCell::new(engine),
            #[cfg(test)]
            path,
            last_index: last_index
                .max(snapshot.as_ref().map(|s| s.last_included_index).unwrap_or(0)),
            base,
            snapshot,
            #[cfg(test)]
            syncs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// How many times the log has actually been flushed to stable storage.
    #[cfg(test)]
    pub(crate) fn sync_count(&self) -> u64 {
        self.engine.borrow().sync_count()
    }

    /// A handle on that count, updated by [`Self::sync`]. See the field.
    #[cfg(test)]
    pub(crate) fn sync_ticker(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        std::sync::Arc::clone(&self.syncs)
    }

    #[cfg(test)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flushes the Raft log to stable storage. The driver calls this after
    /// draining a `Ready` and **before** sending anything (§1.5: disk before
    /// network) — a vote must be durable before it is granted on the wire, or
    /// a crash lets the node vote twice in one term and election safety is
    /// gone.
    ///
    /// Under `FsyncPolicy::EveryWrite` this is redundant — each write has
    /// already synced on the way in. Under `FsyncPolicy::GroupCommit`, which
    /// is now the node's default for the log, **it is the only thing that
    /// makes anything durable**, and the ordering it sits in is the entire
    /// safety argument rather than a nicety. A no-op when nothing is pending,
    /// which is what lets the driver call it unconditionally.
    pub fn sync(&self) -> Result<(), BitcaskStorageError> {
        let mut engine = self.engine.borrow_mut();
        engine.sync()?;
        #[cfg(test)]
        self.syncs.store(engine.sync_count(), std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn put_meta(&self) -> Result<(), BitcaskStorageError> {
        let encoded =
            bincode::serialize(&LogMeta { last_index: self.last_index, base_index: self.base })?;
        self.engine.borrow_mut().put(LOG_META_KEY, &encoded)?;
        Ok(())
    }
}

impl RaftStorage for BitcaskStorage {
    type Error = BitcaskStorageError;

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error> {
        let encoded = bincode::serialize(hs)?;
        self.engine.borrow_mut().put(HARD_STATE_KEY, &encoded)?;
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState, Self::Error> {
        match self.engine.borrow_mut().get(HARD_STATE_KEY)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(HardState::default()),
        }
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let expected = self.last_index + 1;
        if first.index != expected {
            return Err(BitcaskStorageError::Gap { expected, got: first.index });
        }
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(BitcaskStorageError::NotContiguous { at: pair[1].index });
            }
        }

        for entry in entries {
            let encoded = bincode::serialize(entry)?;
            self.engine.borrow_mut().put(&entry_key(entry.index), &encoded)?;
        }
        self.last_index = entries.last().expect("checked non-empty").index;
        self.put_meta()
    }

    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error> {
        let hi = hi.min(self.last_index + 1);
        let mut out = Vec::new();
        // A read-only borrow now that `Engine::get` takes `&self`, so this no
        // longer contends with any other reader of the same storage.
        let engine = self.engine.borrow();
        for index in lo.max(1).max(self.base)..hi {
            if let Some(bytes) = engine.get(&entry_key(index))? {
                out.push(bincode::deserialize(&bytes)?);
            }
        }
        Ok(out)
    }

    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error> {
        if idx == 0 {
            return Ok(Some(0));
        }
        if idx > self.last_index {
            return Ok(None);
        }
        if let Some(bytes) = self.engine.borrow().get(&entry_key(idx))? {
            return Ok(Some(bincode::deserialize::<Entry>(&bytes)?.term));
        }
        // The boundary term survives the prefix it describes; below it the
        // log is gone and only the snapshot knows anything.
        if let Some(snap) = &self.snapshot
            && idx == snap.last_included_index
        {
            return Ok(Some(snap.last_included_term));
        }
        Ok(None)
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        if from > self.last_index {
            return Ok(());
        }
        // Never truncate into the snapshot: a suffix conflict at or below the
        // compacted prefix means everything live diverges, so drop all of it
        // and fall back to the snapshot boundary.
        let floor =
            self.base.max(self.snapshot.as_ref().map(|s| s.last_included_index + 1).unwrap_or(1));
        for index in from.max(floor)..=self.last_index {
            self.engine.borrow_mut().delete(&entry_key(index))?;
        }
        let snap_last = self.snapshot.as_ref().map(|s| s.last_included_index).unwrap_or(0);
        self.last_index = from.saturating_sub(1).max(snap_last);
        self.put_meta()
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.last_index)
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(self.snapshot.clone())
    }

    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        let encoded = bincode::serialize(snapshot)?;
        self.engine.borrow_mut().put(SNAPSHOT_KEY, &encoded)?;
        self.last_index = self.last_index.max(snapshot.last_included_index);
        self.snapshot = Some(snapshot.clone());
        self.put_meta()
    }

    fn first_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.base)
    }

    fn truncate_prefix(&mut self, up_to: LogIndex) -> Result<(), Self::Error> {
        if up_to < self.base {
            return Ok(());
        }
        // Meta first, entries after. A crash between them then leaves the
        // entries redundantly stored behind an advanced base — invisible via
        // `entries`/`first_index`, still answering the same terms — instead of
        // entries missing below a base that claims them gone, which would make
        // the leader send a `prev_log_term` of 0 and stall the follower. The
        // leftovers are reclaimed by the next truncation past them; they are
        // never wrong, only briefly wasteful.
        let old_base = self.base;
        self.base = up_to + 1;
        self.put_meta()?;
        for index in old_base..=up_to {
            self.engine.borrow_mut().delete(&entry_key(index))?;
        }
        Ok(())
    }
}
