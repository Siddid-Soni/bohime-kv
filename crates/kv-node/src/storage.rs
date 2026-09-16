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
use kv_storage::Engine;
use serde::{Deserialize, Serialize};

const HARD_STATE_KEY: &[u8] = b"\x00hard_state";
const LOG_META_KEY: &[u8] = b"\x00log_meta";

fn entry_key(index: LogIndex) -> Vec<u8> {
    format!("e{index:020}").into_bytes()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct LogMeta {
    last_index: LogIndex,
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
}

impl BitcaskStorage {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BitcaskStorageError> {
        let path = dir.as_ref().to_path_buf();
        let mut engine = Engine::open(&path)?;
        let last_index = match engine.get(LOG_META_KEY)? {
            Some(bytes) => bincode::deserialize::<LogMeta>(&bytes)?.last_index,
            None => 0,
        };
        Ok(Self {
            engine: RefCell::new(engine),
            #[cfg(test)]
            path,
            last_index,
        })
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
    /// Redundant under the default `FsyncPolicy::EveryWrite`, where each write
    /// has already synced on the way in, and a no-op when nothing is pending.
    /// It is here so the ordering is explicit in the driver rather than an
    /// accident of the policy: switching to `GroupCommit` for throughput must
    /// not silently drop the guarantee.
    pub fn sync(&self) -> Result<(), BitcaskStorageError> {
        self.engine.borrow_mut().sync()?;
        Ok(())
    }

    fn put_meta(&self) -> Result<(), BitcaskStorageError> {
        let encoded = bincode::serialize(&LogMeta { last_index: self.last_index })?;
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
        let mut engine = self.engine.borrow_mut();
        for index in lo.max(1)..hi {
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
        match self.engine.borrow_mut().get(&entry_key(idx))? {
            Some(bytes) => Ok(Some(bincode::deserialize::<Entry>(&bytes)?.term)),
            None => Ok(None),
        }
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        if from > self.last_index {
            return Ok(());
        }
        for index in from..=self.last_index {
            self.engine.borrow_mut().delete(&entry_key(index))?;
        }
        self.last_index = from.saturating_sub(1);
        self.put_meta()
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.last_index)
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(None)
    }
}
