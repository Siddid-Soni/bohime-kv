//! Engine configuration (M1.7). The fsync policy is the durability knob: it
//! decides how many acknowledged writes the engine is willing to lose to a
//! machine crash in exchange for throughput. The decision itself is a pure
//! function so it can be tested without a disk.

use std::time::Duration;

/// When the engine flushes the active segment to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Never sync explicitly; the OS flushes when it feels like it. Fastest,
    /// and loses every unflushed write on a machine crash. Benchmarks and
    /// caches only.
    Never,
    /// Sync before every `put`/`delete` returns. Slowest, and the only policy
    /// under which a returned write is actually durable.
    EveryWrite,
    /// Sync once per batch: whenever `max_records` writes have accumulated or
    /// `max_delay` has elapsed since the last sync, whichever comes first.
    /// Bounds the loss window in both records and time.
    GroupCommit { max_records: usize, max_delay: Duration },
}

impl FsyncPolicy {
    /// Whether an engine holding `unsynced` unflushed records, `elapsed`
    /// since its last flush, should flush now. Always false when nothing is
    /// pending — an idle engine must not issue fsyncs forever.
    pub(crate) fn should_sync(&self, unsynced: usize, elapsed: Duration) -> bool {
        if unsynced == 0 {
            return false;
        }
        match *self {
            FsyncPolicy::Never => false,
            FsyncPolicy::EveryWrite => true,
            FsyncPolicy::GroupCommit { max_records, max_delay } => {
                unsynced >= max_records || elapsed >= max_delay
            }
        }
    }
}

/// Which keydir implementation the engine holds (M11.5, plan §1.15).
///
/// A flag rather than a compile-time choice because the two have different
/// costs and neither dominates: left-right doubles the keydir's memory (plus
/// the pending overlay) to buy reads that never take a lock, and the keydir is
/// ~40 bytes a key with every key resident. An operator with 10M keys a shard
/// and a read-light workload should be able to say no.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexKind {
    /// `Arc<RwLock<HashMap>>`. Every write is visible the moment it returns.
    Locked,
    /// Wait-free reads through two copies and an oplog. A write is visible
    /// only after `publish`.
    #[default]
    LeftRight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineConfig {
    pub max_segment_size: u64,
    pub fsync_policy: FsyncPolicy,
    pub index: IndexKind,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_segment_size: 64 * 1024 * 1024,
            fsync_policy: FsyncPolicy::EveryWrite,
            index: IndexKind::default(),
        }
    }
}
