//! The read path's keydir half (M11.5, plan §1.15).
//!
//! `read_engine.rs` took the *disk* read off the driver loop at M11. What was
//! left was the keydir lookup: `Group::answer_read` resolved it on the driver,
//! one read at a time, because `Engine::put` mutates the keydir and a reader
//! cannot hold `&Engine` across that. With the keydir behind `left-right` it
//! can — through a [`ReadView`], which needs no `&mut` and takes no lock.
//!
//! So this is the other end of that change: a small pool of tasks, each
//! holding its own `ReadView` per group, that resolve reads the driver hands
//! them and pass the resolved location on to the read engine. The driver's
//! part of a read is now one channel send.
//!
//! **Why a pool and not a task per read.** A `ReadView` is minted from a
//! factory, and minting one registers an epoch slot behind a mutex inside
//! `left-right`. Doing that per read would put a mutex acquisition back on
//! every read — the exact cost the milestone removes. So handles are
//! long-lived: one per (task, group), minted on first use.
//!
//! **Why not the tonic service struct**, which is where §1.15 and the
//! roadmap's task 4 put the factory: a linearizable read cannot be resolved
//! without a ReadIndex confirmation, and only the driver can produce one. The
//! service would have to be told "this read is confirmed at index N" —
//! a protocol change through `ClientReply`. The factory lives here instead,
//! which keeps the same property the task was after: shared factory, per-task
//! handle, resolution off the driver.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use kv_raft::LogIndex;
use kv_storage::{ReadView, ReadViewFactory};
use tokio::sync::{mpsc, oneshot};

use crate::driver::ClientReply;
use crate::read_engine::ReadEngine;
use crate::transport::group::GroupId;

/// Reads queued for resolution per pool task. Small: a read waits on the
/// driver's own queue before it gets here, and the disk queue is behind this.
const QUEUE_DEPTH: usize = 256;

/// Upper bound on resolver tasks. The work per read is a hash lookup and a
/// channel send, so this is about spreading the two cache misses across cores
/// rather than about parallel throughput; past a handful it buys nothing and
/// costs handles.
const MAX_RESOLVERS: usize = 8;

/// How far a group's state machine has been made **visible to readers**.
///
/// Distinct from its applied index, and that distinction is the milestone.
/// `applied_index` moves when an entry is absorbed into the write copy;
/// this moves when a `publish()` swaps it into the copy readers are on. A
/// read confirmed at index N may be served only once this reaches N —
/// waiting on the applied index instead returns state the confirmation does
/// not cover, which is a stale read that violates linearizability and that
/// no single-threaded test can see.
#[derive(Debug, Default)]
pub(crate) struct Visibility {
    index: AtomicU64,
}

impl Visibility {
    /// Called by the group that owns the engine, after `publish()` returns
    /// true. `Release` so that everything the publish made visible is
    /// ordered before the index that advertises it.
    pub(crate) fn publish(&self, index: LogIndex) {
        self.index.fetch_max(index, Ordering::Release);
    }

    pub(crate) fn index(&self) -> LogIndex {
        self.index.load(Ordering::Acquire)
    }
}

/// One group's reader-side handles: how to mint a view of its state machine,
/// and how far that view is caught up.
#[derive(Clone)]
pub(crate) struct GroupReads {
    pub(crate) factory: ReadViewFactory,
    pub(crate) visible: Arc<Visibility>,
}

/// One read to resolve.
struct ResolveJob {
    group: GroupId,
    key: Vec<u8>,
    /// The index the ReadIndex round confirmed. Carried so the resolver can
    /// check the invariant at the point it actually reads, rather than
    /// trusting that the dispatcher checked it.
    at: LogIndex,
    reply: oneshot::Sender<ClientReply>,
}

/// The pool, from the driver's side.
pub(crate) struct ReadResolvers {
    /// One queue per task rather than one shared queue: an `mpsc` has a single
    /// receiver, and putting a mutex around one would reintroduce the
    /// contention this exists to remove.
    queues: Vec<mpsc::Sender<ResolveJob>>,
    next: AtomicUsize,
    /// Every hosted group's reader handles, for tasks to mint views from.
    /// Written when a group is founded, read on a task's first read of it.
    groups: Arc<std::sync::RwLock<BTreeMap<GroupId, GroupReads>>>,
}

impl ReadResolvers {
    /// Starts the pool. One `ReadEngine` behind it, shared: a ring per
    /// resolver would be several rings' worth of queues for one node's reads.
    pub(crate) fn start() -> Self {
        Self::with_size(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2)
                .clamp(2, MAX_RESOLVERS),
        )
    }

    pub(crate) fn with_size(size: usize) -> Self {
        let reads = Arc::new(ReadEngine::new());
        let groups = Arc::new(std::sync::RwLock::new(BTreeMap::new()));
        let mut queues = Vec::with_capacity(size);
        for _ in 0..size {
            let (tx, rx) = mpsc::channel(QUEUE_DEPTH);
            queues.push(tx);
            tokio::spawn(resolve(rx, Arc::clone(&reads), Arc::clone(&groups)));
        }
        Self { queues, next: AtomicUsize::new(0), groups }
    }

    /// Makes a group's state machine readable. Called by the driver when it
    /// takes a group on.
    pub(crate) fn host(&self, group: GroupId, reads: GroupReads) {
        self.groups.write().expect("read registry lock is never poisoned").insert(group, reads);
    }

    /// Hands one confirmed read to the pool.
    ///
    /// Round-robin rather than least-loaded: the work is a lookup and a send,
    /// so the imbalance a counter leaves is smaller than the bookkeeping to
    /// avoid it. A full queue means the disk is far behind; the driver's own
    /// queue sheds long before this, so the read is dropped rather than
    /// blocking the loop.
    pub(crate) fn submit(
        &self,
        group: GroupId,
        key: Vec<u8>,
        at: LogIndex,
        reply: oneshot::Sender<ClientReply>,
    ) {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.queues.len();
        if self.queues[index].try_send(ResolveJob { group, key, at, reply }).is_err() {
            tracing::error!(group, "read resolver queue is full; dropping read");
        }
    }
}

/// One resolver task.
///
/// The `ReadView`s live in this task's own `views` map for as long as it runs.
/// They are `!Sync` — that is exactly the type-level statement that they are
/// this task's and no one else's.
async fn resolve(
    mut jobs: mpsc::Receiver<ResolveJob>,
    reads: Arc<ReadEngine>,
    groups: Arc<std::sync::RwLock<BTreeMap<GroupId, GroupReads>>>,
) {
    let mut views: BTreeMap<GroupId, (ReadView, Arc<Visibility>)> = BTreeMap::new();

    while let Some(job) = jobs.recv().await {
        let ResolveJob { group, key, at, reply } = job;
        if let std::collections::btree_map::Entry::Vacant(slot) = views.entry(group) {
            let Some(hosted) =
                groups.read().expect("read registry lock is never poisoned").get(&group).cloned()
            else {
                // The group went away between the driver dispatching this and
                // this task picking it up. Dropping the sender answers the
                // client as unavailable, which is what it retries.
                tracing::debug!(group, "no reader handles for this group");
                continue;
            };
            slot.insert((hosted.factory.view(), hosted.visible));
        }
        let (view, visible) = views.get(&group).expect("just inserted");

        // Diagnostics, not a second gate. The guarantee is the driver's:
        // `Group::serve_ready_reads` dispatches a read only once its
        // confirmed index is published. Refusing the read here as well would
        // turn the stale read this milestone is about into a `NotLeader` the
        // client silently retries — the bug would be invisible and the
        // regression test would pass against the wrong wait. So if the
        // invariant is ever broken, this says so and the answer is wrong,
        // which is what a test can see.
        let published = visible.index();
        if published < at {
            tracing::error!(
                group,
                confirmed_at = at,
                published,
                "a read reached the resolver before its index was visible: \
                 this answer may be stale (M11.5, §1.15)"
            );
        }

        match view.locate(&key) {
            Some(located) => reads.submit(located, reply),
            None => {
                let _ = reply.send(ClientReply::Value(None));
            }
        }
    }
}
