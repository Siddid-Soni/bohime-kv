//! The read path's I/O engine.
//!
//! A `Get` that has passed ReadIndex needs one disk read. The question is who
//! waits for it. Doing it inline stalls the driver's `select!` loop — ticks
//! included — for the whole of a seek, once per read. Handing each read to a
//! blocking thread fixes that but binds concurrency to threads: one read in
//! flight per thread, plus a handoff that costs more than a page-cache hit.
//!
//! Real databases do neither. Seastar/ScyllaDB, TigerBeetle and modern
//! Postgres keep **many reads in flight per thread** through a submission and
//! completion queue. That is what `UringEngine` does: one thread owns a ring,
//! submits every read that arrives, and reaps completions as the kernel
//! finishes them. Concurrency is bounded by the ring, not by thread count.
//!
//! **`io_uring` is not assumed.** Docker's default seccomp profile blocks its
//! syscalls, hardened hosts set `io_uring_disabled` to 1 or 2, and plenty of
//! kernels predate the operations used here. All of those are deployments this
//! has to serve, so ring construction is a runtime probe and `BlockingEngine`
//! is a first-class fallback rather than a panic path. `ReadEngine::new`
//! chooses once, at startup, and logs which it got.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use io_uring::{IoUring, opcode, types};
use kv_storage::ValueRef;
use tokio::sync::oneshot;

use crate::driver::ClientReply;

/// Ring depth, and so the ceiling on reads in flight at once. Submissions past
/// it wait for a completion rather than being dropped — backpressure, not loss.
const RING_ENTRIES: u32 = 256;

/// Bound on reads queued but not yet submitted. Full means the disk is behind;
/// the driver sheds at its own queue long before this, so this exists to keep
/// memory bounded rather than to be hit.
const QUEUE_DEPTH: usize = 1024;

/// Operator override for the read path: `io_uring`, `blocking`, or `auto`.
/// Exists so a ring can be turned off in response to a kernel advisory without
/// shipping a new binary.
const ENGINE_ENV: &str = "BOHIME_READ_ENGINE";

/// One read to perform: where it lives, and who is waiting for it.
///
/// The `ValueRef` is held until the read completes, not merely until it is
/// submitted. It keeps the segment map alive, and that is what holds the
/// descriptor open underneath an in-flight submission.
struct Job {
    value: ValueRef,
    reply: oneshot::Sender<ClientReply>,
}

/// Performs the reads the driver resolves.
pub(crate) enum ReadEngine {
    Uring(UringEngine),
    Blocking(BlockingEngine),
}

impl ReadEngine {
    /// Takes a ring if the kernel will give us one and the operator has not
    /// said otherwise. Called once at startup: probing per read would be a
    /// syscall per read to answer a question whose answer cannot change.
    ///
    /// An unusable `BOHIME_READ_ENGINE` is logged and ignored rather than
    /// fatal — refusing to start over a malformed performance knob would turn
    /// a typo into an outage.
    pub(crate) fn new() -> Self {
        let setting = std::env::var(ENGINE_ENV).ok();
        match Self::from_setting(setting.as_deref()) {
            Ok(engine) => engine,
            Err(value) => {
                tracing::warn!(
                    setting = %value,
                    "unrecognised {ENGINE_ENV}; expected io_uring, blocking, or auto. \
                     Falling back to auto"
                );
                Self::from_setting(None).expect("auto is always valid")
            }
        }
    }

    /// Resolves the configured preference. `Err` carries the offending value.
    ///
    /// `io_uring` here is a *preference*, not a demand: if the operator asks
    /// for it and the kernel refuses, serving reads slowly beats not serving
    /// them. The log line is what tells them which they got.
    pub(crate) fn from_setting(setting: Option<&str>) -> Result<Self, String> {
        match setting.map(str::trim) {
            None | Some("") | Some("auto") => Ok(Self::detect()),
            Some("io_uring") => match Self::io_uring() {
                Some(engine) => {
                    tracing::info!(entries = RING_ENTRIES, "read path: io_uring (requested)");
                    Ok(engine)
                }
                None => {
                    tracing::warn!("io_uring requested but unavailable; using blocking fallback");
                    Ok(Self::blocking())
                }
            },
            Some("blocking") => {
                tracing::info!("read path: blocking fallback (requested)");
                Ok(Self::blocking())
            }
            Some(other) => Err(other.to_string()),
        }
    }

    fn detect() -> Self {
        match Self::io_uring() {
            Some(engine) => {
                tracing::info!(entries = RING_ENTRIES, "read path: io_uring");
                engine
            }
            None => {
                tracing::info!(
                    "read path: blocking fallback (no io_uring — seccomp, \
                     io_uring_disabled, or an older kernel)"
                );
                Self::blocking()
            }
        }
    }

    /// `None` when no ring is available. Public to the crate so the tests can
    /// exercise each backend deliberately instead of whichever the host
    /// happens to allow.
    pub(crate) fn io_uring() -> Option<Self> {
        UringEngine::start().map(Self::Uring)
    }

    pub(crate) fn blocking() -> Self {
        Self::Blocking(BlockingEngine)
    }

    /// Hands off one resolved read. Never blocks the caller on the disk, which
    /// is the entire contract: this is called from the driver's loop.
    pub(crate) fn submit(&self, value: ValueRef, reply: oneshot::Sender<ClientReply>) {
        match self {
            Self::Uring(engine) => engine.submit(Job { value, reply }),
            Self::Blocking(engine) => engine.submit(Job { value, reply }),
        }
    }
}

/// The fallback: one blocking-pool task per read.
///
/// Kept deliberately simple. It is the path that runs where a ring is
/// unavailable, so its virtue is that there is very little of it to be wrong.
pub(crate) struct BlockingEngine;

impl BlockingEngine {
    fn submit(&self, job: Job) {
        tokio::task::spawn_blocking(move || {
            let value = job.value.read().ok();
            let _ = job.reply.send(ClientReply::Value(value));
        });
    }
}

/// A dedicated thread owning an `io_uring`, fed over a bounded channel.
pub(crate) struct UringEngine {
    jobs: SyncSender<Job>,
}

impl UringEngine {
    /// Builds the ring on the worker thread and reports back whether it came
    /// up, so a kernel that refuses one is a `None` here rather than a failure
    /// later, when there would be a client waiting on it.
    fn start() -> Option<Self> {
        let (jobs, receiver) = sync_channel(QUEUE_DEPTH);
        let (ready, started) = std::sync::mpsc::channel();

        let spawned = std::thread::Builder::new().name("kv-read-uring".into()).spawn(move || {
            match IoUring::new(RING_ENTRIES) {
                Ok(ring) => {
                    let _ = ready.send(true);
                    run(ring, receiver);
                }
                Err(error) => {
                    tracing::debug!(%error, "io_uring unavailable");
                    let _ = ready.send(false);
                }
            }
        });

        if spawned.is_err() {
            return None;
        }
        // The worker owns `receiver`; dropping `jobs` on the failure path lets
        // it exit cleanly rather than parking on a channel nobody feeds.
        match started.recv() {
            Ok(true) => Some(Self { jobs }),
            _ => None,
        }
    }

    fn submit(&self, job: Job) {
        // Blocks only if a thousand reads are already queued, which means the
        // disk is far behind; the driver's own bounded queue sheds first.
        if self.jobs.send(job).is_err() {
            tracing::error!("read engine thread is gone; dropping read");
        }
    }
}

/// A submission the kernel is still working on.
///
/// `buf` must not be touched until the completion arrives — the kernel holds a
/// pointer into it. Moving this struct is fine: the pointer is into the `Vec`'s
/// heap allocation, which does not move when the struct does.
struct InFlight {
    value: ValueRef,
    reply: oneshot::Sender<ClientReply>,
    buf: Vec<u8>,
    /// Bytes already read. `io_uring` may return a short read, exactly as
    /// `read(2)` may, so the remainder is resubmitted from here.
    filled: usize,
}

/// The worker loop: accept jobs, submit reads, reap completions.
///
/// Blocks on the channel only when nothing is outstanding, and on the ring only
/// when something is. That way an idle engine costs nothing and a busy one
/// never sleeps holding completed work.
fn run(mut ring: IoUring, jobs: Receiver<Job>) {
    let mut in_flight: HashMap<u64, InFlight> = HashMap::new();
    let mut next_id: u64 = 0;

    loop {
        if in_flight.is_empty() {
            // Nothing outstanding: wait for work, and exit when the driver
            // drops its sender.
            match jobs.recv() {
                Ok(job) => start_read(&mut ring, &mut in_flight, &mut next_id, job),
                Err(_) => return,
            }
        }

        // Take whatever else has arrived, up to the ring's depth.
        while in_flight.len() < RING_ENTRIES as usize {
            match jobs.try_recv() {
                Ok(job) => start_read(&mut ring, &mut in_flight, &mut next_id, job),
                Err(_) => break,
            }
        }

        if in_flight.is_empty() {
            continue;
        }
        if ring.submit_and_wait(1).is_err() {
            // A ring that will not submit cannot be recovered from here.
            // Answering `None` would be indistinguishable from a missing key,
            // so drop the senders instead: the driver reports the read as
            // failed rather than as absent.
            tracing::error!("io_uring submit failed; failing outstanding reads");
            in_flight.clear();
            continue;
        }

        let completions: Vec<(u64, i32)> =
            ring.completion().map(|cqe| (cqe.user_data(), cqe.result())).collect();
        for (id, result) in completions {
            complete(&mut ring, &mut in_flight, id, result);
        }
    }
}

/// Pushes one read onto the ring's submission queue.
fn start_read(
    ring: &mut IoUring,
    in_flight: &mut HashMap<u64, InFlight>,
    next_id: &mut u64,
    job: Job,
) {
    let id = *next_id;
    *next_id = next_id.wrapping_add(1);

    let len = job.value.len();
    let entry = InFlight { value: job.value, reply: job.reply, buf: vec![0u8; len], filled: 0 };
    in_flight.insert(id, entry);
    push(ring, in_flight, id);
}

/// Builds and pushes the SQE for whatever of `id`'s read is still outstanding.
fn push(ring: &mut IoUring, in_flight: &mut HashMap<u64, InFlight>, id: u64) {
    let Some(entry) = in_flight.get_mut(&id) else {
        return;
    };
    let offset = entry.value.offset() + entry.filled as u64;
    let remaining = entry.buf.len() - entry.filled;
    // SAFETY: the pointer is into `entry.buf`'s heap allocation, which lives in
    // `in_flight` until this read completes and is not touched meanwhile. The
    // descriptor comes from `entry.value`, which is held for the same span and
    // is what keeps it open.
    let ptr = unsafe { entry.buf.as_mut_ptr().add(entry.filled) };
    let sqe = opcode::Read::new(types::Fd(entry.value.raw_fd()), ptr, remaining as u32)
        .offset(offset)
        .build()
        .user_data(id);

    // SAFETY: `sqe` refers only to the buffer and descriptor described above,
    // both of which outlive the submission.
    if unsafe { ring.submission().push(&sqe) }.is_err() {
        // The queue is full. Flush it and retry once; the ring is sized so this
        // is rare, and a second failure means something is badly wrong.
        let _ = ring.submit();
        if unsafe { ring.submission().push(&sqe) }.is_err() {
            tracing::error!("io_uring submission queue full; failing read");
            in_flight.remove(&id);
        }
    }
}

/// Handles one completion: finish, continue a short read, or fail.
fn complete(ring: &mut IoUring, in_flight: &mut HashMap<u64, InFlight>, id: u64, result: i32) {
    let Some(entry) = in_flight.get_mut(&id) else {
        return;
    };

    if result < 0 {
        let error = std::io::Error::from_raw_os_error(-result);
        // EINTR and EAGAIN are transient; anything else is real.
        if matches!(error.kind(), std::io::ErrorKind::Interrupted) {
            push(ring, in_flight, id);
            return;
        }
        tracing::warn!(%error, "read failed");
        in_flight.remove(&id);
        return;
    }

    if result == 0 {
        // EOF short of the record. The keydir pointed past the end of the
        // segment, which means the index and the file disagree.
        tracing::error!(id, "short read hit EOF before the record ended");
        in_flight.remove(&id);
        return;
    }

    entry.filled += result as usize;
    if entry.filled < entry.buf.len() {
        // A short read, which `read(2)` and so `io_uring` are both entitled to
        // return. Continue from where it stopped.
        push(ring, in_flight, id);
        return;
    }

    let entry = in_flight.remove(&id).expect("just borrowed");
    // Decoding — including the CRC check — belongs to kv-storage, so the two
    // read paths cannot disagree about what a record means.
    let value = entry.value.decode(&entry.buf).ok();
    let _ = entry.reply.send(ClientReply::Value(value));
}
