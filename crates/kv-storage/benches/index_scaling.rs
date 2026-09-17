//! M11.5's ✅ "read throughput vs. core count (1→20) for `RwLock<HashMap>`,
//! `DashMap`, and left-right".
//!
//! **What is measured, and why it is the keydir alone.** `benches/
//! read_scaling.rs` already measures the whole read — keydir lookup plus one
//! pread plus a record decode — and found that it goes flat at 16 threads on a
//! ceiling *inside the kernel*: every reader shares one `struct file` per
//! segment, and a bare pread loop on a shared handle plateaus at the same
//! ~6.4 Mops/s while one fd per thread reaches ~16.8. That ceiling is lower
//! than any of the three keydirs below, so a whole-read benchmark cannot tell
//! them apart: all three arms would come out flat and equal, and the graph
//! would be a picture of a file descriptor.
//!
//! So this measures the thing left-right actually replaces — the keydir
//! lookup — and `read_scaling.rs` stays as the measurement of what a read
//! costs end to end. Read the two together: this is the ceiling the index
//! imposes, that one is the ceiling the read path currently sits on.
//!
//! Three arms, all over the same 100k-key map with the same pseudo-random
//! access pattern:
//!
//! - `rwlock` — `Arc<RwLock<HashMap>>`, which is what the engine's `locked`
//!   keydir is and what every version before M11.5 used. Every reader takes
//!   the same lock.
//! - `dashmap` — sharded locking. The obvious middle option, included because
//!   "why not just use DashMap" is the first question this design invites.
//! - `left_right` — two copies, one oplog, wait-free reads.
//!
//! and a **writer-present** variant of each: the same read load with one
//! thread writing continuously, which is the case the three actually differ
//! in. With no writer an `RwLock` read lock is uncontended-ish and the point
//! of the exercise is lost.
//!
//! Threads are spawned once per point and parked on a `Barrier`, so the timed
//! region is the work rather than `clone_3`. `left_right` mints one
//! `ReadHandle` per thread outside the timed region, which is how the read
//! path uses it: the factory is shared, the handle is not.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, RwLock};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use dashmap::DashMap;
use left_right::{Absorb, ReadHandle, ReadHandleFactory, WriteHandle};

const KEYS: usize = 100_000;
const OPS_PER_ROUND: usize = 200_000;
const THREAD_COUNTS: [usize; 6] = [1, 2, 4, 8, 16, 20];

/// A location, the same shape as the engine's `ValueLoc`: three words, `Copy`.
/// Duplicated rather than imported because the engine's is crate-private, and
/// a benchmark of the *primitives* should not need the engine's types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Loc {
    segment_id: u32,
    offset: u64,
    len: u32,
}

fn loc(i: usize) -> Loc {
    Loc { segment_id: 0, offset: i as u64 * 64, len: 48 }
}

fn keys() -> Vec<Vec<u8>> {
    (0..KEYS).map(|i| format!("key-{i:016x}").into_bytes()).collect()
}

/// xorshift64*, so the access pattern is the same for every arm and needs no
/// dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

fn rng_for(thread: usize) -> Rng {
    Rng(0x9e37_79b9_7f4a_7c15 ^ (thread as u64).wrapping_mul(0x517c_c1b7) | 1)
}

// ---- the left-right arm's structure and oplog ----

#[derive(Default)]
struct KeyDir {
    entries: HashMap<Vec<u8>, Loc>,
}

enum Op {
    Insert(Vec<u8>, Loc),
}

impl Absorb<Op> for KeyDir {
    fn absorb_first(&mut self, op: &mut Op, _: &Self) {
        match op {
            Op::Insert(key, loc) => {
                self.entries.insert(key.clone(), *loc);
            }
        }
    }

    fn absorb_second(&mut self, op: Op, _: &Self) {
        match op {
            Op::Insert(key, loc) => {
                self.entries.insert(key, loc);
            }
        }
    }

    fn sync_with(&mut self, first: &Self) {
        self.entries.clone_from(&first.entries);
    }
}

/// The three keydirs, each in the two halves the benchmark needs: something
/// shared that readers hold, and a writer.
enum Arm {
    /// No synchronisation at all: a bare `Arc<HashMap>` nobody writes to.
    /// Not a candidate — a keydir is written — but the control that says how
    /// much of each arm's cost is the map and how much is the coordination.
    /// Without it an 8× gap between two arms is a number, not a finding.
    Plain,
    RwLock,
    DashMap,
    LeftRight,
}

impl Arm {
    fn name(&self) -> &'static str {
        match self {
            Arm::Plain => "plain_control",
            Arm::RwLock => "rwlock",
            Arm::DashMap => "dashmap",
            Arm::LeftRight => "left_right",
        }
    }
}

/// What one reader thread holds. Built per thread, outside the timed region.
enum Reader {
    Plain(Arc<HashMap<Vec<u8>, Loc>>),
    RwLock(Arc<RwLock<HashMap<Vec<u8>, Loc>>>),
    DashMap(Arc<DashMap<Vec<u8>, Loc>>),
    LeftRight(ReadHandle<KeyDir>),
}

impl Reader {
    /// What this reader finds for one key. Separate from `read` only so the
    /// sanity check below can use the identical path.
    fn get(&self, key: &[u8]) -> Option<Loc> {
        match self {
            Self::Plain(map) => map.get(key).copied(),
            Self::RwLock(map) => map.read().unwrap().get(key).copied(),
            Self::DashMap(map) => map.get(key).map(|entry| *entry),
            Self::LeftRight(handle) => {
                handle.enter().and_then(|keydir| keydir.entries.get(key).copied())
            }
        }
    }

    fn read(&self, keys: &[Vec<u8>], rng: &mut Rng) {
        for _ in 0..OPS_PER_ROUND {
            let key = &keys[(rng.next() % KEYS as u64) as usize];
            match self {
                Self::Plain(map) => {
                    black_box(map.get(key).copied());
                }
                Self::RwLock(map) => {
                    black_box(map.read().unwrap().get(key).copied());
                }
                Self::DashMap(map) => {
                    black_box(map.get(key).map(|entry| *entry));
                }
                Self::LeftRight(handle) => {
                    black_box(handle.enter().and_then(|keydir| keydir.entries.get(key).copied()));
                }
            }
        }
    }
}

/// What the writer thread holds.
enum Writer {
    /// The control arm has no writer; it is never selected with one.
    Plain,
    RwLock(Arc<RwLock<HashMap<Vec<u8>, Loc>>>),
    DashMap(Arc<DashMap<Vec<u8>, Loc>>),
    LeftRight(WriteHandle<KeyDir, Op>),
}

impl Writer {
    /// One batch of writes followed by one publish, which is the shape the
    /// apply loop produces: `commit_index` advances in batches, so the whole
    /// batch is absorbed and swapped in once.
    fn write_batch(&mut self, keys: &[Vec<u8>], rng: &mut Rng, batch: usize) {
        for _ in 0..batch {
            let i = (rng.next() % KEYS as u64) as usize;
            let key = keys[i].clone();
            match self {
                Self::Plain => unreachable!("the control arm is never run with a writer"),
                Self::RwLock(map) => {
                    map.write().unwrap().insert(key, loc(i));
                }
                Self::DashMap(map) => {
                    map.insert(key, loc(i));
                }
                Self::LeftRight(writer) => {
                    writer.append(Op::Insert(key, loc(i)));
                }
            }
        }
        if let Self::LeftRight(writer) = self {
            writer.publish();
        }
    }
}

/// Builds one arm, populated with every key.
fn build(arm: &Arm, keys: &[Vec<u8>]) -> (Box<dyn Fn() -> Reader + Send + Sync>, Writer) {
    match arm {
        Arm::Plain => {
            let map: HashMap<Vec<u8>, Loc> =
                keys.iter().enumerate().map(|(i, k)| (k.clone(), loc(i))).collect();
            let map = Arc::new(map);
            (Box::new(move || Reader::Plain(Arc::clone(&map))), Writer::Plain)
        }
        Arm::RwLock => {
            let map: HashMap<Vec<u8>, Loc> =
                keys.iter().enumerate().map(|(i, k)| (k.clone(), loc(i))).collect();
            let map = Arc::new(RwLock::new(map));
            let for_readers = Arc::clone(&map);
            (Box::new(move || Reader::RwLock(Arc::clone(&for_readers))), Writer::RwLock(map))
        }
        Arm::DashMap => {
            let map: DashMap<Vec<u8>, Loc> =
                keys.iter().enumerate().map(|(i, k)| (k.clone(), loc(i))).collect();
            let map = Arc::new(map);
            let for_readers = Arc::clone(&map);
            (Box::new(move || Reader::DashMap(Arc::clone(&for_readers))), Writer::DashMap(map))
        }
        Arm::LeftRight => {
            let (mut writer, _reader) = left_right::new::<KeyDir, Op>();
            for (i, key) in keys.iter().enumerate() {
                writer.append(Op::Insert(key.clone(), loc(i)));
            }
            writer.publish();
            // Twice: the first swap leaves one copy still empty (left-right
            // applies pre-first-publish appends directly to the write copy),
            // and the second brings the other up to date through `sync_with`.
            writer.publish();
            let factory: ReadHandleFactory<KeyDir> = writer.factory();
            (Box::new(move || Reader::LeftRight(factory.handle())), Writer::LeftRight(writer))
        }
    }
}

/// `with_writer` is the case the three arms differ in: a reader-only workload
/// barely touches an `RwLock`'s contended path.
fn bench_arm(c: &mut Criterion, arm: Arm, with_writer: bool) {
    let keys = keys();
    let keys = &keys;
    let group_name = if with_writer {
        format!("keydir_{}_writing", arm.name())
    } else {
        format!("keydir_{}", arm.name())
    };
    let mut group = c.benchmark_group(group_name);

    for threads in THREAD_COUNTS {
        let (make_reader, mut writer) = build(&arm, keys);

        // A benchmark of a lookup that finds nothing is a benchmark of a hash
        // and a branch, and it flatters whichever arm has the cheapest miss.
        // left-right is the one at risk twice over: its readers see only what
        // a `publish` swapped in, *and* `enter()` returns `None` the moment
        // the `WriteHandle` is dropped. The first version of this benchmark
        // dropped the writer in the no-writer arms and measured 200k misses
        // at 60 Melem/s — ten times faster than an unsynchronised `HashMap`,
        // which is how it was caught. So this checks, on the same path the
        // timed loop uses, both before the point and after it.
        let probe = |when: &str| {
            let reader = make_reader();
            for i in [0, KEYS / 2, KEYS - 1] {
                assert_eq!(
                    reader.get(&keys[i]),
                    Some(loc(i)),
                    "{} ({when}): the reader cannot see key {i}, so this point measures misses",
                    arm.name()
                );
            }
        };
        probe("before");
        let barrier = Barrier::new(threads + 1);
        let stop = AtomicBool::new(false);
        let writes = AtomicU64::new(0);
        let (barrier, stop, writes) = (&barrier, &stop, &writes);
        let make_reader = &make_reader;

        std::thread::scope(|scope| {
            for t in 0..threads {
                scope.spawn(move || {
                    let reader = make_reader();
                    let mut rng = rng_for(t);
                    loop {
                        barrier.wait();
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        reader.read(keys, &mut rng);
                        barrier.wait();
                    }
                });
            }

            // One thread owns the writer for the whole point, whether or not
            // it writes. It has to exist even in the no-writer arms: dropping
            // a `WriteHandle` makes every `ReadHandle::enter` return `None`,
            // so a writer that goes out of scope does not leave a static map
            // behind — it leaves an unreadable one.
            //
            // The writer runs for the whole point rather than per round, so
            // the readers meet a moving map the way they would in production.
            let writer_thread = scope.spawn(move || {
                let mut rng = rng_for(999);
                while !stop.load(Ordering::Acquire) {
                    if with_writer {
                        writer.write_batch(keys, &mut rng, 64);
                        writes.fetch_add(64, Ordering::Relaxed);
                    } else {
                        // Holding the handle, costing nothing. A spin here
                        // would take a core away from the readers and change
                        // the thing being measured.
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                drop(writer);
            });

            group.throughput(Throughput::Elements((threads * OPS_PER_ROUND) as u64));
            group.bench_function(BenchmarkId::from_parameter(threads), |b| {
                b.iter(|| {
                    barrier.wait();
                    barrier.wait();
                });
            });

            // Still readable after the point, which is what says the map was
            // readable *during* it.
            probe("after");

            stop.store(true, Ordering::Release);
            barrier.wait();
            writer_thread.join().expect("the writer thread does not panic");
        });
    }
    group.finish();
}

fn bench_index_scaling(c: &mut Criterion) {
    for arm in [Arm::Plain, Arm::RwLock, Arm::DashMap, Arm::LeftRight] {
        bench_arm(c, arm, false);
    }
    for arm in [Arm::RwLock, Arm::DashMap, Arm::LeftRight] {
        bench_arm(c, arm, true);
    }
}

criterion_group!(benches, bench_index_scaling);
criterion_main!(benches);
