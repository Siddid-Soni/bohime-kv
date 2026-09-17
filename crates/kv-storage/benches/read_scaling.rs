//! Read-path benchmarks (M11.5's ✅ "read throughput vs. core count (1→20)").
//!
//! `Engine::get` takes `&self` — `read_value_at` uses pread, so the offset
//! travels with the call instead of living in the file's cursor and any
//! number of threads can be inside one `Engine` at once. These benchmarks
//! measure what that actually buys, and what a read costs when it misses the
//! page cache, which is the whole justification for the driver handing reads
//! to `spawn_blocking`.
//!
//! Two groups:
//!
//! - `read_scaling` — N threads sharing one `&Engine`, N ∈ {1,2,4,8,16,20},
//!   in three arms: the real `get`, the in-memory `locate` half of it, and a
//!   syscall-free `cpu_control` that measures what this machine's own
//!   topology allows. Threads are spawned once per point and parked on a
//!   `Barrier`, so the timed region is the work and two barrier crossings,
//!   not `clone_3`.
//! - `single_read` — one `get`, warm versus with the segment files' page cache
//!   dropped (`posix_fadvise(POSIX_FADV_DONTNEED)`) immediately before it.
//!
//! The data directory is deliberately **not** `tempfile::tempdir()`: `/tmp` is
//! tmpfs on this box, where a "cold" read is still a memory read and fsync is
//! a no-op. It lives under `CARGO_TARGET_TMPDIR`, which is inside `target/`
//! on real disk.
//!
//! **What the first run found** (i9-12900H, 6 P-cores + 8 E-cores, btrfs on
//! NVMe): `get` does *not* scale linearly. It goes 0.90 → 6.5 Melem/s from 1
//! to 20 threads — **7.2×**, and flat from 16 threads on. The ceiling is not
//! the keydir and not this crate: threads sharing one `File` contend inside
//! the kernel (`f_count`/`f_ra` on one `struct file`), and a bare pread loop
//! on a shared handle plateaus at the same ~6.4 Mops/s, while the same loop
//! with one fd per thread reaches ~16.8. pread made concurrent reads *legal*;
//! per-reader file handles are what would make them *scale*. `locate` has its
//! own ceiling at ~14.7 Mops/s — it clones `Arc<SegmentMap>`, one contended
//! refcount for every reader.

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use kv_storage::{Engine, EngineConfig, FsyncPolicy, ReadView, ReadViewFactory};
use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};

/// Keys in the working set. 500k × 200-byte values is ~110 MiB of records:
/// comfortably larger than this box's 24 MiB L3 (so the scaling numbers are
/// not measuring cache residency) and comfortably smaller than its 30 GiB of
/// RAM (so the warm arm really is warm).
const KEYS: usize = 500_000;
const VALUE_LEN: usize = 200;
/// Reads each thread performs per timed iteration. Large enough that the two
/// barrier crossings around the iteration are noise (~10 ms of work at one
/// thread), small enough that a 20-thread sweep still finishes quickly.
const READS_PER_THREAD: usize = 25_000;
const THREAD_COUNTS: [usize; 6] = [1, 2, 4, 8, 16, 20];

/// xorshift64*, inline so the read loop needs no `rand` dependency and no
/// precomputed index table competing for cache with the engine's keydir.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
}

fn key_of(i: usize) -> Vec<u8> {
    format!("key-{i:012}").into_bytes()
}

/// Values must not be compressible: the target directory is btrfs with
/// `compress=zstd:3`, and a run of identical bytes would shrink the on-disk
/// footprint enough to make the cold-read number a fiction.
fn value_of(i: usize) -> Vec<u8> {
    let mut rng = Rng(i as u64 | 1);
    (0..VALUE_LEN).map(|_| (rng.next() >> 33) as u8).collect()
}

fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR")).join("read_scaling_data")
}

/// Builds the working set once per process and returns it with every key
/// pre-encoded, so the timed loop allocates nothing of its own.
fn populate() -> (Engine, Vec<Vec<u8>>) {
    let dir = data_dir();
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    // `Never` for the fill (500k fsyncs would dominate setup); one explicit
    // `sync` afterwards, plus the per-file fsync in `drop_page_cache`, is what
    // makes the pages clean enough for `DONTNEED` to actually evict them.
    let config = EngineConfig { fsync_policy: FsyncPolicy::Never, ..Default::default() };
    let mut engine = Engine::open_with_config(&dir, config).unwrap();

    let keys: Vec<Vec<u8>> = (0..KEYS).map(key_of).collect();
    for (i, key) in keys.iter().enumerate() {
        engine.put(key, &value_of(i)).unwrap();
    }
    engine.sync().unwrap();
    (engine, keys)
}

/// Touches every key so the whole working set is page-cache resident.
fn warm(engine: &Engine, keys: &[Vec<u8>]) {
    for key in keys {
        black_box(engine.get(key).unwrap());
    }
}

unsafe extern "C" {
    fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
}

/// `POSIX_FADV_DONTNEED` on Linux.
const FADV_DONTNEED: i32 = 4;

/// Drops the page cache for every segment file in the data directory.
///
/// This is the honest limit of a user-space cold-cache measurement: `fadvise`
/// is advisory, and it can only evict pages that are clean and not mapped
/// elsewhere — hence the `fsync` first. Whether it worked is visible in the
/// result: a genuine NVMe read is two orders of magnitude slower than a
/// page-cache hit, so if `cold` lands near `warm`, it did not.
fn drop_page_cache(dir: &Path) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("seg") {
            continue;
        }
        let file = fs::File::open(&path).unwrap();
        let _ = file.sync_all();
        // SAFETY: `file` owns a valid fd for the duration of the call, and
        // `posix_fadvise` only advises the kernel about that fd's page cache.
        let rc = unsafe { posix_fadvise(file.as_raw_fd(), 0, 0, FADV_DONTNEED) };
        assert_eq!(rc, 0, "posix_fadvise failed for {}", path.display());
    }
}

/// What one worker does per operation.
///
/// `Get` is the thing being measured; the other two are controls, there so a
/// sub-linear `Get` curve can be attributed rather than guessed at.
#[derive(Clone, Copy)]
enum Arm {
    /// The real read path: keydir lookup plus one pread plus a record decode.
    Get,
    /// The half of the read path that stays on the engine's owner thread —
    /// no syscall. Note it clones `Arc<SegmentMap>`, so unlike `Get` it
    /// touches one refcount shared by every thread.
    Locate,
    /// Pure user-space control: random access into the same key table, no
    /// syscall, no shared cache line. This is the ceiling this machine's
    /// 6 P-cores + 8 E-cores and its frequency scaling actually allow, and
    /// the number `Get` should be compared against — not against 20×.
    Cpu,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Get => "get",
            Arm::Locate => "locate",
            Arm::Cpu => "cpu_control",
        }
    }

    /// Chosen per arm so one round is ~10-30 ms of work at one thread, which
    /// keeps the two barrier crossings around it under a percent.
    fn ops_per_round(self) -> usize {
        match self {
            Arm::Get => READS_PER_THREAD,
            Arm::Locate => 250_000,
            Arm::Cpu => 250_000,
        }
    }

    fn run(self, view: &ReadView, keys: &[Vec<u8>], rng: &mut Rng) {
        for _ in 0..self.ops_per_round() {
            let key = &keys[(rng.next() % KEYS as u64) as usize];
            match self {
                Arm::Get => {
                    black_box(view.get(key).unwrap());
                }
                Arm::Locate => {
                    black_box(view.locate(key));
                }
                Arm::Cpu => {
                    let mut h = 0u64;
                    for _ in 0..4 {
                        for b in key {
                            h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
                        }
                    }
                    black_box(h);
                }
            }
        }
    }
}

fn bench_read_scaling(c: &mut Criterion) {
    let (engine, keys) = populate();
    warm(&engine, &keys);
    // Since M11.5 a reader is a `ReadView`, not a shared `&Engine`: the
    // left-right `ReadHandle` inside one is `!Sync`, so each thread mints its
    // own from the shared factory. Minting is per-thread and outside the
    // timed region, which is also how the read path uses it.
    let factory = engine.read_view_factory();
    let factory = &factory;
    let keys = &keys;

    let mut group = c.benchmark_group("read_scaling");
    for arm in [Arm::Get, Arm::Locate, Arm::Cpu] {
        for threads in THREAD_COUNTS {
            let barrier = Barrier::new(threads + 1);
            let stop = AtomicBool::new(false);
            let barrier = &barrier;
            let stop = &stop;

            std::thread::scope(|scope| {
                for t in 0..threads {
                    scope.spawn(move || {
                        let seed = 0x9e37_79b9_7f4a_7c15 ^ (t as u64).wrapping_mul(0x517c_c1b7);
                        let mut rng = Rng(seed);
                        let view = ReadViewFactory::view(factory);
                        loop {
                            barrier.wait();
                            if stop.load(Ordering::Acquire) {
                                return;
                            }
                            arm.run(&view, keys, &mut rng);
                            barrier.wait();
                        }
                    });
                }

                group.throughput(Throughput::Elements((threads * arm.ops_per_round()) as u64));
                group.bench_function(BenchmarkId::new(arm.name(), threads), |b| {
                    b.iter(|| {
                        // Release the workers, then wait for all of them to
                        // finish the round. The timed region is one round.
                        barrier.wait();
                        barrier.wait();
                    });
                });

                stop.store(true, Ordering::Release);
                barrier.wait();
            });
        }
    }
    group.finish();
}

fn bench_single_read(c: &mut Criterion) {
    let (engine, keys) = populate();
    warm(&engine, &keys);
    let dir = data_dir();
    let mut rng = Rng(0xdead_beef_cafe_f00d);

    let mut group = c.benchmark_group("single_read");
    group.throughput(Throughput::Elements(1));

    group.bench_function("warm", |b| {
        b.iter(|| {
            let key = &keys[(rng.next() % KEYS as u64) as usize];
            black_box(engine.get(key).unwrap())
        });
    });

    group.bench_function("cold", |b| {
        b.iter_batched(
            || {
                let key = keys[(rng.next() % KEYS as u64) as usize].clone();
                drop_page_cache(&dir);
                key
            },
            |key| black_box(engine.get(&key).unwrap()),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_read_scaling, bench_single_read);
criterion_main!(benches);
