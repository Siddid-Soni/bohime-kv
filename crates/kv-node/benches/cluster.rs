//! Cluster-level write throughput (the M13 prerequisite, not a milestone).
//!
//! Everything else in this repo that carries a number is a *micro*-benchmark
//! inside one crate. This one spawns three real `kv-node` processes, talks to
//! them through the real `kv-client` over real gRPC, and measures what a
//! caller actually gets: writes per second and the latency distribution
//! behind it, at a fixed durability setting, across shard counts.
//!
//! It exists to answer one question M11 asserted and never measured: **does
//! sharding multiply write throughput?** M11's gate proves that writes to
//! different shards proceed concurrently on different leaders. That is a
//! liveness property. It says nothing about aggregate throughput, which is
//! what a comparison against etcd or TiKV would be about.
//!
//! ## Where the data lives, and why that is the first thing to check
//!
//! The data directories are under `CARGO_TARGET_TMPDIR` — inside `target/`,
//! on this box's btrfs root — and **not** `tempfile::tempdir()`. `/tmp` here
//! is tmpfs, where `fdatasync` returns without doing anything: a group-commit
//! benchmark run there measures nothing at all, because the thing being
//! batched costs zero. `crates/kv-storage/benches/engine.rs`'s fsync-policy
//! arm makes exactly that mistake today. The harness prints the filesystem it
//! is writing to before the first number, so the mistake cannot be made
//! silently twice.
//!
//! Values are drawn from a xorshift PRNG rather than repeated, because `/home`
//! is mounted `compress=zstd:3` and a compressible value would make the write
//! path look free.
//!
//! ## The control arms
//!
//! A throughput number without a control is a number you end up defending.
//! Three run alongside every point:
//!
//! - **`fsync_probe`** — append 4 KiB and `fdatasync`, in a loop, on the same
//!   directory the cluster uses. This is the physical ceiling. Any claimed
//!   write rate has to be consistent with `probe / fsyncs-per-write`, and if
//!   it is not, the benchmark is measuring something else.
//! - **`read`** — the same harness, the same cluster, `Get` instead of `Put`.
//!   A read still takes a ReadIndex quorum round trip but touches no disk on
//!   the write path, so it bounds how much of the write number is the client,
//!   the runtime and gRPC rather than durability. If reads and writes come out
//!   equal, the bottleneck is the harness. It reads back what the write arm
//!   wrote and asserts on a miss — see `keys`, and note that every read number
//!   published before 2026-09-18 was taken against keys that had never been
//!   written.
//! - **`--log-fsync never`** — run the whole sweep a second time with
//!   `BOHIME_BENCH_FSYNC=never`. That arm is the no-durability ceiling for the
//!   real code path. Group commit has to move the default arm *toward* it and
//!   must never pass it.
//! - **`BOHIME_BENCH_DIR=/tmp`** — the same sweep with `fdatasync` removed by
//!   the filesystem rather than by the code. Never a result; only ever the
//!   answer to "is this curve the architecture or is it the disk?".
//!
//! ## What the first run found
//!
//! Three `kv-node` processes on an i9-12900H, btrfs on NVMe, 32 concurrent
//! clients, 256-byte values, RF 3. The raw `fdatasync` control ran at
//! 440-650/s depending on the run.
//!
//! | shards | before | after | tmpfs control (before) |
//! |--------|--------|-------|------------------------|
//! | 1      | 58     | 91    | 121                    |
//! | 8      | 88     | 237   | 1068                   |
//! | 64     | 82     | 137   | 1897                   |
//!
//! Writes per second. "before" is `EveryWrite` on both the log and the state
//! machine; "after" is `--log-fsync group-commit --state-fsync group-commit`.
//!
//! The before column is **flat** — sharding did not multiply write throughput
//! at all. The tmpfs column, the same binary with the disk removed, rises
//! 15.7× over the same range. So the flatness was never the architecture: it
//! was a fixed number of `fdatasync`s per write against a device that does
//! ~500 a second, and every shard's log lands on that one device. Reducing the
//! fsyncs per write is the only thing that moves it, which is why group commit
//! and not pipelining was the first lever.
//!
//! The 64-shard point falling below the 8-shard point is a *different*
//! bottleneck. The `get` arm appeared to show it plainly — reads went
//! 14.4k → 8.9k → 2.2k op/s over the same sweep, touching no disk at all —
//! and that was read as the shared tick loop's per-group cost. **It was not.**
//! It was this harness's own cold clients paying `kv-client`'s 50 ms retry
//! pause once per shard they had to learn a leader for, and the driver loop
//! spends 0.2% of itself on all 64 groups. See
//! `docs/superpowers/plans/2026-09-18-tick-loop.md`. The write arm's turnover
//! is the fsync budget: with `BOHIME_BENCH_DIR=/tmp` it is 83 → 615 → 3295,
//! rising 39.7× with no turnover at all.
//!
//! Which is the standing lesson for this file: a number taken with a fixed
//! client count across a rising shard count is measuring the *load generator*
//! as much as the system, and the first thing to do with any curve here is
//! re-run it with `BOHIME_BENCH_CLIENTS` swept.
//!
//! ## Running it
//!
//! ```text
//! cargo bench -p kv-node --bench cluster
//! BOHIME_BENCH_SHARDS=1,8,64 BOHIME_BENCH_CLIENTS=32,128,512 cargo bench -p kv-node --bench cluster
//! BOHIME_BENCH_ARMS=get cargo bench -p kv-node --bench cluster   # reads only
//! BOHIME_BENCH_FSYNC=every-write cargo bench -p kv-node --bench cluster
//! BOHIME_BENCH_DIR=/tmp cargo bench -p kv-node --bench cluster   # control only
//! ```

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kv_client::Client;

/// Bytes per value. Large enough that the record is not pure overhead, small
/// enough that the disk is not the bandwidth bottleneck instead of the fsync.
const VALUE_LEN: usize = 256;
/// Writes each concurrent client issues in the timed region.
const OPS_PER_CLIENT: usize = 400;
/// Writes each client issues before the clock starts, to get past leader
/// election, the first segment allocation and the client's placement fetch.
const WARMUP_PER_CLIENT: usize = 40;
const NODES: usize = 3;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn shard_counts() -> Vec<u16> {
    match std::env::var("BOHIME_BENCH_SHARDS") {
        Ok(v) => v.split(',').filter_map(|s| s.trim().parse().ok()).collect(),
        Err(_) => vec![1, 8, 64],
    }
}

/// xorshift64*, inline so the value generator needs no `rand` dependency and
/// is reproducible run to run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }
}

// ---------------------------------------------------------------------------
// Process cluster
// ---------------------------------------------------------------------------

struct Node {
    child: Child,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Cluster {
    _nodes: Vec<Node>,
    endpoints: Vec<(u64, String)>,
    _dir: TempDirOnDisk,
}

/// A scratch directory on **real disk**, removed on drop.
///
/// `tempfile::tempdir()` would put this on `/tmp`, which is tmpfs here. The
/// whole point of this benchmark is the cost of `fdatasync`, and tmpfs does
/// not have one.
struct TempDirOnDisk(PathBuf);

impl TempDirOnDisk {
    fn new(tag: &str) -> Self {
        let root = bench_root().join("cluster-bench");
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let path = root.join(format!("{tag}-{pid}-{nonce}"));
        std::fs::create_dir_all(&path).expect("scratch directory is creatable");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirOnDisk {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Where the cluster's data directories go.
///
/// `CARGO_TARGET_TMPDIR` — real disk — unless `BOHIME_BENCH_DIR` overrides it.
/// The override exists for exactly one experiment: pointing it at `/tmp`
/// (tmpfs on this box) makes `fdatasync` a no-op, so the same sweep runs with
/// the durability cost removed and nothing else changed. If the shard curve is
/// flat on disk and not flat on tmpfs, the flatness is the device's fsync
/// budget rather than the architecture. It is a control, never a result: a
/// throughput number taken there is fiction.
fn bench_root() -> PathBuf {
    match std::env::var("BOHIME_BENCH_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_TARGET_TMPDIR")),
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// The `kv-node` executable, rebuilt in release first.
///
/// Same reasoning as M6's gate (`src/tests/end_to_end.rs`): `cargo bench`
/// builds bench binaries, not the crate's `[[bin]]`, so whatever sits in
/// `target/release/kv-node` is from whenever somebody last ran `cargo build
/// --release`. Benchmarking a stale binary is how a wrong number gets
/// published, and this benchmark exists specifically to produce a before and
/// an after that differ by one commit.
fn kv_node_binary() -> PathBuf {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--release", "--quiet", "-p", "kv-node", "--bin", "kv-node"])
        .status()
        .expect("cargo build runs");
    assert!(status.success(), "kv-node failed to build; refusing to benchmark a stale binary");
    let mut path = std::env::current_exe().expect("bench binary has a path");
    path.pop(); // deps/
    path.pop(); // release/
    path.push("kv-node");
    assert!(path.exists(), "kv-node binary not found at {}", path.display());
    path
}

fn spawn_cluster(bin: &Path, shards: u16, fsync: Option<&str>) -> Cluster {
    let dir = TempDirOnDisk::new(&format!("s{shards}"));
    let ports: Vec<u16> = (0..NODES).map(|_| free_port()).collect();
    let nodes = ports
        .iter()
        .enumerate()
        .map(|(i, &port)| {
            let id = i as u64 + 1;
            let node_dir = dir.path().join(format!("n{id}"));
            std::fs::create_dir_all(&node_dir).unwrap();
            let mut cmd = Command::new(bin);
            cmd.arg("--id")
                .arg(id.to_string())
                .arg("--listen")
                .arg(format!("127.0.0.1:{port}"))
                .arg("--data-dir")
                .arg(&node_dir)
                .arg("--shards")
                .arg(shards.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // Passed only when asked for, so the "before" sweep runs against a
            // binary that has no such flag at all.
            if let Some(policy) = fsync {
                cmd.arg("--log-fsync").arg(policy);
            }
            for (j, &peer_port) in ports.iter().enumerate() {
                if j != i {
                    cmd.arg("--peer").arg(format!("{}=http://127.0.0.1:{peer_port}", j + 1));
                }
            }
            Node { child: cmd.spawn().expect("kv-node starts") }
        })
        .collect();
    let endpoints = ports
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64 + 1, format!("http://127.0.0.1:{p}")))
        .collect();
    Cluster { _nodes: nodes, endpoints, _dir: dir }
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

struct Sample {
    elapsed: Duration,
    latencies_us: Vec<u64>,
}

impl Sample {
    fn ops(&self) -> usize {
        self.latencies_us.len()
    }

    fn per_sec(&self) -> f64 {
        self.ops() as f64 / self.elapsed.as_secs_f64()
    }

    fn pct(&self, sorted: &[u64], p: f64) -> f64 {
        if sorted.is_empty() {
            return f64::NAN;
        }
        let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[idx] as f64 / 1000.0
    }

    fn report(&self, label: &str) {
        let mut sorted = self.latencies_us.clone();
        sorted.sort_unstable();
        println!(
            "  {label:<22} {:>9.0} op/s   p50 {:>7.2} ms  p99 {:>8.2} ms  max {:>8.2} ms  ({} ops in {:.2}s)",
            self.per_sec(),
            self.pct(&sorted, 0.50),
            self.pct(&sorted, 0.99),
            self.pct(&sorted, 1.0),
            self.ops(),
            self.elapsed.as_secs_f64(),
        );
    }
}

/// Runs `clients` closed-loop tasks, each issuing `ops` operations in
/// sequence, and times the whole region. Closed loop rather than open: this
/// measures what a fixed pool of callers gets, which is also what makes the
/// latency numbers mean something.
async fn drive<F, Fut>(endpoints: &[(u64, String)], clients: usize, ops: usize, op: F) -> Sample
where
    F: Fn(Client, usize, usize) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Vec<u64>> + Send,
{
    let mut handles = Vec::with_capacity(clients);
    let start = Instant::now();
    // The client's per-attempt deadline, overridable because it is the term
    // that decides where the closed-loop curve breaks: a fixed pool of
    // `clients` breaks when queueing delay crosses it, which by Little's law
    // is at `throughput * deadline` concurrent requests and not before.
    let deadline = std::env::var("BOHIME_BENCH_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis);
    for c in 0..clients {
        let client = Client::new(endpoints.to_vec());
        let client = match deadline {
            Some(d) => client.with_request_timeout(d),
            None => client,
        };
        let op = op.clone();
        handles.push(tokio::spawn(async move { op(client, c, ops).await }));
    }
    let mut latencies = Vec::with_capacity(clients * ops);
    for h in handles {
        latencies.extend(h.await.expect("load task does not panic"));
    }
    Sample { elapsed: start.elapsed(), latencies_us: latencies }
}

/// The key stream one client writes and then reads back.
///
/// **Its own generator**, drawing nothing else from it, and that is the point.
/// Both arms used to share one `Rng` per client and take the key from it —
/// but `put_load` also drew 256 bytes of value from that same stream before
/// each key, so the two arms walked it at different strides and the read arm
/// asked for keys that were never written. Every "get (control)" number
/// before 2026-09-18 is a miss rate of 100%: a real ReadIndex round trip and
/// a real keydir lookup, but never a value off the disk.
///
/// Randomised rather than sequential: sequential keys under one client would
/// land in one shard and the shard sweep would measure nothing. `phase`
/// separates the warmup key space from the timed one, so a timed write is a
/// fresh key rather than an overwrite of a slot the keydir already has.
fn keys(c: usize, phase: u64) -> Rng {
    Rng(0x243F_6A88_85A3_08D3
        ^ phase.wrapping_mul(0xD1B5_4A32_D192_ED03)
        ^ (c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

async fn put_load(mut client: Client, c: usize, ops: usize, phase: u64) -> Vec<u64> {
    let mut keys = keys(c, phase);
    let mut values = Rng(0xA076_1D64_78BD_642F ^ (c as u64).wrapping_mul(0xE703_7ED1_A0B4_28DB));
    let mut value = vec![0u8; VALUE_LEN];
    let mut out = Vec::with_capacity(ops);
    for i in 0..ops {
        values.fill(&mut value);
        let key = format!("bench/{phase}/{c}/{:016x}", keys.next());
        let t = Instant::now();
        client.put(key.as_bytes(), &value).await.unwrap_or_else(|e| {
            panic!("put {i} from client {c} failed: {e}");
        });
        out.push(t.elapsed().as_micros() as u64);
    }
    out
}

/// Reads back what `put_load` wrote for the same client and phase, in the
/// same order. A miss is a bug in the harness, not a result, so it asserts.
async fn get_load(mut client: Client, c: usize, ops: usize, phase: u64) -> Vec<u64> {
    let mut keys = keys(c, phase);
    let mut out = Vec::with_capacity(ops);
    for i in 0..ops {
        let key = format!("bench/{phase}/{c}/{:016x}", keys.next());
        let t = Instant::now();
        let got = client.get(key.as_bytes()).await.expect("get succeeds");
        out.push(t.elapsed().as_micros() as u64);
        assert!(got.is_some(), "get {i} from client {c} missed a key the put arm wrote");
    }
    out
}

/// The physical ceiling: 4 KiB appended and `fdatasync`ed, in a loop, on the
/// directory the cluster is about to use.
///
/// This is the control that catches the tmpfs mistake. On tmpfs it reports
/// millions per second; on this box's btrfs it reports thousands. Reading it
/// first makes the rest of the numbers interpretable.
fn fsync_probe(dir: &Path, iters: usize) -> f64 {
    let path = dir.join("fsync-probe");
    let mut file = std::fs::File::create(&path).expect("probe file");
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    let mut buf = vec![0u8; 4096];
    // Warm the allocation path so the first extent is not in the measurement.
    for _ in 0..16 {
        rng.fill(&mut buf);
        file.write_all(&buf).unwrap();
        file.sync_data().unwrap();
    }
    let start = Instant::now();
    for _ in 0..iters {
        rng.fill(&mut buf);
        file.write_all(&buf).unwrap();
        file.sync_data().unwrap();
    }
    let rate = iters as f64 / start.elapsed().as_secs_f64();
    let _ = std::fs::remove_file(&path);
    rate
}

fn filesystem_of(path: &Path) -> String {
    // /proc/self/mountinfo is the honest answer and needs no external tool.
    let target = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let Ok(mounts) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return "unknown".into();
    };
    let mut best: Option<(usize, String)> = None;
    for line in mounts.lines() {
        let Some((pre, post)) = line.split_once(" - ") else { continue };
        let mount_point = pre.split_whitespace().nth(4).unwrap_or("");
        let fstype = post.split_whitespace().next().unwrap_or("");
        let opts = post.split_whitespace().nth(2).unwrap_or("");
        if target.starts_with(mount_point)
            && best.as_ref().is_none_or(|(len, _)| mount_point.len() > *len)
        {
            best = Some((mount_point.len(), format!("{fstype} [{opts}] at {mount_point}")));
        }
    }
    best.map(|(_, s)| s).unwrap_or_else(|| "unknown".into())
}

/// Which arms to run, in `BOHIME_BENCH_ARMS` (default both).
///
/// The read arm is the one that isolates the driver loop — it takes a
/// ReadIndex quorum round trip and touches no disk — so a sweep hunting a
/// per-group cost wants `BOHIME_BENCH_ARMS=get` and not to spend minutes a
/// point on `fdatasync`.
fn arms() -> (bool, bool) {
    match std::env::var("BOHIME_BENCH_ARMS") {
        Ok(v) => {
            let list: Vec<&str> = v.split(',').map(|s| s.trim()).collect();
            (list.contains(&"put"), list.contains(&"get"))
        }
        Err(_) => (true, true),
    }
}

/// Client counts to sweep at each shard count, in `BOHIME_BENCH_CLIENTS`
/// (comma-separated). Holding one client count fixed while shard count rises
/// starves the added groups of offered load, so a shard curve taken that way
/// measures the methodology as much as the system.
fn client_counts() -> Vec<usize> {
    match std::env::var("BOHIME_BENCH_CLIENTS") {
        Ok(v) => v.split(',').filter_map(|s| s.trim().parse().ok()).collect(),
        Err(_) => vec![32],
    }
}

fn main() {
    let client_counts = client_counts();
    let ops = env_usize("BOHIME_BENCH_OPS", OPS_PER_CLIENT);
    let fsync = std::env::var("BOHIME_BENCH_FSYNC").ok();
    let shards = shard_counts();
    let (do_put, do_get) = arms();

    let root = bench_root();
    std::fs::create_dir_all(&root).unwrap();

    println!("== bohime cluster write throughput ==");
    println!("data directory : {}", root.display());
    println!("filesystem     : {}", filesystem_of(&root));
    println!("nodes          : {NODES} processes, RF 3, loopback gRPC");
    println!("load           : clients {client_counts:?} x {ops} ops, {VALUE_LEN}B values");
    println!("arms           : put={do_put} get={do_get}");
    match &fsync {
        Some(p) => println!("log fsync      : --log-fsync {p}"),
        None => println!("log fsync      : (flag not passed; the binary's default)"),
    }
    println!();
    let probe = fsync_probe(&root, 300);
    println!("control: raw 4KiB append+fdatasync on that directory: {probe:.0} fsync/s");
    println!();

    let bin = kv_node_binary();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for &n in &shards {
        let cluster = spawn_cluster(&bin, n, fsync.as_deref());
        println!("shards = {n}");
        runtime.block_on(async {
            // Warmup: also the readiness gate. The client retries through a
            // cold cluster, so the first successful put is the signal that
            // the meta group elected, published a placement, and the shard
            // groups founded and elected in turn.
            let warm =
                drive(&cluster.endpoints, 32, WARMUP_PER_CLIENT, |c, i, o| put_load(c, i, o, 0))
                    .await;
            println!("  (warmup {} ops in {:.2}s)", warm.ops(), warm.elapsed.as_secs_f64());

            for (i, &clients) in client_counts.iter().enumerate() {
                // Each client count gets its own phase, so a timed write is
                // always a fresh key and the read arm has exactly the keys
                // this client count wrote.
                let phase = i as u64 + 1;
                // The read arm reads back what the write arm wrote, so the
                // write has to happen even when only the read is being
                // measured — untimed, and only as far as this arm will read.
                if do_put || do_get {
                    let puts = drive(&cluster.endpoints, clients, ops, move |c, i, o| {
                        put_load(c, i, o, phase)
                    })
                    .await;
                    if do_put {
                        puts.report(&format!("put   c={clients}"));
                    } else {
                        println!(
                            "  (prefill {} ops in {:.2}s)",
                            puts.ops(),
                            puts.elapsed.as_secs_f64()
                        );
                    }
                }
                if do_get {
                    let gets = drive(&cluster.endpoints, clients, ops, move |c, i, o| {
                        get_load(c, i, o, phase)
                    })
                    .await;
                    gets.report(&format!("get   c={clients}"));
                }
            }
        });
        println!();
        drop(cluster);
    }
}
