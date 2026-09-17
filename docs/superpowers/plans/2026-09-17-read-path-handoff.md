# Handoff — read-path and request-batching work (2026-09-17)

Picks up after M7 (`6c704b9`). This is **not** a milestone in `docs/DESIGN.md`;
it is out-of-order work done at the user's direction, ahead of M8.

**Branch:** `m6-single-shard-node`. **Nothing is committed.** Nine files are
modified and two are untracked (`crates/kv-storage/benches/read_scaling.rs` and
this document). The user has not asked for a commit — do not make one unless
they do.

## The question that started it

The user asked where `left-right` is used and said reads and writes should
carry no locks that slow performance.

**Findings, both verified by grep, both load-bearing for everything below:**

1. **`left-right` is used nowhere.** No `use left_right`, no `Absorb`, no
   `ReadHandle`, no `publish()` in any crate. It appears only as an unused
   dependency (`Cargo.toml:32`, `kv-storage/Cargo.toml:15`,
   `kv-node/Cargo.toml:20`) reserving a name for M11.5, plus comments.
   `KeyDirIndex` is the seam it will slot into; the live impl is `HashMapIndex`.
2. **There were already no locks.** The only `Mutex`/`RwLock` in `crates/` is
   the test Switchboard (`kv-node/src/tests/cluster.rs:42`).

That second point is the one to carry forward: **"no locks" and "parallel" are
different claims, and this codebase had the first without the second.** There
was nothing to contend over because `Engine` is owned outright by one `Driver`
and every request funnels through one bounded mpsc into one `select!` loop.
That is *worse* than `RwLock<HashMap>` for read throughput — an `RwLock` at
least admits N readers at once.

## What was changed

All TDD. Every test was watched fail first; the observed failure is recorded
next to each item because it is the evidence the test can catch the bug.

### 1. ReadIndex rounds batch — `kv-raft/src/node.rs`

`read_index` used to do `read_round += 1; round_acks.clear()` per call, so a
second reader **discarded the first reader's in-flight confirmation**. Under
read arrival faster than the peer round-trip, `round_acks` was cleared before
it could ever reach quorum and reads starved until a lull. Never stale, so
every M7 test passed; just unboundedly slow.

Now: a read arriving while a round is outstanding goes to `queued_reads` and
shares the *next* round. `try_confirm_reads` loops — it confirms the current
round, then promotes the queue and opens one more round.

**Safety argument, do not break it:** a queued read is confirmed only by a
round broadcast *after* it arrived, so it never inherits evidence predating its
own request. That is what `an_ack_from_before_the_read_does_not_confirm_it`
guards.

- `a_second_read_does_not_discard_the_first_rounds_acks` — RED: `left: []`, `right: [7]`
- `reads_arriving_during_a_round_share_the_next_one` — RED: a read broadcast a round of its own

### 2. Proposals batch — `kv-raft/src/node.rs`

`propose` broadcast to every peer immediately, so N writes between two drains
cost N×peers messages, each a superset of the last — only the final one
mattered. Now `replication_pending` defers to the next drain;
`broadcast_heartbeats` clears it, since a broadcast *is* the flush.

- `proposals_arriving_together_share_one_append` — RED: `left: 8, right: 2`

**A regression was found and fixed while doing this.** `tick`/`step` and
`ready` are documented as alternative drains of the same queue, and kv-raft's
own test harness (`src/tests/harness.rs`) uses only the former. A deferred
proposal was invisible to it. `flush_replication` is now called from `tick`
and `step` too.

- `a_deferred_proposal_surfaces_in_the_next_tick` — RED: `left: 0, right: 2`

Liveness does not depend on the flush: an unflushed proposal still ships on the
next heartbeat, which sends from `next_index` regardless.

### 3. `Engine::get` takes `&self` — `kv-storage/src/engine.rs`

`read_value_at` used `seek` + `read_exact`. A seek mutates the file's shared
cursor, which is the **only** reason `get` ever needed `&mut self`. It now uses
`read_exact_at` (pread): the offset travels with the call, so concurrent
readers cannot move each other's cursor.

This removed the *correctness* obstacle to concurrent reads — the shared
cursor — and was necessary. It is not a lock being removed; there was no lock.

**It did not deliver scaling.** See "Measured, and it contradicts the above"
below. Do not repeat the claim that pread makes reads scale with cores.

- `many_threads_read_one_engine_concurrently` — RED: `cannot borrow *engine as mutable`

Knock-on: `BitcaskStorage::entries` dropped from `borrow_mut()` to `borrow()`.
Linux-only now (`std::os::unix::fs::FileExt`), which the project already is.

### 4. Reads leave the Raft loop — `kv-storage`, `kv-node/src/driver.rs`

`Engine::locate(key) -> Option<ValueRef>` does the keydir lookup in memory and
returns the resolved offset plus an `Arc<SegmentMap>` snapshot. `Driver::
answer_read` calls it on the driver task, then hands the `ValueRef` to
`spawn_blocking` for the actual pread.

The split is the point: **the keydir stays on the single owner because it is
what races with `apply`; only immutable state crosses the thread boundary.**

- `a_located_value_reads_from_another_thread_while_the_owner_writes` — RED: `no method named locate`
- `a_located_value_survives_compaction_deleting_its_segment` — same

Two design notes:

- `segments` became `Arc<SegmentMap>`, mutated copy-on-write via
  `Arc::make_mut`. (The map's value type was `Arc<File>` at this point;
  change 5 below replaced it with `SegmentHandles`.) **`ArcSwap` was
  considered and rejected**: only the driver mutates the pointer, so ArcSwap
  buys nothing over COW. The user approved "segments via ArcSwap" and was told
  of the substitution. Revisit only if consistency with M10's shard map is
  wanted.
- Compaction unlinks a segment a reader may hold. On Unix the inode outlives
  the directory entry while an fd refers to it, so the read still returns
  correct bytes. That is asserted, not assumed.

## Evidence

- `cargo nextest run --workspace` — **212 passed** (8 new), 2 skipped
- `cargo fmt --all -- --check` — clean
- `cargo clippy --workspace --all-targets -- -D warnings` — clean
- **100k-seed nemesis sweep, release: two runs, 682s and 678s, both exit 0.**
  The second covers all `kv-raft` changes. `kv-sim` depends only on `kv-raft`,
  `rand`, `tracing` — not `kv-storage` or `kv-node` — so the later `ValueRef`
  and driver work cannot affect that gate.
- CI tokio guard: `cargo tree` confirms neither `kv-raft` nor `kv-sim` has
  picked up tokio.

Baseline in CLAUDE.md is 863s at `6c704b9`. The 682s/678s runs are consistent
with fewer simulated messages but were single runs on a box that was also
compiling. **Do not write them into the docs as a measured speedup.**

## Measured, and it contradicts the above

`crates/kv-storage/benches/read_scaling.rs` (added after the four changes;
second `[[bench]]` target, `engine.rs` untouched). Run with
`cargo bench -p kv-storage --bench read_scaling`.

**Read scaling is not linear.** `Engine::get`, N threads sharing one `&Engine`,
two independent runs plus a third confirming run:

| threads | 1 | 2 | 4 | 8 | 16 | 20 |
|---|---|---|---|---|---|---|
| Melem/s | 0.83–0.90 | 1.59–1.63 | 2.70–2.73 | 4.11–4.20 | 6.70–6.87 | 6.09–6.50 |

**1→20 is ~7.2×, and the curve is flat from 16 threads on** — 20 threads was at
or below 16 in two of three runs. Per-read latency goes 1.18 µs → 3.3 µs.

Part of that is the machine: this box is an i9-12900H, **20 logical CPUs but 14
heterogeneous physical cores** (6 P + HT, 8 E), so 20× never existed. A
syscall-free control arm tops out at 11.2×. `get` reaches ~64% of that.

**The rest is the shared `Arc<File>`, and it is in the kernel.** A standalone
pread diagnostic (200-byte reads, warm page cache) isolates it:

| | 1 thr | 8 thr | 20 thr | 1→20 |
|---|---|---|---|---|
| memcpy control | 15.60 | 63.68 | 94.47 | 6.1× |
| all threads share one `File` | 2.16 | 6.39 | **6.42** | **3.0×** |
| one fd each, same inode | 2.04 | 11.42 | 16.78 | 8.2× |
| one file each | 2.30 | 12.61 | 17.36 | 7.5× |

Per-fd ≈ per-file, so the inode is not the contention point — the shared
`struct file` is (`f_count`, bumped by `fget` on every syscall once the fd
table is shared, plus the per-handle readahead state `f_ra`). `Engine::get` at
16–20 threads sits *exactly* on the shared-handle ceiling.

**Consequence for M11.5, and it is the important one:** §1.15 promises reads
that "scale linearly with cores." `left-right` fixes the **keydir**, which is
not what is capping this curve. One `Arc<File>` per segment plateaus at ~6.4M
reads/s no matter how many cores you add. **Per-reader file handles (dup'd fds,
or `io_uring` with registered fds) matter more here than `left-right` does.**
Anyone who builds M11.5's scaling graph without fixing the handle first will
find it flat and misattribute the cause.

Secondary, and a self-inflicted one: `Engine::locate` clones
`Arc<SegmentMap>` per call, so under N threads it contends on a single
refcount and saturates at **14.7 Mops/s from 8 threads onward** (3.85×, flat to
20). This does **not** bite today — `locate` is called only from the single
driver task — but it becomes a ceiling the moment reads move fully off the
driver, which is exactly what M11.5 is for.

**Warm vs. cold:** 1.17 µs warm, 69.4 µs cold — a **59× gap**. Cold is honest:
`posix_fadvise(POSIX_FADV_DONTNEED)` after fsync, run untimed in criterion's
`iter_batched` setup, with eviction verified independently via `fincore`
(16384 pages resident → 0 pages after). This supports the *direction* of the
`answer_read` trade — a few-µs `spawn_blocking` handoff is clearly bad against
1.17 µs and clearly good against 69.4 µs, and 59× is wide enough that the exact
handoff cost doesn't change the conclusion. The handoff cost itself was **not**
measured; that needs a bench in `kv-node`, since `kv-storage` has no tokio and
should not grow one.

Not measured, stated plainly: a cold *device* (only page cache was evicted, not
the drive cache or FTL, so 69 µs is a floor not a worst case); cold reads
*under concurrency* (fadvise cannot hold with 20 threads re-warming); and
`spawn_blocking`'s own cost.

Caveats on all numbers: threads are not pinned, and the frequency governor
ramps with load, so the 1-thread points run at a different clock than the
20-thread ones. Treat 7.2× as approximate; run-to-run spread was ~±7%.

### Two incidental findings worth acting on

1. **`/tmp` on this box is tmpfs.** Any bench using `tempfile::tempdir()` is
   measuring memory, and fsync there is a no-op. The **existing
   `benches/engine.rs` fsync-policy bench does this**, so its `every_write` arm
   is not currently measuring fsync. `read_scaling.rs` uses
   `CARGO_TARGET_TMPDIR` instead.
2. **`/home` is btrfs with `compress=zstd:3`.** Benchmark values must be
   pseudo-random or the cold-read number is fiction.

## What is NOT done

**The funnel is still there.** Reads no longer block the loop on disk, but
every `Get` still crosses one bounded mpsc into one driver task for the keydir
lookup. Removing that requires the keydir to be readable from other threads.

`Engine::put` mutates **both** `self.index` (every write) and `self.segments`
(on rotation). So a reader thread cannot hold `&Engine` while the driver
writes. The only two ways out:

- a lock — `RwLock<Engine>`, which the user has explicitly ruled out;
- the keydir behind `left-right` plus segment handles shared — i.e. **M11.5**.

This is the honest conclusion of the whole investigation: **`left-right` is not
optional decoration for this problem, it is the thing that lets a reader see a
*mutating* keydir with no lock — exactly what `DESIGN.md` §1.15 claims.** What
was wrong was the ordering, not the tool. pread had to land first, or
left-right would have removed the keydir obstacle and left `&mut File` blocking
every reader anyway.

**No benchmark exists.** The `spawn_blocking` handoff costs more than a
page-cache hit; it pays off because Bitcask guarantees one disk seek per
uncached value, so past the page cache an inline read stalls the `select!`
loop — ticks included — once per read. On a fully cached working set the change
is net negative. That trade is asserted in a comment on `answer_read` and
**has not been measured.** M11.5's own ✅ criteria require exactly this
benchmark ("read throughput vs. core count (1→20)").

## Open decisions for the user

1. ~~Build the read benchmark.~~ **Done** — see "Measured" above. It changed
   the picture: fix the shared file handle before anything else claims read
   scaling.
2. ~~Give each reader its own fd.~~ **Done, and it under-delivered.** See
   "Per-segment handle fan-out" below. Note for anyone tempted by
   `File::try_clone`: that is `dup`, which yields a new descriptor onto the
   *same* `struct file` and fixes nothing. Each handle must be its own `open`.
3. **The remaining read work is no longer about file handles.** At 24 handles
   the curve reaches 83% of this machine's measured ceiling, so what is left
   inside `Engine::get` is the two allocations per read (the pread buffer and
   `Record::decode`'s value) and the `Arc<SegmentMap>` clone in `locate`.
   Those are the next things to measure, not more fds.
4. Pull M11.5 forward in full — `impl Absorb<KvOp>`, `published_index` with
   `Release`, `ReadHandleFactory` in the tonic service, compaction rerouted
   through `Relocate` ops. Note §1.15's named correctness trap: ReadIndex must
   then wait on `published_index`, **not** `last_applied`, or it serves stale
   reads that appear only under concurrency.
5. Return to M8 (snapshots) and leave the read path at M11.5 where
   `DESIGN.md` put it.

## Constraints any continuation must respect

- **Strict TDD.** Failing test first, confirm it fails for the expected reason,
  then implement. Not optional in this repo.
- **Test placement.** No inline tests, no `mod tests;` tail on source files.
  Tests live in `src/tests/<name>.rs`, declared in `src/tests/mod.rs`, reached
  by absolute `crate::` paths — never `use super::*`.
- **Purity boundary.** No tokio, tonic, filesystem, async, or clock reads in
  `kv-raft`. CI has a guard; M4's determinism depends on it.
- **The sweep gate.** Re-run the 100k nemesis sweep before claiming any
  `kv-raft` or `kv-sim` change is safe:
  `cargo nextest run --release -p kv-sim --run-ignored all -E 'test(sweep_full)'`
  (~11 min release). `cargo fmt --all -- --check` and
  `cargo clippy --workspace --all-targets -- -D warnings` must be clean.
- **Do not commit or push** unless the user asks.

## Change 5 — per-segment handle fan-out, built then reverted

Briefly, `SegmentMap` held several `Arc<File>` per segment so concurrent
readers would not share a kernel `struct file`, picked by a per-thread slot.
Measured +8% at four handles, +23% at twenty-four (1→20 scaling 7.2× → 9.3×,
against a machine ceiling of 11.2×), with a useful shape: **the contention does
not degrade gracefully** — 1, 4 and 8 handles were within run-to-run spread of
each other, and only ~one handle per concurrent reader separated from the pack.

**Reverted**, because it solved the problem in the wrong unit. Tying a handle
to a thread assumes one in-flight read per thread, which is the blocking model.
See change 6. The numbers are kept here because they still apply to the pread
fallback if that ever becomes the common path.

Also learned: the isolated pread diagnostic predicted 3.0× → 8.2× and the real
path delivered far less, because `Engine::get` is not only a syscall — it also
does a hash lookup, an `Arc<SegmentMap>` clone, and **two allocations per read**
(the pread buffer and the `Vec` from `Record::decode`). Under 20 threads those
contend in the allocator. That is where the remaining distance to the ceiling
lives, and it is the next thing to measure.

## Change 6 — reads go through io_uring, with a real fallback

`crates/kv-node/src/read_engine.rs`. One thread owns an `IoUring`, takes
resolved reads over a bounded channel, submits them, and reaps completions.
**Many reads in flight per thread**, which is the model Seastar/ScyllaDB,
TigerBeetle and modern Postgres use — concurrency bounded by the ring, not by
thread count. No `spawn_blocking` handoff on this path, so the warm-read
trade-off that `answer_read` used to make blindly no longer exists.

`kv-storage` gained the accessors an external I/O engine needs — `ValueRef::
raw_fd/offset/len/decode`, alongside the existing `read()`. Decoding (and so
the CRC check) stays in `kv-storage` so the two read paths cannot disagree
about what a record means.

- `a_located_value_can_be_read_through_its_raw_descriptor` — RED: `no method named len/raw_fd/offset/decode`
- `the_fallback_answers_many_overlapping_reads`, `io_uring_answers_many_overlapping_reads`
- `the_io_uring_path_can_be_disabled_by_configuration` — RED: `no function from_setting`
- `an_unknown_read_engine_setting_is_rejected`

### The production constraints this is built around

- **`io_uring` is never assumed.** Docker's default seccomp profile blocks the
  syscalls, hardened hosts set `io_uring_disabled`, and Google disabled it
  fleet-wide after a run of CVEs. Ring construction is a runtime probe;
  `BlockingEngine` is a first-class path, not a panic branch.
- **`BOHIME_READ_ENGINE`** = `io_uring` | `blocking` | `auto`. An operator can
  turn the ring off in response to a kernel advisory without shipping a new
  binary. An unrecognised value warns and falls back to `auto` rather than
  refusing to start — a typo in a performance knob must not become an outage.
  Asking for `io_uring` where none exists warns and degrades, for the same
  reason: serving reads slowly beats not serving them.
- **CI gates both paths.** A second `cargo nextest run --workspace` runs with
  `BOHIME_READ_ENGINE=blocking`, because which path a hosted runner takes is
  not ours to decide, and an untested fallback is not a fallback. **Both
  suites pass: 217 tests each.**
- **Correctness details worth not breaking.** The `ValueRef` is held until the
  read *completes*, not until it is submitted — it keeps the segment map alive,
  which is what holds the descriptor open under an in-flight SQE. Short reads
  are resubmitted for the remainder (`io_uring` may return them exactly as
  `read(2)` may). `EINTR` retries. A read that fails drops its sender rather
  than answering `None`, because `None` is indistinguishable from a missing key.

### Known gaps in change 6

- **Registered files are not used yet.** `IORING_REGISTER_FILES` +
  `IOSQE_FIXED_FILE` would bypass `fget`/`fput` per submission, which is the
  contention change 5 worked around. Not done because the registration table
  must track segment rotation and compaction, and getting that lifecycle wrong
  is worse than the contention. Plain fds are correct today; this is the next
  optimisation, and it is what makes the fan-out permanently unnecessary.
- **Not benchmarked.** `read_scaling.rs` measures `Engine::get`, which is the
  pread path. There is no bench covering the ring, and the claim that it beats
  the blocking path on this workload is so far reasoning, not measurement.
- **One ring per driver.** Fine now (one driver per node). At M11 a node runs a
  driver per shard and they must share one ring rather than open one each.
- **`kv-node` is now Linux-only** at compile time, not just at runtime — the
  `io-uring` crate does not build elsewhere. Consistent with a project whose
  storage layer already requires `std::os::unix` and whose CI is Linux, but it
  is a harder constraint than before. Gate the dependency behind
  `[target.'cfg(target_os = "linux")'.dependencies]` if that ever matters.

