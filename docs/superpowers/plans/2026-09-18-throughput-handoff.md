# Handoff — write throughput, and what it takes to benchmark honestly (2026-09-18)

Written for whoever picks up the work after M11.5. It is **not** a milestone:
it is the thing that has to happen before the M13 benchmark table means
anything, and before any comparison against etcd or TiKV is worth running.

## The question that produced this

"Can we now benchmark against similar key-value databases, or are we still
behind that?"

The answer was: you can produce numbers today, but they would mostly measure
three things already known to be missing, in a predictable direction. This
handoff is the ordered fix.

## State

`M0-M11.5` are done and committed — `7858930` (code) and `e137e82` (plans) on
`m6-single-shard-node`. `CLAUDE.md` was brought up to date with them in the
same session that wrote this; it is accurate as of now and is the fastest way
into the architecture.

Verified at that commit: 396 tests pass on both read engines, `cargo fmt
--check` and `cargo clippy --workspace --all-targets -D warnings` clean, M4's
10k-seed election gate in 0.93s, M11.5's loom gate under `--cfg loom`.

## What is actually missing, measured or read from the code

**1. Group commit is not implemented on the Raft log. This is the big one.**

`BitcaskStorage::append` (`crates/kv-node/src/storage.rs:143-163`) does one
`Engine::put` per entry and then one `put_meta`. The Raft log's engine runs
`FsyncPolicy::EveryWrite`, so **N entries cost N+1 fsyncs**. M7's proposal
batching gets several client writes into one `append` call, which amortizes
the metadata sync but not the per-entry one.

`docs/DESIGN.md:343` names "batching concurrent writes into one fsync" as the
plan's actual answer to many concurrent writers. `FsyncPolicy::GroupCommit`
already exists in `kv-storage/src/config.rs` and is simply not used for the
log. The driver's `Group::drain` already calls `storage().sync()` once per
drain, before anything leaves the process (§1.5, disk before network) — and
the comment at `storage.rs:109` says outright that this sync is redundant
under `EveryWrite` and exists so that switching to `GroupCommit` cannot
silently drop the ordering guarantee. The seam is already there.

**The safety argument you must not break:** a vote must be durable before it
is granted on the wire, and an entry must be durable before it is counted
toward a commit. Group commit is safe only because `drain` syncs before it
sends. Any change that lets a message leave before the sync is an election
safety bug, and M4's nemesis sweep is the thing that would catch it — see the
debt below about the sweep not having run since M7.

**2. AppendEntries pipelining is not implemented.**

`next_index` advances only when a response arrives
(`crates/kv-raft/src/node.rs:877`), and `send_append` always sends from
`next_index` — so the leader is effectively stop-and-wait per peer, one batch
in flight, retried on the next heartbeat. `docs/DESIGN.md:370` calls
pipelining "a real throughput multiplier". This one lives in `kv-raft`, which
means the nemesis sweep gates it.

**3. There is no load generator and no cluster benchmark.**

`crates/kv-storage/benches/` holds `engine.rs`, `read_scaling.rs` and
`index_scaling.rs` — all storage micro-benchmarks. `benches/cluster.rs` and
`scripts/demo.sh` are M13 deliverables that do not exist. Nothing today
measures end-to-end throughput or latency against a running cluster.

**4. The read path is capped below the keydir, and M11.5 did not change it.**

~6.4M reads/s on one shared `struct file` per segment, measured. That ceiling
sits *below* every keydir arm M11.5 benchmarked, so a whole-read graph would
be flat for all three index implementations. Fixing it means per-reader file
handles — dup'd fds, or `IORING_REGISTER_FILES`, which `read_engine.rs` does
not currently use. Worth its own milestone; not this one.

## Benchmarking traps on this machine, already paid for once

1. **`/tmp` is tmpfs.** Any bench using `tempfile::tempdir()` measures memory
   and its fsync is a no-op. `crates/kv-storage/benches/engine.rs`'s
   fsync-policy arm does exactly this, so **its `every_write` number is not
   measuring fsync** — a wrong number already committed. `read_scaling.rs`
   uses `CARGO_TARGET_TMPDIR`; copy that.
2. **`/home` is btrfs with `compress=zstd:3`.** Benchmark values must be
   pseudo-random or cold numbers are fiction.
3. **20 logical cores, 14 heterogeneous physical (6 P + HT, 8 E).** 20× never
   existed; a syscall-free control arm tops out at 11.2×. Threads are not
   pinned and the governor ramps with load, so 1-thread and 20-thread points
   run at different clocks. Run-to-run spread was ~±7%.
4. **Always include a control arm.** M11.5's benchmark reported left-right at
   8.5× a lock on *one thread*, which no lock explains; an unsynchronized
   control showed the arm was timing 200k misses, because a dropped
   `WriteHandle` makes `enter()` return `None` forever. The control is what
   caught it. A benchmark without one is a number you will end up defending.

## The order the work should go in

**Step 1 — measure Bohime against itself, before changing anything.**
Throughput and latency vs. shard count (1 → 8 → 64), at fixed durability, from
a real multi-process cluster. This answers the question M11 asserted but never
measured: *does sharding multiply write throughput?* The M11 gate proves writes
to different shards proceed concurrently on different leaders; it does not
prove aggregate throughput scales. If that curve is flat, it is much more
important than anything else in this document, and it is better to find out
before the fix than after.

This also gives every later step a before-number that was taken on the same
box, on the same day, with the same governor behaviour.

**Step 2 — group commit on the Raft log.** The largest lever, the design
already specifies it, and the seam already exists. Expect the write path to go
from ~1 fsync per entry to ~1 per drain. Keep the disk-before-network ordering
and say in the commit message why it is still safe.

**Step 3 — re-measure step 1.** Same harness, same box. The delta is the
number worth publishing.

**Step 4 — one honest external comparison.** etcd, same box, same durability
setting, same client concurrency, single-shard first. Not to win: to find out
whether the architecture is in the right order of magnitude. If it is 10× off
after group commit, that is worth knowing before M12 and M13 spend sessions on
polish.

Pipelining (2) is deliberately *not* in this list. It lives in `kv-raft` and
therefore needs the nemesis sweep, which has not run since M7 — so it should
follow the sweep being brought back to green, not precede it.

## Constraints

- **TDD.** Failing test first, confirm the reason, then implement. A benchmark
  is not a test: correctness changes to the log still need tests.
- **Test placement.** No inline tests, no `mod tests;` tail. `src/tests/<name>.rs`
  declared in `src/tests/mod.rs`, absolute `crate::` paths, never `use super::*`.
- **Purity boundary.** `kv-raft`, `kv-sim`, `kv-ring` must never gain tokio;
  CI enforces it. `kv-storage` has no tokio and should not grow one — a
  cluster-level benchmark belongs in `kv-node`.
- **Both read engines.** CI runs the suite twice; keep the
  `BOHIME_READ_ENGINE=blocking` arm green.
- **Touching `kv-raft` or `kv-sim` obliges you to re-run the 100k nemesis
  sweep** (~15 min release). Group commit is a `kv-node`/`kv-storage` change
  and should not need it; pipelining is not.
- **Do not commit unless asked.** Do not edit `README.md` or `docs/DESIGN.md` —
  §1.15 is now wrong in three ways M11.5 documented, and revising the design
  bible is the user's call.
- **A green test suite is not evidence about performance or concurrency.** M11
  shipped 378 passing tests alongside a bug that left a tenth of its shards
  permanently leaderless on a real cluster. Measure.

## Debts carried forward, unchanged

1. **The 100k-seed nemesis sweep has not run since `6c704b9` (M7).** M8 and M9
   changed `kv-raft`'s message shapes and commit path. M10, M11 and M11.5 did
   not touch `kv-raft` or `kv-sim` at all.
2. **The sweep does not cover snapshots.** `kv-sim` never takes one, and
   `invariants.rs` would report *false* violations against a compacted prefix
   until `log_matching` and `leader_completeness` learn about `first_index`.
   That is the prerequisite for adding a compaction fault.
3. **`docs/DESIGN.md` §1.15 is wrong in three ways** M11.5 found and recorded;
   the README has no scaling graph yet, and if one is added it should be the
   *index* graph captioned as such, with the file-handle ceiling named beside
   it.
4. **The branch is still called `m6-single-shard-node`** and holds M7 through
   M11.5, including the milestone that removed the single shard.
