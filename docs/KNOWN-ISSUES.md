# Known issues

Things that are wrong, missing, or unverified, that we chose not to fix at the
moment we found them. Each entry says how it was found, why it is still here,
and what fixing it looks like — so that picking one up does not start with
re-deriving the problem.

`docs/DESIGN.md` is the design bible and `CLAUDE.md` describes the code as it
is; neither is a defect list. This file is. Delete an entry when it is fixed,
rather than annotating it.

Last reviewed: 2026-09-18, at `e6a0c74` plus the uncommitted tick-loop work.

---

## 1. Write-path congestion collapse — cause unknown

**Open, and three hypotheses deep.** Full account in
`docs/superpowers/plans/2026-09-18-congestion.md`.

On tmpfs, with no `fdatasync` anywhere, write throughput falls by about half
for every doubling of the client count above 64. At 64 shards: **5170 → 3019 →
1665 → 784 op/s** at 64 → 128 → 256 → 512 clients. p50 stays well behaved
(10.8 → 70.6 ms) while p99 runs from 38.6 ms to 5.89 s, so what is lost is a
**starved tail**, not uniform slowdown.

This entry previously blamed the client's handling of `ResourceExhausted`.
That was wrong, and so were the two hypotheses after it. Ruled out by
measurement:

- **Shedding.** A client-side counter shows `shed = 0` at 32, 128 and 256
  clients. The 256-deep request channel never fills, because the driver drains
  it straight into `Group::pending`, which is unbounded — so the bound is a
  buffer in front of an unbounded queue and is not admission control at all.
- **The client's 2 s deadline.** Raised to 15 s: 79 op/s vs 77, p99 24.9 s vs
  28.3 s. No change.
- **Expiry manufacturing duplicate proposals.** Real, fixed, and worth nothing
  in throughput (1561/726/355/227 → 1533/686/321/226 at 8 shards, inside this
  box's spread).

Two genuine amplifiers were found and fixed along the way — the client no
longer redials a node that was merely slow or busy, and a leader that is still
committing no longer tells clients it is not the leader. Both are pinned by
tests. **Neither moves the curve.**

Next step is in the plan doc, and it starts with ruling out the load generator
itself: `drive()` spawns one tokio task per client in a single runtime beside
three `kv-node` processes on one box, latency is measured client-side, and
this benchmark has published wrong numbers three times before (§7).
`BOHIME_BENCH_DIR=/tmp` reproduces the whole curve in ~30 s.

## 2. `benches/engine.rs` does not measure fsync

`crates/kv-storage/benches/engine.rs` compares `Never`, `EveryWrite` and
`GroupCommit`, and two things make its numbers meaningless on this box:

- It opens the engine under `tempfile::tempdir()`, which is `/tmp`, which is
  **tmpfs** here. `fdatasync` on tmpfs is close to free, so the arms measure
  the same thing. `benches/read_scaling.rs:21` already knows this and says so
  in a comment; `engine.rs` predates that knowledge and was never revisited.
- The timed closure contains `drop(engine); drop(dir);` — segment file close
  and directory teardown are inside the measurement.

Anything that has ever been concluded from this benchmark should be treated as
unmeasured. `benches/cluster.rs` prints the filesystem it is running on before
its first number for exactly this reason, and `engine.rs` should do the same.

## 3. `read_view.rs` has had its safety net deliberately removed

`crates/kv-node/src/read_view.rs` — the resolver does **not** re-check the
published-index invariant and answer `NotLeader` when it fails. It logs
`tracing::error!` and answers anyway.

This is on purpose and it is a real trade. The re-check was a correct safety
net, and it made M11.5's stale-read bug invisible to
`tests::wait_free_reads::a_get_after_a_put_returns_never_misses_it`: the test
passed with the bug in, because the net converted a stale read into a
redirect. Removing it gave the test teeth.

The cost is that a violation now returns a possibly-stale value to a client
instead of a redirect. That is the right call while the test is the thing
protecting us and the wrong call in production. Worth a second opinion, and
worth revisiting if the invariant ever fires in a real run.

## 4. The 100k-seed nemesis sweep is four milestones overdue

It last passed at **`6c704b9` (M7)**, in 863 s in release. M8 and M9 changed
`kv-raft`'s message shapes and its commit path. M10, M11 and M11.5 did not
touch `kv-raft` or `kv-sim` at all, and neither did the throughput or
tick-loop work — so the exposure is M8 and M9 only, but it is real.

```
cargo nextest run --release -p kv-sim --run-ignored all -E 'test(sweep_full)'
```

~15 min in release (~80 min in debug, which is why it is a nightly job and not
on the push path). Any change to `kv-raft` or `kv-sim` obliges re-running it
before the change can be called safe.

## 5. The nemesis cannot fault snapshots, and cannot until `invariants.rs` learns `first_index`

`kv-sim` has no reference to snapshots: nothing calls `take_snapshot`, and the
nemesis has no compaction fault. So M8's entire mechanism — the part that
truncates a log prefix — has never been exercised under partition, loss and
restart, which is the only place its bugs live.

Adding the fault is blocked on the invariant suite, not on the nemesis.
`invariants.rs`'s `log_matching` and `leader_completeness` treat a compacted
prefix as divergence and would report **false** violations. They need to be
taught about `first_index` first. That is the actual unit of work here, and it
is the higher-value half.

## 6. DESIGN §1.15 names one correctness trap; M11.5 hit three

§1.15 is otherwise accurate — the `Absorb` contract, the `ReadHandleFactory`
detail, the "publish per batch", the `arc-swap`-for-the-shard-map call are all
exactly what landed. But it presents the published-vs-applied index as *the*
trap, and two others turned out to be just as load-bearing:

- **The writer cannot read its own unpublished writes.** `WriteHandle` derefs
  to the *published* copy, and the apply loop is read-modify-write (`Cas`,
  `session::cached`, the address book), so entry N+1 would not see entry N.
  §1.15 frames staleness purely as a reader problem; the writer's half is
  harder and needed an overlay of keys touched since the last publish.
- **A `ValueLoc` is meaningless without the segment map it was recorded in**,
  and compaction reuses retired segment ids. Keydir and segment map therefore
  have to publish *together* inside one `KeyDir`. §1.15's "Compaction becomes a
  second writer" paragraph anticipates the relocation race and not this one;
  a reader on the outgoing copy consulting the current segment map read an old
  offset out of the merged file.

Not urgent — the code is right and the traps are recorded in `CLAUDE.md`. But
§1.15 is what a reader of this project is pointed at, and it currently
under-sells the problem. Revising the design bible is the owner's call, which
is why this is a note here rather than an edit there.

## 7. Measurement debts

- **256 shards has never been benchmarked.** The M11 tick-loop guard runs at
  256 in-process, and `main` raises `RLIMIT_NOFILE` for the 512 Bitcask
  instances it implies, but no throughput number exists above 64.
- **Any write benchmark against a real disk on this box is device-bound from
  8 shards up.** The device does 593–742 fsync/s measured; 8 shards at 228
  writes/s across 3 nodes is already 684. Conclusions about write *scaling*
  need `BOHIME_BENCH_DIR=/tmp` as a control arm, and that arm is not
  measuring durability.
- **Every `get` number published before 2026-09-18 was a 100% miss rate.**
  `benches/cluster.rs` seeded both arms from one RNG, but the write arm drew a
  256-byte value before each key and the read arm did not — strides of 33 and
  1 through the same stream, so the read arm asked for keys that were never
  written. Fixed, with an assertion on a miss. The M11.5 wait-free read path
  had therefore never been exercised end to end by a benchmark until then.
  Treat any `get` figure in a plan doc dated before 2026-09-18 as void.

## 8. Smaller things

- **`CLAUDE.md:237` is stale**: it says group commit is not implemented on the
  Raft log. `a62cc7c` implemented it. `CLAUDE.md` is gitignored
  (`.gitignore:8`), so this cannot be fixed by a commit — it has to be fixed in
  each working copy, which is why it is recorded here.
- **AppendEntries pipelining is still absent**: `next_index` only advances on
  an ack, so the leader is stop-and-wait per peer. This one is *planned* —
  roadmap M12.5 — and is listed here only so that it is not rediscovered as a
  surprise.
- **The branch is named `m6-single-shard-node`** and contains M0 through
  M11.5. The name predates M7.
- **`AdminService::Rebalance` and `VerifyShard` answer `unimplemented`** until
  M12, and there is no admin CLI — membership changes go through gRPC
  directly.
