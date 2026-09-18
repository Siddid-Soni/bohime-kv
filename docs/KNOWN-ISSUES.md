# Known issues

Things that are wrong, missing, or unverified, that we chose not to fix at the
moment we found them. Each entry says how it was found, why it is still here,
and what fixing it looks like — so that picking one up does not start with
re-deriving the problem.

`docs/DESIGN.md` is the design bible and `CLAUDE.md` describes the code as it
is; neither is a defect list. This file is. Delete an entry when it is fixed,
rather than annotating it.

Last reviewed: 2026-09-18, at `7a19b60` plus the uncommitted M12.1 work.

---

## 1. `benches/cluster.rs` measured itself (fixed, kept as a warning)

**Resolved 2026-09-18.** Full account in
`docs/superpowers/plans/2026-09-18-congestion.md`. Left here because it is the
fourth time a benchmark in this repository has published a confident number
that was an artifact of how it was taken, and because two sessions of work
were spent on the thing it appeared to show.

The apparent finding was congestion collapse: on tmpfs, with no `fdatasync`
anywhere, 64 shards fell **5170 → 3019 → 1665 → 784 op/s** across 64 → 128 →
256 → 512 clients. Offered load up, delivered throughput down, p99 from 38.6 ms
to 5.89 s. Three hypotheses were chased and refuted by measurement — shedding
(`shed = 0` throughout), the client's 2 s deadline (raised to 15 s, no change),
and server-side expiry manufacturing duplicate proposals (real, fixed, worth
nothing).

It was the harness, twice over:

- **The arms shared one cluster.** Each measured its position in the sweep,
  not its client count. Reversed, the curve reverses: 2347 → 1199 → 919 → 836
  at 512 → 256 → 128 → 64, first arm fastest either way.
- **The arms did unequal work.** Ops were *per client*, so the 64-client arm
  wrote 2560 records in 0.51 s and the 512-client arm 20480 in 8.56 s.

Both fixed — a cluster per arm, and `TOTAL_OPS` held constant when the client
count is swept. 64 shards is now flat across an 8× range of concurrency
(2992 / 3525 / 3543 / 3474 op/s) with p50 growing linearly (14 → 31 → 59 →
78 ms), which is a saturated system behaving properly.

**What is still open:** a loaded cluster really is slower than a fresh one —
that part was not an artifact, it was just not what the axis said. Nothing
measures it today because every arm now starts fresh. Candidates are in the
plan doc.

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
- **Four benchmark defects so far, three of them found in one week.**
  `benches/engine.rs` cannot measure fsync at all (§2); the M11.5 left-right
  arm timed 200k misses; `benches/cluster.rs`'s read arm ran at a 100% miss
  rate for its whole existence; and its client sweep measured arm ordering
  (§1). The pattern is not carelessness about code — it is that a benchmark
  has no failing state, so nothing tells you it is wrong. Every number in this
  repository should be read with "what is the control arm?" in hand.
- **Every `get` number published before 2026-09-18 was a 100% miss rate.**
  `benches/cluster.rs` seeded both arms from one RNG, but the write arm drew a
  256-byte value before each key and the read arm did not — strides of 33 and
  1 through the same stream, so the read arm asked for keys that were never
  written. Fixed, with an assertion on a miss. The M11.5 wait-free read path
  had therefore never been exercised end to end by a benchmark until then.
  Treat any `get` figure in a plan doc dated before 2026-09-18 as void.

## 8. A removed replica keeps its shard's data forever

**Found by M12.1's gate**, on its second run, and characterised exactly:
node 2 holding a departed shard with `replicas [1, 2, 3, 4], leader None`.

`kv-raft/src/node.rs:182`'s `replication_targets` is derived from the live
config, and `RemoveVoter` takes effect when the entry is **appended** — so a
leader stops replicating to the departing node strictly before the entry that
removes it goes out. The departing node is left believing it is still a voter,
with no leader and nobody to ask. It is not merely slow to find out; there is
no path by which it ever can.

M12.1's departure rule needs two witnesses before it deletes a shard's
directories: the group's own committed config must exclude this node, and the
published map must too. The second arrives routinely. The first cannot arrive
at all for a node that was *removed*, so a node that loses a replica slot in a
rebalance keeps that shard's Bitcask pair on disk indefinitely. Disk usage
after a rebalance only grows.

The mechanism is built and tested and does fire where the witness can arrive —
`tests::shards::a_shard_is_deleted_only_when_both_witnesses_agree`,
`tests::driver::{a_group_that_still_names_this_node_is_not_released,
an_adopted_group_that_was_never_contacted_is_reclaimed}`. What is missing is
the signal, and `tests::migrate::the_m12_1_gate` asserts the orphan is still
there so that fixing this makes the gate fail rather than silently pass.

Two ways to fix it, both bigger than M12.1 was:

- **An explicit tombstone message** from the leader to the node it removed —
  what TiKV does. A `kv-raft` message-shape change, which obliges re-running
  the 100k-seed nemesis sweep (§4).
- **A node-to-node control path**, so a replica whose map excludes a shard can
  ask that shard's leader whether the group still contains it. No `kv-raft`
  change, but a new cross-node admin call with leader discovery and retries,
  and the in-process harness has no gRPC to test it over.

## 9. A shard that never converges holds a migration slot indefinitely

`--max-migrations` (default 4) caps the shards one node moves at once by
taking the diverging ones in shard-id order. A shard whose learner never
catches up, or whose leadership keeps changing, stays diverging forever and
keeps consuming one of those slots, and nothing says so beyond the reconciler
re-proposing every pass. Not *wrong* — the old replicas keep serving and the
map stays published — but a rebalance can stall with no signal. A log line
naming the shard and how long it has diverged is the cheap fix; the real one
is M13's operational surface.

## 10. A rebalance has never been benchmarked

M12.1 moves shards by snapshot, and a shard group now takes a snapshot every
time it admits a member. Nothing measures what that costs a cluster under
load: not the pause on the group taking it, not the receiver's ingest, not the
effect on foreground latency, and not how `--max-migrations` trades those
against how long a rebalance takes. §7's warning applies in full — there is no
control arm here yet.

## 11. Smaller things

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
- **`AdminService::VerifyShard` answers `unimplemented`** until M12.2, and
  there is no admin CLI — membership changes go through gRPC directly.
  `Rebalance` is implemented since M12.1: it runs this node's migration pass
  now and answers with how many shards it leads that still differ from the
  map.
- **`CLAUDE.md` is stale about M12** as well as about group commit: it says
  shard migration is absent, that a `--join` node's shards never arrive, and
  that `Rebalance` is unimplemented. All three are wrong since M12.1. It is
  gitignored (`.gitignore:8`), so this cannot be fixed by a commit — it has to
  be fixed in each working copy.
