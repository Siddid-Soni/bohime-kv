# Write throughput — measure, fix, re-measure (2026-09-18)

Not a milestone. This is the work `2026-09-18-throughput-handoff.md` ordered:
measure Bohime's write path against itself before touching it, implement the
largest lever the design already names, and re-measure on the same box.

## The headline, first

**Sharding did not multiply write throughput. The curve was flat.**

Three real `kv-node` processes, RF 3, 32 concurrent clients, 256-byte
pseudo-random values, data directories on **btrfs on NVMe**
(`/home/siddid/Projects/bohime/target/tmp`, via `CARGO_TARGET_TMPDIR` —
explicitly *not* `tempfile::tempdir()`, which is tmpfs on this box):

| shards | before (writes/s) | p50 | p99 |
|--------|-------------------|-----|-----|
| 1  | 58 | 561 ms | 809 ms |
| 8  | 88 | 332 ms | 1012 ms |
| 64 | 82 | 411 ms | 575 ms |

58 → 88 → 82 is flat. M11's gate proves writes to different shards proceed
concurrently on different leaders; it does not and cannot prove aggregate
throughput scales, and it does not.

**The cause is not the architecture.** The same binary, the same sweep, the
only change being a data directory on tmpfs (`BOHIME_BENCH_DIR=/tmp`, where
`fdatasync` is a no-op):

| shards | tmpfs control (writes/s) |
|--------|--------------------------|
| 1  | 121  |
| 8  | 1068 |
| 64 | 1897 |

**15.7× over the same range.** So the multi-Raft design does scale with shard
count. What flattened it on disk is that every shard's log lands on the same
physical device, each write cost a fixed number of `fdatasync`s, and this box
does ~500 of those a second — the benchmark's own raw-`fdatasync` control arm
measured 437-647/s across runs. Shards multiply CPU and lock parallelism. They
cannot multiply a device's fsync budget. Only spending fewer fsyncs per write
can.

## After

Same box, same harness, same day. Two runs of the full sweep, ~5% apart:

| shards | before | after | ratio |
|--------|--------|-------|-------|
| 1  | 58 | 92 / 90   | 1.6× |
| 8  | 88 | 231 / 242 | 2.6× |
| 64 | 82 | 134 / 140 | 1.6× |

Latency at 8 shards went from p50 332 ms / p99 1012 ms to p50 141 ms / p99
214 ms.

The curve is no longer flat: 91 → 237 → 137. It rises to 8 shards and then
falls, which is a *second* bottleneck and not this one — see "What is now in
front".

Isolating the two levers at 8 shards, one arm at a time, same cluster shape:

| log fsync | state fsync | writes/s |
|-----------|-------------|----------|
| `every-write` | `every-write` | 89  |
| `group-commit` | `every-write` | 119 |
| `never` | `every-write` | 122 |
| `group-commit` | `never` | 286 |

Group commit on the log is worth **+34%**, and 119 against the 122 that
`never` gives means it captures essentially *all* of the log's available gain —
there is nothing left there. The state machine's own fsyncs were the larger
half, and they were not in the handoff at all.

## What landed

Two changes, both in `kv-node`/`kv-storage`. `kv-raft` and `kv-sim` were not
touched, so this adds nothing to the nemesis-sweep gap M8/M9 already carry.

### 1. `--log-fsync`, defaulting to `group-commit`

`BitcaskStorage::open_with_policy` opens the Raft log under a chosen
`FsyncPolicy` instead of the engine default. `NodeConfig::log_fsync` carries
it; `main.rs` and `shards.rs` pass it. `LogFsync` is the CLI's enum
(`every-write` | `group-commit` | `never`), separate from `FsyncPolicy` for
the same reason `KeydirImpl` is separate from `IndexKind` — `GroupCommit`'s
two parameters are not knobs an operator should have to reason about.

Appending N entries used to cost N+1 fsyncs (one per entry plus one for
`\x00log_meta`). It now costs one per drain. `tests::group_commit` asserts
both numbers directly, through `Engine::sync_count`.

`Group::drain`'s sync became **unconditional**. It used to be guarded on the
`Ready` having entries or a hard state, which was a free optimisation under
`EveryWrite` — every write had already synced on the way in, so the guard could
not skip anything that mattered. Under `GroupCommit` that guard becomes a
correctness question instead: `truncate_suffix`, `truncate_prefix` and
`save_snapshot` all dirty the log without putting anything in `ready.entries`,
and deciding which of them may be left unsynced is exactly the kind of case
analysis that breaks silently. `Engine::sync` is a no-op when nothing is
pending, so asking every time costs a branch.

The group-commit bound is deliberately loose (4096 records / 100 ms). The bound
that actually holds is the drain, so tight values here would only fire
*between* drains under light load and defeat the point.

### 2. `--state-fsync`, defaulting to `group-commit`

This was not in the handoff and is the larger half of the win.

`NodeConfig::engine_config` handed every state machine
`FsyncPolicy::EveryWrite` — directly beneath a comment saying a Bitcask holding
a *replicated* state machine "is durable through the Raft log rather than
through its own fsync". **The comment was right and the code disagreed with
it.** Every applied `Put` cost two extra fsyncs a node: the value, and its
session-table row.

Dropping them is safe because the state machine is a cache of the log — except
in exactly one place. A snapshot drops the log prefix that could rebuild the
state, and `Group::new` starts replay at the stored snapshot's index on the
stated assumption that "the state on disk already reflects the stored
snapshot". So `Group::maybe_snapshot` now fsyncs the state machine *before*
`take_snapshot` truncates. One fsync per snapshot buys back two per applied
write.

### Tests

`crates/kv-node/src/tests/group_commit.rs`, six tests, 402 in the workspace
(was 396):

- the two fsync-cost assertions above, and a reopen that proves a
  group-committed log reads back everything it synced;
- `a_vote_is_durable_before_it_is_granted_on_the_wire` and
  `an_entry_is_durable_before_it_is_replicated` — the ordering;
- `the_state_machine_is_fsynced_before_the_log_prefix_that_could_rebuild_it_is_dropped`.

The two ordering tests use a `PeerLink` that records the log's **own** fsync
counter at the instant of each send, so a zero means no fsync had happened —
not merely that nobody had called `sync`. That needed a handle on a counter
that outlives a move into `Group`, which is why `Engine::sync_count` became
public and `Engine::sync_counter` exists.

### `crates/kv-node/benches/cluster.rs`

Three `kv-node` processes, the real `kv-client` over real gRPC, closed-loop
load, put and get arms, throughput and p50/p99/max. `harness = false` rather
than criterion: the thing being measured is a load generator against a live
cluster, and criterion's model does not fit that.

It prints the filesystem it is about to write to before the first number, and
rebuilds `kv-node` in release itself for the same reason M6's gate does.
Knobs: `BOHIME_BENCH_SHARDS`, `BOHIME_BENCH_CLIENTS`, `BOHIME_BENCH_OPS`,
`BOHIME_BENCH_FSYNC`, `BOHIME_BENCH_DIR`.

## The safety argument for group commit

Raft needs two things on the disk before anything else happens:

1. A **vote** must be durable before it is granted on the wire. A node that
   voted, crashed before the vote reached the disk, came back and voted again
   in the same term would let two leaders be elected in one term.
2. An **entry** must be durable before it is counted toward a commit. A leader
   that counted its own unsynced entry toward a quorum, crashed, and came back
   without it would have committed an entry it no longer holds.

Neither is a statement about *when* the fsync happens. Both are statements
about the fsync happening **before the process externalises anything that
depends on it** — §1.5, disk before network. Group commit changes only the
first half. It does not change the second.

`Group::drain` is the single exit. `RaftNode::step`/`propose` are pure: they
write through to storage and stage messages in `Ready`, and nothing leaves the
process until `drain` runs. `drain` syncs (step 1), then answers the inbound
RPC (step 2), then sends to peers (step 3), then applies — which is where the
client reply and the read-view publish happen (step 4). So under `GroupCommit`
a write that is lost to a crash is a write no other node, and no client, was
ever told about. That is indistinguishable from the crash having happened one
instant earlier, which Raft already tolerates.

Under `EveryWrite` that ordering held **by accident**: every write synced on
the way in, so the drain's sync was a no-op and the ordering could not be
violated even if the code had got it wrong. Group commit removes the accident,
which is why the ordering is now pinned by a test rather than by a comment.
Deleting `self.node.storage().sync()?` from `drain` makes both ordering tests
fail with the RequestVote in the message.

The state machine's argument is different and simpler: it holds nothing the log
does not already hold durably, and a restart replays the log tail over it. Its
fsyncs buy no data — they only shorten replay. The one place that stops being
true is the snapshot, which is why `maybe_snapshot` syncs first.

## Things that came out differently from the handoff, CLAUDE.md or DESIGN.md

**The handoff named the wrong largest lever.** It called group commit on the
Raft log "the big one". Measured, the log is worth +34% and the state machine
is worth a further 2.4×. Both are the same underlying cause — fsyncs per write
— but the handoff's file-and-line inventory stopped at `storage.rs` and never
looked at `config.rs:122`.

**`NodeConfig::engine_config`'s comment contradicted its code.** It claimed the
state machine did not need its own fsync while configuring `EveryWrite`. The
comment is now the behaviour.

**`Group::drain`'s sync guard was load-bearing in a way its comment did not
say.** `storage.rs:109` said the sync existed so that "switching to
`GroupCommit` for throughput must not silently drop the ordering guarantee" —
correct — but the call site next to it was conditional on `ready.entries` or
`ready.hard_state`, which under group commit would have left
truncate/snapshot-dirtied state unsynced. The seam was there; it was not quite
finished.

**The existing 156 kv-node tests do not detect disk-before-network being
removed.** Verified by mutation: with `self.node.storage().sync()?` deleted
from `drain`, all 156 pre-existing tests pass, including M6's three-process
gate and the partition harness. Only the two new ordering tests fail. This is
the M11 lesson again ("a green test suite is not evidence about performance or
concurrency") in a new place: it was not evidence about durability ordering
either.

**`crates/kv-storage/benches/engine.rs`'s fsync arm is still wrong** and was
not touched here. It uses `tempfile::tempdir()`, so its `every_write` number is
measured on tmpfs against a no-op `fdatasync`. The number this session measured
on the same box with a real directory is ~500/s; whatever that bench reports is
not that.

**`--shards 256`, the default, was never benchmarked here.** The sweep stops at
64 because 64 already shows the shared-tick-loop ceiling (below), and 256 × 3
nodes takes long enough to found that the warmup dominates a short run.

## What is now in front

**The shared tick loop, not durability, is the next ceiling.** The `get` control
arm — which takes a ReadIndex quorum round trip and touches no disk — falls
14.4k → 8.9k → 2.2k op/s over 1 → 8 → 64 shards, and the write curve turns over
between 8 and 64 for the same reason. This is per-group cost in `Driver::run`,
and it is the same shape M11 already found once (reconciliation at O(groups) per
message). It is now the thing a shard sweep measures.

**The single-shard point is not fsync-bound any more and is still slow.** 91
writes/s at 1 shard with 32 clients, p50 342 ms, against a device doing ~500
fsync/s. One shard means one Raft group means one serialized log, and every
client's write waits behind every other's round trip. That is where
AppendEntries pipelining (`DESIGN.md:370`) would show up — deliberately out of
scope here, and still gated on the nemesis sweep returning to green.

**An external comparison is now worth running.** Step 4 of the handoff. Before
this change the answer would have been "Bohime is fsync-bound at 5 fsyncs a
write, which tells you nothing about the architecture"; now the numbers are the
architecture's.

## Verification

All five commands pass at the end of this session:

```
cargo nextest run --workspace                                    # 402 passed, 2 skipped
BOHIME_READ_ENGINE=blocking cargo nextest run --workspace        # 402 passed, 2 skipped
cargo fmt --all -- --check                                       # clean
cargo clippy --workspace --all-targets -- -D warnings            # clean
cargo nextest run -p kv-sim --run-ignored all -E 'test(election_sweep_full)'   # 1.73s
```

Nothing is committed. `kv-raft` and `kv-sim` are untouched, so the 100k-seed
nemesis sweep is no more overdue than it already was.

## Benchmarking notes for whoever runs this next

- `/tmp` is tmpfs. `BOHIME_BENCH_DIR=/tmp` is a **control**, never a result.
  The benchmark prints its filesystem before the first number so the mistake
  cannot be made silently.
- `/home` is btrfs `compress=zstd:3`; the benchmark's values are xorshift
  output for that reason.
- The raw `fdatasync` control varied 437-647/s across runs on an otherwise idle
  box; treat a single throughput number as ±10% at best, and the shard-curve
  *shape* as the thing worth reading.
- The 64-shard points have the widest spread, because founding 64 groups × 3
  nodes overlaps the warmup.
