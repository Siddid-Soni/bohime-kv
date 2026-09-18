# The write-path collapse was the benchmark (2026-09-18)

Not a milestone. `docs/KNOWN-ISSUES.md` §1 claimed the collapse was the client
treating `ResourceExhausted` as a dead node. That diagnosis was wrong, and so
were the two that replaced it, and in the end **so was the premise**: there is
no congestion collapse. `benches/cluster.rs` was measuring itself.

## The resolution, which came last

The four hypotheses were chased in this order, and only the fourth survived:
shedding, the client's deadline, server-side expiry — then the harness.

**The arms shared one cluster.** `spawn_cluster` ran once per shard count, and
every client-count arm inside it wrote to the cluster the previous arms had
already filled. So each arm measured its own *position in the sweep*, not its
client count. Reversing the order reverses the curve:

| clients | forward sweep | reverse sweep | fresh cluster per arm |
|---|---|---|---|
| 64  | **5170** | 836  | 2992 |
| 128 | 3019     | 919  | 3525 |
| 256 | 1665     | 1199 | 3543 |
| 512 | **784**  | 2347 | 3474 |

The first arm is fastest either way, whichever client count it holds. Run
entirely alone, the 512-client arm gives 1739-2392 op/s against the 784 it
reported as the last arm of a sweep.

**And the arms did unequal work.** `BOHIME_BENCH_OPS` is ops *per client*, so
the 64-client arm wrote 2560 records and the 512-client arm 20480 — 0.51 s
against 8.56 s. The short arm never leaves its cold-start transient and the
long one amortises it away. A swept client count now holds `TOTAL_OPS`
constant.

With both fixed, 64 shards is **flat across an 8x range of concurrency** —
2992 / 3525 / 3543 / 3474 op/s — while p50 grows linearly with it, 14 -> 31 ->
59 -> 78 ms. Throughput plateaus, latency grows with the queue: a saturated
system behaving exactly as it should.

That is the fourth benchmark in this repository to publish a confident number
that was an artifact of how it was taken.

## What the collapse looked like before that

At 64 shards on **tmpfs — no `fdatasync` anywhere** — write throughput falls
by roughly half for every doubling of the client count, from 64 clients up:

| clients | 1 shard | 8 shards | 64 shards |
|---|---|---|---|
| 64  | 182 op/s | 1561 | **5170** |
| 128 | 133      | 726  | **3019** |
| 256 | 279      | 355  | **1665** |
| 512 | 305      | 227  | **784**  |

Closed loop, 3 `kv-node` processes, RF 3, 40 ops per client, 256 B values.
This is congestion collapse by the textbook definition — offered load rises,
delivered throughput falls — and **it has nothing to do with durability**. The
same shape on disk is slower but identical in form.

p50 is well behaved throughout (64 shards: 10.8 → 25.5 → 32.5 → 70.6 ms). p99
is not (38.6 ms → 5.89 s). So the lost throughput is a **starved tail**, not
uniform slowdown: most requests are served at close to the expected rate and a
minority wait tens of seconds.

None of it was real. The tail was later arms of a sweep running against a
cluster the earlier arms had loaded up.

## What was ruled out, and how

### 1. Shedding. `ResourceExhausted` essentially never fires.

`kv_service.rs:79` sheds a full request queue, and KNOWN-ISSUES §1 had the
client's one-arm-for-every-`Status` error handling turning that into a lost
channel and a lost leader cache. A counter on every client-side status, at 64
shards on disk:

| clients | shed | unavailable | other error | not-leader | dials |
|---|---|---|---|---|---|
| 32  | **0** | 0 | 0   | 852   | 96   |
| 128 | **0** | 0 | 0   | 3316  | 384  |
| 256 | **0** | 0 | 572 | 17094 | 1340 |

`shed = 0` everywhere. The 256-deep request channel is never full, because the
driver drains it immediately into `Group::pending`, which is **unbounded**. The
bound is a buffer in front of an unbounded queue, so it is not admission
control and it never fires. That alone disposes of the original diagnosis.

### 2. The client's deadline. Raising it 2 s → 15 s changes nothing.

`REQUEST_TIMEOUT` is 2 s, and at 256 clients on disk the queueing delay is
right at it — `256 clients / 130 op/s = 1.97 s` — which made Little's law look
like the whole story. It is not: with `BOHIME_BENCH_TIMEOUT_MS=15000`, 256
clients gives **79 op/s, p99 24.9 s**, against 77 op/s and p99 28.3 s at 2 s.
Refuted.

### 3. Expiry manufacturing duplicate proposals. Removing it changes nothing.

This one was worth the money even though it did not pay. `Group::
expire_stale_requests` answered `NotLeader` to any request that outlived
`request_timeout` (`tick × election_timeout × 6` = 1.2 s at the defaults). Its
own doc comment names the difficulty — it exists for the partitioned leader,
and "the client cannot tell that from slowness" — and until now neither could
the driver. So an overloaded leader told clients it was not the leader, with a
hint pointing back at itself. The client returned and **proposed the entry
again**; the session table deduplicates the *apply*, so the duplicate is
correct, but it still costs a log entry, a replication round and an fsync.
That is a textbook amplifier, and the not-leader-per-operation rate rising
from 0.43 to 1.11 between 128 and 256 clients is consistent with it.

It is fixed (below). It is worth **nothing** in throughput:

| clients, 8 shards | before | after |
|---|---|---|
| 64  | 1561 | 1533 |
| 128 | 726  | 686  |
| 256 | 355  | 321  |
| 512 | 227  | 226  |

Inside this box's run-to-run spread, which is wide: the `fdatasync` control
read 166, 592, 173 and 572 per second on four runs of the same command.

## What was fixed

Both are defects on their own terms, both are pinned by tests, and **neither
moves the throughput curve**. Said plainly so that nobody reads a win into it.

**The client no longer treats a busy node as a dead one.** It had one arm for
every `tonic::Status`, calling `forget(id)` — which drops the cached channel
*and* the per-shard leader cache. Two things now differ:

- The per-attempt deadline moved off `Endpoint::timeout` and onto the client's
  own clock. Tower's elapsed error arrives as `Code::Unknown` with the message
  "transport error", which is indistinguishable from a connection that really
  broke — so the layer below was destroying the one distinction the client
  needs to make.
- `Refusal::{Busy, Gone}` splits reachable-but-refusing from unreachable.
  `ResourceExhausted` and a blown deadline are `Busy`: keep the channel, keep
  the leader, back off. Everything else is `Gone`.

The amplification was measurable: 572 errors produced 572 extra dials at 256
clients, against 768 legitimate ones.

**`RETRY_PAUSE` now doubles on consecutive backpressure**, three times, to
400 ms. Only on backpressure — a round in which nothing was reachable keeps
the flat 50 ms, because a cluster that is down does not become less down if
the client waits longer, and backing off there would only delay saying so.
(An earlier draft added a 30 s wall-clock budget as well. It made the
256-client arm fail outright where it had previously completed, so it is out.)

**A leader that is still committing no longer expires requests it will
serve.** `expiry_decision(leading, since_commit, request_timeout)` separates
the two reasons a request outlives its deadline, using the one thing that
tells them apart: a partitioned leader's commit index cannot move. Redirect
when it has not moved for a whole `request_timeout`; otherwise leave the
request pending. Expiring it does not withdraw the entry already proposed — it
only asks the client for a second copy of it.

## What is still open

**Why a loaded cluster is slower than a fresh one.** The artifact is gone from
the client curve, but the effect underneath it is real and unexplained: the
same cluster serves later arms markedly worse than earlier ones. Candidates,
untested — the Raft logs and keydirs growing, `take_snapshot` scanning a
larger state machine on the shared driver loop (the reason `main` puts the
meta group on its own driver in the first place), and the session table
accumulating a `client_id` per `Client` the harness ever built. A benchmark
that ran a cluster for an hour would care; none of the numbers here do, now
that every arm starts fresh.

**Whether the shard curve has the same defect.** It should not —
`spawn_cluster` was always per shard count — but no one has checked whether
the *order* of shard counts matters, and that is one reversed run away.

`BOHIME_BENCH_DIR=/tmp` reproduces any of this in about 30 seconds, which
remains the single most useful thing this session produced: the disk was
hiding a 20-minute iteration loop behind a 3-minute one.

## Verification

- `cargo nextest run --workspace` — **413 passed, 2 skipped** (405 before: 5
  client tests, 3 expiry tests).
- `BOHIME_READ_ENGINE=blocking cargo nextest run --workspace` — same.
- `cargo fmt --all -- --check` clean; `cargo clippy --workspace --all-targets
  -- -D warnings` zero.
- `kv-raft`, `kv-sim` and `kv-ring` are untouched, so the overdue 100k nemesis
  sweep (KNOWN-ISSUES §4) is no more overdue than it was.

Mutation checks, run rather than assumed:

| revert | result |
|---|---|
| `forget(id)` on every `Refusal` | 4 of 5 backpressure tests fail: `a node that was merely slow got redialled` (3 dials, not 1); busy get and put redial 4 times; sustained backpressure redials. The negative control (`an_unreachable_node_is_still_forgotten`) still passes. |
| backoff back to a flat `RETRY_PAUSE` | `sustained backpressure did not back off: 11 requests in 600ms` |
| `expiry_decision` always `Redirect` | `a_leader_that_is_still_committing_lets_the_request_wait` fails; the other two still pass |
