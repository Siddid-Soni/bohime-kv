# Bohime — a sharded, Raft-replicated key-value store in Rust over gRPC

## Context

You want a portfolio-grade distributed systems project in Rust that a reviewer
can look at and immediately believe you understand replication, consensus, and
partitioning. You already know consistent hashing and Merkle trees; you want
Raft and whatever else is needed explained.

The decided shape (from your answers):

- **Multi-Raft sharded** — consistent hashing maps keys to shards, each shard is
  an independent Raft group of 3 replicas. This is the TiKV/CockroachDB design.
- **Raft implemented from scratch** — election, replication, persistence,
  snapshots, membership change. This is the differentiator on the resume.
- **Own Bitcask-style storage engine** — append-only segments, in-memory keydir,
  compaction. A second independent resume bullet.

Project directory: `/home/siddid/Projects/bohime`.

Outcome: a cluster you can start with one command, that survives killing a node
mid-write, that passes a linearizability check under fault injection, and that
has a README a hiring manager can skim in 90 seconds.

---

# Part 1 — Concepts

Ordered so each builds on the last. Concepts you already have (consistent
hashing, Merkle trees) get a short note on *where they attach*, not a tutorial.

## 1.1 The replicated state machine model

This is the frame everything else sits in. A **state machine** is a
deterministic function `(state, command) -> (state, response)`. If you have N
copies of the same state machine and you feed them the *exact same sequence of
commands*, every copy ends in the exact same state. That's the whole trick.

So distributed consistency reduces to one problem: **make every replica agree on
an identical, totally-ordered log of commands.** That agreement problem is
called **consensus**. Raft is a consensus algorithm.

For us: the state machine is the KV map, the commands are `Put(k,v)`,
`Delete(k)`, `CAS(k, old, new)`. Determinism is a hard requirement — no
timestamps generated inside the state machine, no HashMap iteration order
affecting results, no randomness.

## 1.2 Consistency models (what you're promising the caller)

- **Linearizable** (a.k.a. strong / atomic consistency). Every operation appears
  to take effect instantaneously at some single point between its invocation and
  its response, and that order is consistent with real time. If a write completes
  at 10:00:00.000, every read starting after that must see it. **This is what
  we're building.**
- **Sequential** — all replicas see the same order, but that order need not match
  real time. Weaker; a read can go back in time.
- **Eventual** — replicas converge if writes stop. Dynamo/Cassandra default.
  Cheap, but the client has to handle conflicting versions.

Why it matters for the resume: saying "linearizable" is a strong, checkable
claim. We will actually check it (M13, linearizability checker under faults),
which is rare in portfolio projects and is the thing worth talking about in an
interview.

## 1.3 CAP and PACELC, stated honestly

**CAP:** when the network partitions (P), you must choose between staying
available (A) and staying consistent (C). Raft chooses **CP**: a minority
partition refuses to serve writes rather than diverge.

**PACELC** is the more useful version: *if Partition then A-or-C, Else
Latency-or-Consistency*. Even with a healthy network, linearizable reads cost a
round trip. That "else" branch is why we implement ReadIndex and lease reads
(§1.10) — it's the knob between latency and correctness in the normal case.

## 1.4 Quorums

A **quorum** is any majority: `floor(N/2) + 1`. With N=3 that's 2; with N=5 it's
3. The property that makes consensus work: **any two majorities of the same set
intersect in at least one member.** So if a value was accepted by a majority,
any future majority contains at least one node that knows about it.

Consequence: a 3-node group tolerates 1 failure, a 5-node group tolerates 2.
Even-sized groups are pointless (N=4 also tolerates only 1) — always odd.

## 1.5 Raft — the core

Raft's design goal was *understandability*, and it gets there by decomposing
consensus into three near-independent pieces: **leader election**, **log
replication**, and **safety**. Read the extended paper (Ongaro & Ousterhout,
"In Search of an Understandable Consensus Algorithm") — figure 2 is the whole
algorithm on one page and our implementation should map onto it line by line.

### Terms

Time is divided into **terms**, numbered with monotonically increasing integers.
A term is a logical clock: it begins with an election, and has **at most one
leader**. If an election splits the vote, the term ends leaderless and a new one
starts.

Every RPC carries the sender's term. Two universal rules, applied *before* any
other message handling:

1. If a message's term > my term: adopt it, and immediately become a follower.
2. If a message's term < my term: reject it, and reply with my term.

Almost every Raft bug comes from getting these two rules wrong or applying them
in the wrong order. In our code they will live in one function that all message
handling routes through.

### Roles

Every node is exactly one of:

- **Follower** — passive. Responds to leaders and candidates. If it hears
  nothing for `election_timeout`, it becomes a candidate.
- **Candidate** — trying to get elected.
- **Leader** — handles all client requests, replicates the log, sends periodic
  heartbeats.

### Leader election

A follower whose election timer fires: increments its term, becomes candidate,
votes for itself, and sends `RequestVote` to everyone. A node grants its vote if
(a) the candidate's term is at least its own, (b) it hasn't already voted in this
term, and (c) the candidate's log is **at least as up to date** as its own
(§ the election restriction, below).

Outcomes: majority of votes → leader; hears from a legitimate leader → step down
to follower; timeout with no winner → new term, try again.

**Split votes** are prevented statistically by **randomized election timeouts** —
each node picks a fresh random value in e.g. `[150ms, 300ms]` every time it
resets. This is the one place randomness is essential; it must be injectable so
tests can be deterministic.

A new leader's first act is to send empty `AppendEntries` (heartbeats) to assert
authority and suppress other elections.

### Log replication

The log is an array of entries, each `{ term, index, command }`. Index is 1-based
and contiguous. On a client write the leader appends locally, then sends
`AppendEntries` to followers with:

- `prev_log_index` / `prev_log_term` — the entry immediately *before* the ones
  being sent
- `entries[]`
- `leader_commit`

A follower **rejects** the request if it has no entry at `prev_log_index` with
`prev_log_term`. This is the **Log Matching Property**: if two logs contain an
entry with the same index and term, then the logs are identical in all entries up
to that index. It's an induction, and it's what lets the leader repair a divergent
follower by backing up one step at a time.

The leader keeps `next_index[peer]` (optimistically `last_log_index + 1`) and
`match_index[peer]` (0). On rejection it decrements `next_index` and retries.
Naive decrement is O(log length) round trips — the standard fix, which we will
implement, is for the rejection to carry a **conflict hint** (`conflict_term`,
`conflict_index`) so the leader can skip a whole term in one step.

### Commitment

An entry is **committed** once it is stored on a majority. The leader advances
`commit_index` to the highest N such that a majority of `match_index >= N` **and
`log[N].term == current_term`**.

That last clause is subtle and is Raft's most famous gotcha (paper figure 8): a
leader must **never** commit an entry from a *previous* term by counting replicas
alone. Such an entry can still be overwritten. It only becomes committed
indirectly, once an entry from the *current* term commits on top of it. This is
why a new leader immediately appends a **no-op entry** in its own term — it makes
everything before it committable and lets reads proceed. We will do that.

Committed entries are then **applied** to the state machine in index order;
`last_applied` trails `commit_index`. Only after applying does the leader reply
to the client.

### Safety: the election restriction

The **Leader Completeness Property**: any entry committed in a given term is
present in the log of every leader of every later term.

Raft achieves this without any log-repair-at-election by restricting *who can
win*: a voter refuses its vote unless the candidate's log is at least as
up-to-date as its own, where "more up to date" means **higher last term, or the
same last term and a longer log**. Since committed means "on a majority", and
winning means "votes from a majority", and two majorities intersect, the winner
necessarily has every committed entry.

### What must be on disk before you reply

Three pieces of state must be **fsynced before responding to any RPC**:

1. `current_term`
2. `voted_for`
3. the log entries themselves

Skipping the fsync is how real systems lose data: a node that forgets it voted
can vote twice in one term and elect two leaders. This is a place where our test
suite (M1.6, M4) will explicitly inject crashes.

## 1.6 Log compaction and snapshots

The log grows without bound. **Snapshotting** replaces a prefix with a serialized
copy of the state machine at a point in time, plus `last_included_index` and
`last_included_term` (needed so `AppendEntries` consistency checks still work at
the boundary).

If a follower falls so far behind that the leader has already discarded the
entries it needs, the leader sends **InstallSnapshot** instead — shipping the
state directly, chunked. Our Bitcask engine makes the snapshot cheap: it is
essentially a compacted segment file, streamed over gRPC.

## 1.7 Membership changes

Adding or removing a node cannot be done by editing a config file, because during
the switchover two different majorities can exist simultaneously and elect two
leaders. Raft offers two solutions:

- **Joint consensus** — a transitional configuration `C_old,new` requiring
  majorities in *both* old and new sets. General, handles arbitrary changes.
- **Single-server change** — add or remove exactly one node at a time. Any
  old majority and any new majority necessarily overlap, so no joint phase is
  needed. Much simpler.

We implement **single-server change**, with the config stored *as a log entry*
and applied as soon as it is appended (not when committed — this is per the
paper and is a common source of confusion; note it in the code).

Also needed: a **learner / non-voting** state, so a new node can catch up on the
log before it counts toward quorum. Adding a cold node straight to voting status
can stall the group.

## 1.8 Client interaction and exactly-once semantics

A client sends `Put(k,v)`, the leader commits it, then crashes before replying.
The client retries. Without protection the write applies twice — harmless for
`Put`, fatal for `Increment` or `CAS`.

Fix: every client has a unique `client_id`; every request carries a monotonic
`sequence_number`. The state machine keeps a **session table** of
`client_id -> (last_seq, last_response)` and, on a duplicate, returns the cached
response instead of re-applying. Because the session table lives *inside* the
state machine, it is replicated and survives leader failover.

Clients also need **leader discovery**: any node that isn't leader replies with
a `NotLeader { leader_hint }` so the client can redirect and cache the hint.

## 1.9 Failure detection

Raft folds failure detection into heartbeats: `heartbeat_interval` (~50ms) must
be comfortably less than `election_timeout` (~150-300ms), which must be well
under mean-time-between-failures. Getting this ratio wrong causes spurious
elections under load — a good thing to demonstrate awareness of in a README.

## 1.10 Linearizable reads without writing to the log

The naive way to serve a read linearizably is to push it through the log as a
command. Correct, but expensive — a disk write and a round trip for a read.

Two better options, both of which we implement:

- **ReadIndex** — the leader records the current `commit_index`, confirms it is
  still leader by exchanging one round of heartbeats with a majority, waits until
  `last_applied >= that index`, then reads locally. Costs a network round trip
  but no disk write.
- **Lease read** — if the leader received heartbeat acks from a majority at time
  T, it may assume leadership until `T + election_timeout - clock_drift_bound`
  and read purely locally, zero round trips. Faster, but its correctness rests on
  bounded clock drift. We will implement it as an *opt-in flag*, documented
  honestly as such. Knowing *why* it's opt-in is exactly the kind of thing an
  interviewer probes.

There is also **follower read** via ReadIndex forwarded to the leader — useful
for read scaling, and a natural stretch goal.

## 1.11 Sharding, and where your ring hashing attaches

One Raft group's throughput is capped by one leader's disk and NIC. To scale,
**partition** the keyspace into shards, each with its own Raft group. This is
**multi-Raft**.

Your consistent hashing goes here, with one adjustment worth knowing: production
systems don't hash keys directly onto nodes. They hash keys onto a **fixed number
of shards** (we'll use 256), and then maintain an explicit, versioned
`shard -> [replica nodes]` map. Reason: with direct key→node hashing, adding a
node silently moves data and you have no way to represent "this shard is
currently mid-migration." An explicit map gives you a place to put that state.

Virtual nodes (~128-256 per physical node) still matter, for balancing shards
across nodes of unequal capacity.

**Where does the shard map itself live?** It's cluster metadata that must be
consistent — so it lives in its own small Raft group (call it the **meta group**,
analogous to TiKV's Placement Driver). This is a satisfying moment in the
project: you use your own Raft implementation as a library, twice, for two
different state machines.

**Cross-shard operations** are the hard part and we explicitly **do not** do
them. No multi-key transactions, no 2PC/Percolator. Single-key linearizable
operations only. Scoping this out is a *good* engineering decision and the README
should say so deliberately rather than silently omitting it.

## 1.12 Rebalancing, and where your Merkle trees attach

When a node joins or leaves, shards must move. The migration is a sequence of
Raft membership changes on the affected shard: add the new node as a learner,
let it catch up via InstallSnapshot + log tail, promote it to voter, remove the
old node. No custom data-movement protocol needed — the snapshot machinery
already does it. Rate-limit it so migrations don't starve foreground traffic.

**Merkle trees** attach as an **anti-entropy verifier**. In a Raft system,
replicas should be identical by construction, so a Merkle diff should always come
back empty — which makes it an excellent *bug detector*. Each replica builds a
tree over its keyspace at a given applied index; a background job compares root
hashes across the group, descends only into mismatching subtrees, and alarms
(and can repair) on divergence. This catches storage-engine corruption, a
non-deterministic state machine, and fsync bugs — exactly the failures Raft
itself cannot see. That's a much more interesting use of a Merkle tree than the
usual Dynamo repair loop, and it's a better interview story.

## 1.13 Storage engine: Bitcask

A **log-structured** design. All writes append to an active segment file; an
in-memory **keydir** maps `key -> (file_id, offset, size, timestamp)`. Reads are
exactly one seek. Nothing is ever updated in place.

Record format: `crc32 | timestamp | key_len | value_len | key | value`. The CRC
is what lets recovery detect a torn write at the tail of the log — on open, scan
forward, stop at the first bad CRC, truncate.

When the active segment exceeds a threshold it is rotated to immutable and a new
one opened. **Compaction** merges immutable segments, keeping only live values,
and writes a **hint file** (keys + offsets, no values) so restart doesn't have to
re-read every value to rebuild the keydir.

Tradeoffs to state in the README: O(1) reads, sequential writes, crash-safe by
construction; but all keys must fit in memory, and range scans are not supported
(the keydir is a hash map). Knowing the limits of your own design is the point.

Durability knob: `fsync` per write (safe, slow) vs. per batch vs. on rotate. We
make it configurable and **default to fsync-per-batch with group commit** —
batching concurrent writes into one fsync is a standard and very legible
optimization.

## 1.14 gRPC / protobuf

**Protobuf** is the schema-first binary wire format; **gRPC** is the RPC layer
over HTTP/2. What you get for free and should be able to explain: HTTP/2
multiplexing (many in-flight RPCs on one TCP connection, no head-of-line blocking
at the request level), bidirectional streaming, and a schema that generates
strongly-typed Rust on both sides.

In Rust: **`prost`** compiles `.proto` → Rust types, **`tonic`** builds the
client/server on top of `tokio` + `hyper`. Compilation happens in a `build.rs`.

Three services in our design:

- `KvService` — the client-facing API (`Get`, `Put`, `Delete`, `Cas`, `Scan`)
- `RaftService` — node-to-node (`RequestVote`, `AppendEntries`,
  `InstallSnapshot` as a client-streaming RPC for chunking)
- `AdminService` — `AddNode`, `RemoveNode`, `ClusterStatus`, `Rebalance`,
  `VerifyShard`

Practical concerns we'll handle: **deadlines** on every RPC (a Raft RPC that
outlives its election timeout is worse than useless — cancel it), **connection
pooling** per peer, **backpressure** (bounded channels; do not let an unbounded
queue of AppendEntries build up against a slow follower), and **pipelining**
(don't wait for one AppendEntries to be acked before sending the next; track
`match_index` from acks instead — this is a real throughput multiplier and worth
benchmarking before/after).

## 1.15 Wait-free reads with `left-right`

`left-right` (jonhoo, v0.11.8) is a concurrency primitive for **one writer, many
readers**. It keeps *two* copies of a structure. Readers always read one copy
with no locking — only an epoch counter increment — so reads are **wait-free**
and scale linearly with cores. The writer mutates the other copy, then
`publish()` swaps the pointers and waits for readers to drain off the now-stale
copy before replaying the same operations onto it, so the two converge.

The contract it demands:

1. **Exactly one writer.** More than one requires an external `Mutex`.
2. **An operational log** — you describe changes as values of an op type `O`,
   not as direct mutation.
3. **Ops applied twice, deterministically** — once per copy.

**Raft supplies all three for free**, which is why this fits unusually well:

| `left-right` requires | Raft already guarantees |
|---|---|
| One writer | The shard's apply loop — structurally single-writer |
| A deterministic oplog | Committed log entries; determinism is already required (§1.1) |
| Replayable twice | `absorb_first` / `absorb_second` replay the same `KvOp` |

The API:

```rust
trait Absorb<O> {
    fn absorb_first(&mut self, operation: &mut O, other: &Self);
    fn absorb_second(&mut self, operation: O, other: &Self);
    fn sync_with(&mut self, first: &Self);
}
let (w, r) = left_right::new::<KeyDir, KvOp>();
w.append(op); w.publish();          // writer
let guard = r.enter();              // reader: Option<ReadGuard<KeyDir>>
```

Note `absorb_first` takes `&mut O` and `absorb_second` takes `O` by value —
write the logic once and call it from both.

**Target in this project: the per-shard Bitcask keydir + session table.** Writer
is the Raft apply loop; readers are every tonic `Get` handler task. This replaces
`RwLock<HashMap>` on the hottest in-memory structure in the system.

**Tonic integration detail:** `ReadHandle` is `!Sync`, so it cannot be shared
across tasks. `ReadHandle::factory()` returns a `ReadHandleFactory` that **is**
`Send + Sync` — store *that* in the gRPC service struct and mint a per-task
handle from it. Without this the design does not compile, and it's the first
thing to get right.

### The correctness trap — this one matters

Readers see state as of the last **`publish()`**, not the last `absorb`. Our
ReadIndex logic (§1.10) waits for `last_applied >= read_index`. With left-right
**that becomes wrong**: an entry can be applied to the write copy and not yet
visible to readers. It must wait for `last_published >= read_index`.

Fix: an `AtomicU64 published_index`, stored with `Release` ordering *after*
`publish()` returns, which ReadIndex waits on instead. Getting this backwards
produces a stale read that violates linearizability, appears only under
concurrency, and passes every single-threaded test. It gets a named regression
test in M11.5.

### Consequences to design around

- **Compaction becomes a second writer.** Bitcask compaction rewrites keydir
  offsets, which left-right forbids. Compaction must instead route
  `KvOp::Relocate { key, old_loc, new_loc }` through the same apply loop,
  applied conditionally (skip if `old_loc` no longer matches — the key was
  overwritten in the interim). **This changes M1.5**, so the keydir must sit
  behind a trait from M1 even though the implementation isn't swapped until M11.5.
- **Publish per batch, not per entry.** Each `publish()` costs a swap plus a
  wait for readers to drain. Raft's `commit_index` already advances in batches,
  so `append()` the whole batch and `publish()` once. The batching is free.
- **Memory doubles.** The keydir is ~40 bytes/key, so 10M keys goes 400MB →
  800MB. Bitcask already requires all keys resident, so this compounds. Make
  the index implementation a config flag.
- **A slow reader stalls the writer** at `publish()`. Our reads are single
  hashmap lookups, so this is fine here — but it is the reason left-right is
  wrong for workloads with long-lived read guards.

### Where *not* to use it

- **`kv-raft` core** — no. It is a single-threaded pure state machine by design;
  adding concurrency there breaks the purity boundary that M4's deterministic
  simulation depends on.
- **The shard map** — use **`arc-swap`** instead. It is 256 entries, replaced
  wholesale on rebalance, and has no natural oplog. `ArcSwap<ShardMap>` is three
  lines and strictly better here. left-right earns its complexity only when the
  structure is too large to clone per write. Choosing the simpler tool where it
  wins is worth stating explicitly in the README.

## 1.16 Testing distributed systems

This is the section that will most distinguish the project.

- **Deterministic simulation testing (DST)** — run the whole cluster in one
  thread, with a virtual clock and a simulated network you control. Because
  everything is deterministic and seeded, a failure is reproducible from its
  seed. This is FoundationDB's and TigerBeetle's approach. It requires the Raft
  core to be a **pure state machine**: `handle(msg) -> Vec<Action>`, no I/O, no
  clock reads, no async. Designing for this from M3 is why the plan orders things
  the way it does.
- **Fault injection** — drop, delay, duplicate, and reorder messages; partition
  the network into arbitrary groups; crash and restart nodes (losing everything
  not fsynced).
- **Property-based testing** (`proptest`) over the pure core: generate random
  message sequences, assert Raft's invariants hold — election safety (≤1 leader
  per term), log matching, leader completeness, state machine safety.
- **Linearizability checking** — record a concurrent history of
  invocations/responses and search for a valid sequential ordering (the
  Wing & Gong / Knossos / Porcupine algorithm). Under fault injection this is the
  strongest correctness evidence a project like this can produce.
- **Jepsen** is the well-known external tool for this class of testing; we won't
  run it (it's Clojure and a heavy lift), but the README should name it as prior
  art and describe what our in-house checker does instead.

---

# Part 2 — Architecture

```
                    ┌──────────────────────────────┐
   client ────────► │  kv-client (smart client)    │
                    │  shard map cache, leader      │
                    │  hints, retry w/ backoff      │
                    └───────────┬──────────────────┘
                                │ gRPC KvService
      ┌─────────────────────────┼─────────────────────────┐
      ▼                         ▼                         ▼
 ┌─────────┐              ┌─────────┐              ┌─────────┐
 │ node A  │              │ node B  │              │ node C  │
 │         │◄── gRPC RaftService (AppendEntries) ─►│         │
 │ shard 0 │              │ shard 0 │              │ shard 0 │
 │ (leader)│              │(follow.)│              │(follow.)│
 │ shard 1 │              │ shard 1 │              │ shard 1 │
 │(follow.)│              │ (leader)│              │(follow.)│
 │ meta    │              │ meta    │              │ meta    │
 └─────────┘              └─────────┘              └─────────┘
   each shard = own Raft group, own log, own Bitcask engine
   meta group = replicated shard map (ring → shard → replicas)
```

Crate layout — the boundaries matter, because they are what make each piece
testable in isolation:

```
bohime/
├── Cargo.toml                 # workspace
├── proto/                     # .proto files (single source of truth)
├── crates/
│   ├── kv-proto/              # prost/tonic codegen only
│   ├── kv-storage/            # Bitcask engine. No knowledge of Raft.
│   │                          # KeyDirIndex trait: HashMap or left-right impl
│   ├── kv-raft/               # PURE Raft state machine. No I/O, no tokio,
│   │                          # no clock, no network. This is the core.
│   ├── kv-ring/               # consistent hashing + shard map types
│   ├── kv-node/               # the impure shell: tokio, tonic, disk,
│   │                          # timers, multi-shard router
│   ├── kv-client/             # smart client library + CLI
│   └── kv-sim/                # deterministic simulator + fault injection
├── tests/                     # cross-crate integration
└── benches/                   # criterion
```

The load-bearing design decision: **`kv-raft` has no dependencies on tokio,
tonic, or the filesystem.** Its API is roughly

```rust
impl RaftNode {
    fn tick(&mut self, now: LogicalTime) -> Vec<Action>;
    fn step(&mut self, msg: Message) -> Vec<Action>;
    fn propose(&mut self, cmd: Vec<u8>) -> Result<LogIndex, ProposeError>;
    fn ready(&mut self) -> Ready;   // entries to persist, msgs to send,
}                                   // entries to apply, snapshot to install
```

`Action`/`Ready` describes *what should happen* — "persist these entries",
"send this message to peer 3", "apply up to index 47". `kv-node` executes them
against real I/O; `kv-sim` executes them against a virtual clock and a lossy
in-memory network. Same code under test, two worlds. This one boundary is what
makes M4's deterministic testing possible at all, and it's the single best
architectural talking point in the project.

---

# Part 3 — Micro-problems and their tests

Each milestone is independently completable and independently demoable. Follow
TDD throughout (the `superpowers:test-driven-development` skill): write the
failing test first, watch it fail, then implement. Commit at every ✅.

### M0 — Environment and workspace  *(~1 session)*
- Create workspace, seven crates, `proto/` dir, git init.
- `rust-toolchain.toml` pinning stable; `rustfmt.toml`; `clippy.toml`.
- Deps added via `cargo add` so versions resolve current (do **not**
  hand-write versions): `tokio`, `tonic`, `prost`, `tonic-build`, `bytes`,
  `serde`, `bincode`, `thiserror`, `anyhow`, `tracing`,
  `tracing-subscriber`, `crc32fast`, `rand`, `clap`, `left-right`, `arc-swap`;
  dev-deps `proptest`, `tempfile`, `criterion`, `assert_cmd`, `loom`
  (`loom` for concurrency-model-checking the left-right integration in M11.5).
- `cargo install cargo-nextest` (parallel test runner; also
  `cargo-insta` for snapshot tests, `cargo-flamegraph` for later profiling).
- GitHub Actions: fmt + clippy `-D warnings` + nextest on push.
- ✅ **Test:** `cargo nextest run` passes with one trivial test per crate;
  CI is green on the first push.

### M1 — Bitcask storage engine  *(~2-3 sessions)*
- **M1.1** Record codec: encode/decode `crc32|ts|klen|vlen|key|value`.
  ✅ roundtrip proptest over arbitrary bytes; corrupting any single byte is
  detected by the CRC.
- **M1.2** `Engine::open/put/get/delete` against a single append-only file.
  ✅ put-then-get; overwrite returns latest; delete returns `None`;
  proptest against a `BTreeMap` model oracle.
- **M1.3** Keydir rebuild on reopen by replaying the log.
  **Put the keydir behind a `KeyDirIndex` trait here** (`get`, `insert`,
  `remove`, `relocate`, `iter`) with a `HashMap` impl. Costs nothing now and is
  the seam left-right swaps into at M11.5 — retrofitting it later touches every
  read path.
  ✅ write 10k keys, drop, reopen, all readable.
- **M1.4** Segment rotation at a size threshold.
  ✅ crossing the threshold creates a new file; reads still resolve across
  segments.
- **M1.5** Compaction + hint files. Compaction **must not mutate the keydir
  directly** — it emits `Relocate { key, old_loc, new_loc }` records applied by
  the index's single writer, skipped when `old_loc` no longer matches. This
  keeps the single-writer property left-right will require at M11.5.
  ✅ 10k overwrites of 100 keys compacts to ~100 live records; data unchanged;
  reopen-from-hint matches reopen-from-scan.
  ✅ A key overwritten *during* compaction keeps the new value, not the
  relocated old one.
- **M1.6** Crash safety: truncate the file mid-record, reopen.
  ✅ engine recovers to the last intact record and accepts new writes;
  test every truncation offset in the final record.
- **M1.7** Configurable fsync policy + group commit.
  ✅ benchmark all three policies with criterion; record the numbers in the
  README.

### M2 — Raft persistent state  *(~1 session)*
- `RaftStorage` trait: `save_hard_state`, `append`, `entries(lo,hi)`,
  `truncate_suffix(from)`, `term(idx)`, `snapshot`.
- Two impls: in-memory (tests) and Bitcask-backed (production).
- ✅ Same trait-conformance suite runs against both impls;
  `truncate_suffix` then `append` leaves no ghost entries;
  hard state survives reopen.

### M3 — Raft core, pure state machine  *(~4-6 sessions — the heart)*
- **M3.1** Types: `Term`, `LogIndex`, `NodeId`, `Message`, `Action`, `Ready`,
  `Config`. No behavior yet.
- **M3.2** Role state machine + `tick()`; timers as counters, not clocks.
  ✅ follower with no heartbeat becomes candidate after `election_timeout`
  ticks; one that keeps receiving heartbeats never does.
- **M3.3** `RequestVote` — grant/deny rules, term rules, vote persistence.
  ✅ candidate with majority becomes leader; never votes twice in one term;
  higher term always causes step-down.
- **M3.4** `AppendEntries` — consistency check, conflict truncation, conflict
  hint on rejection.
  ✅ divergent-follower repair converges (table-driven from paper figure 7);
  the conflict hint converges in ≤2 round trips where naive decrement takes N.
- **M3.5** Commit index advancement + apply.
  ✅ commits at majority `match_index`; **explicitly refuses to commit a
  prior-term entry by count alone** (figure-8 scenario as a named test);
  new leader appends a no-op.
- **M3.6** Election restriction.
  ✅ a node with a stale log cannot win against an up-to-date voter;
  proptest asserting Leader Completeness over random histories.
- ✅ **Milestone test:** invariant suite runnable after *any* step —
  election safety, log matching, leader completeness, state machine safety.

### M4 — Deterministic simulator  *(~2-3 sessions)*
- Virtual clock, seeded RNG, in-memory network with configurable
  drop/delay/duplicate/reorder rates and arbitrary partitions.
- Node crash (lose all non-fsynced state) and restart.
- ✅ 3-node cluster elects exactly one leader within N ticks, over 10k seeds.
- ✅ Under 20% loss and random partitions, invariants never break across
  100k seeded runs; a failing seed reproduces byte-identically.
- ✅ Nemesis test: kill the leader mid-replication, verify no committed entry
  is ever lost.

### M5 — gRPC transport  *(~1-2 sessions)*
- `proto/raft.proto`; `RaftService` server + a per-peer pooled client with
  deadlines, bounded outbound queues, and reconnect-with-backoff.
- ✅ Two processes exchange RequestVote/AppendEntries over real gRPC;
  a killed peer produces backoff, not a busy loop; a full queue sheds load
  instead of growing unboundedly.

### M6 — Single-shard node end to end  *(~2 sessions)*
- `kv-node` binary: Bitcask + Raft core + tonic server + a driver loop that
  ticks, drains `Ready`, persists, sends, applies.
- `proto/kv.proto`; `KvService::{Get,Put,Delete}`; `NotLeader{leader_hint}`.
- ✅ **The first real demo.** Three processes, `put k v` on the leader,
  `get k` on any node returns it; `kill -9` the leader, a new one is elected
  within ~1s and the data is still there.

### M7 — Linearizable reads + client sessions  *(~2 sessions)*
- ReadIndex; lease read behind a flag; session table inside the state machine.
- ✅ ReadIndex never returns stale data under a partition where a deposed
  leader still thinks it leads (this test *fails* against a naive local read —
  demonstrate that, then fix it).
- ✅ A retried `Cas` applies exactly once.

### M8 — Snapshots and compaction  *(~2 sessions)*
- Snapshot at an applied index; log truncation; `InstallSnapshot` as a
  client-streaming RPC with chunking.
- ✅ A node down long enough for the leader to compact past its `next_index`
  catches up via snapshot and rejoins;
  restart-from-snapshot equals restart-from-full-log.

### M9 — Membership change  *(~2 sessions)*
- Learner state, catch-up, promotion; single-server add/remove as a log entry.
- ✅ Grow 3 → 5 under continuous writes with no unavailability window and no
  lost writes; shrink 5 → 3; removing the leader triggers a clean handoff.

### M10 — Ring hashing + meta group  *(~2 sessions)*
- `kv-ring`: 256 shards, virtual nodes, `shard_map: shard -> [replicas]`,
  versioned. Meta group = a second Raft instance whose state machine is the
  shard map.
- Publish the map to request handlers via `ArcSwap<ShardMap>` — read on every
  request, written only on rebalance. Deliberately *not* left-right (§1.15).
- ✅ Key→shard is stable across restarts; adding a node moves ≈`1/N` of
  shards and no more; the shard map is itself linearizable.

### M11 — Multi-Raft  *(~2-3 sessions)*
- Shard router on each node; one shared tick loop and thread pool across
  shard groups (not one thread per shard); per-shard Bitcask instances.
- ✅ 5 nodes × 3 replicas × 8 shards: writes to different shards land on
  different leaders and proceed concurrently; killing a node degrades only
  the shards it led, and only briefly.

### M11.5 — Wait-free reads via `left-right`  *(~2 sessions)*
Deliberately placed here: before M11 there is no read concurrency to measure, so
doing it earlier would be optimizing blind.
- `impl Absorb<KvOp> for KeyDir`; swap the `KeyDirIndex` impl from M1.3.
- Apply loop becomes: `append()` every op in the committed batch → one
  `publish()` → store `published_index` with `Release`.
- ReadIndex switches from waiting on `last_applied` to waiting on
  `published_index` (§1.15).
- `ReadHandleFactory` in the tonic service struct; per-task `ReadHandle`.
- Keep the `RwLock<HashMap>` impl behind a config flag as the comparison arm.
- ✅ **Stale-read regression test:** a `Get` issued after a `Put` returns must
  never miss it. Write it against the *naive* `last_applied` wait first, watch it
  fail, then fix — this is the whole point of the milestone and the story to
  tell about it.
- ✅ `absorb_first` and `absorb_second` leave both copies byte-identical after
  an arbitrary proptest-generated op sequence (`sync_with` correctness).
- ✅ Compaction `Relocate` ops flow through the single writer; `loom` test over
  a small interleaving of apply + read + relocate.
- ✅ Criterion: read throughput vs. core count (1→20) for `RwLock<HashMap>`,
  `DashMap`, and left-right. **The scaling graph goes in the README** — it is
  the most legible performance artifact in the project.
- ✅ Full M4 simulation suite and the M7 linearizability tests still pass
  unchanged.

### M12 — Rebalancing + Merkle verification  *(~2-3 sessions)*
- Migration driver: learner-add → catch-up → promote → remove old, one shard
  at a time, rate-limited.
- Merkle tree over each shard's keyspace at an applied index;
  `AdminService::VerifyShard` compares roots and descends on mismatch.
- ✅ Add a node to a live 3-node cluster under load: data rebalances, no
  request fails, Merkle roots agree across every replica of every shard
  afterwards.
- ✅ Deliberately corrupt one replica's segment file → verification detects
  it and names the divergent key range.

### M13 — Proof, polish, and the resume artifact  *(~2-3 sessions)*
- Linearizability checker over recorded histories; run it against the real
  cluster under the nemesis.
- Criterion benchmarks: throughput/latency vs. shard count, vs. fsync policy,
  with and without AppendEntries pipelining.
- `kv-client` CLI + `docker-compose` (or a shell script) for a 5-node cluster.
- README: architecture diagram, the benchmark table, **what is explicitly not
  supported and why** (no cross-shard transactions, no range scans, lease
  reads assume bounded clock drift), and the seeded-simulation story.
- ✅ `./scripts/demo.sh` brings up 5 nodes, drives load, kills nodes at
  random, and prints a passing linearizability verdict.

**Realistic total: 27-37 focused sessions.** M0-M6 alone (roughly a third) is
already a legitimately impressive project; M7-M9 make it credible; M10-M13 make
it distinctive. The milestone boundaries are deliberately placed so that stopping
after any of M6, M9, or M13 yields something coherent to show.

---

# Part 4 — Environment setup (executed at M0)

Everything needed is already installed: `cargo`/`rustc` 1.98.0, `protoc` 35.1,
`git`, `docker`, 20 cores. Steps:

1. `mkdir -p /home/siddid/Projects/bohime && git init`
2. Workspace `Cargo.toml` with `members = ["crates/*"]`, a shared
   `[workspace.dependencies]` block, and `resolver = "3"`.
3. Seven crates via `cargo new --lib` (plus `--bin` for `kv-node` and
   `kv-client`).
4. `rust-toolchain.toml` (channel = "stable", components = rustfmt, clippy).
5. `proto/{raft,kv,admin}.proto` skeletons + `kv-proto/build.rs` invoking
   `tonic_build`, with `.protoc_arg` set so codegen is reproducible.
6. `cargo add` every dependency listed in M0 — versions resolved at install
   time, then committed via `Cargo.lock`.
7. `cargo install cargo-nextest cargo-insta`.
8. `.github/workflows/ci.yml`: fmt, `clippy -D warnings`, `nextest run`.
9. `.gitignore`, `README.md` stub, `LICENSE` (MIT).
10. Initial commit.

---

# Part 5 — Verification

Per milestone, the ✅ items above are the gate — a milestone is done when its
tests pass under `cargo nextest run`, not when the code compiles.

Whole-system verification at M13:

```bash
cargo nextest run --workspace          # unit + integration
cargo test --release -p kv-sim -- --ignored   # 100k-seed simulation sweep
./scripts/demo.sh                      # 5-node cluster + nemesis + linz check
cargo bench                            # criterion, numbers go in the README
```

The demo script is the artifact you show people: it starts a cluster, hammers it
with concurrent writes, randomly kills and restarts nodes and partitions the
network, then asserts that the recorded history is linearizable and that every
Merkle root agrees.

---

# Part 6 — Reading list

- Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm*
  (extended version) — figure 2 is the spec; figures 7 and 8 are the tests.
- Ongaro's PhD thesis, ch. 4 (membership) and ch. 6 (client interaction) — the
  parts the paper compresses.
- *Bitcask: A Log-Structured Hash Table for Fast Key/Value Data* (Riak, 2010).
- DeCandia et al., *Dynamo* — for the leaderless contrast and Merkle
  anti-entropy.
- Kleppmann, *Designing Data-Intensive Applications*, ch. 5, 6, 9.
- TiKV's multi-raft design docs — the closest production analogue to this design.
- Kyle Kingsbury's Jepsen analyses — how these systems actually break.
- `jonhoo/left-right` README + docs.rs, and Jon Gjengset's stream series on
  building it — the clearest available explanation of the primitive and of
  `evmap`, the concurrent map built on top of it.
