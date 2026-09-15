# M4 — Deterministic simulator

**Status:** in progress. Expanded from the roadmap's M4 section, as that
section requires, now that M3 is complete and `Action`/`Ready` are final.

**Goal:** run whole clusters under adversarial networks, thousands of seeds at
a time, with any failure reproducible byte-for-byte from its seed alone.

---

## The one rule everything else serves

A run is a pure function of its seed. Every deviation from that is a bug in the
simulator, and the expensive property of such bugs is that they are *silent* —
the suite still passes, it just stops being evidence of anything.

Disqualifying, in rough order of how easily each sneaks in:

| Hazard | Why | Guard |
|---|---|---|
| `HashMap`/`HashSet` **iteration** | order is randomised per process | `BTreeMap`/`BTreeSet` everywhere in kv-sim *and* kv-raft |
| `SystemTime` / `Instant` | wall clock | virtual clock only; kv-raft already forbids clock reads |
| `rand::thread_rng` | unseeded | one `StdRng`, seeded, threaded through everything |
| `tokio` | scheduler nondeterminism | CI check that neither crate depends on it |
| float accumulation order | not associative | drop rates compared as integers where possible |

kv-raft was audited for this before starting: `next_index`/`match_index` are
already `BTreeMap`, and `votes_received` is a `HashSet` that is never iterated.
It becomes a `BTreeSet` anyway — the cost is zero and the trap it sets for a
future `for v in &self.votes_received` is not.

## Why the clock is virtual, not fast

Not for speed. A virtual clock makes "what happens at tick N" a total order the
seed fixes, so a 100k-seed sweep and a single replay of seed 41 take the same
code path. A real clock would make the sweep flaky rather than slow.

---

## Tasks

### 1. `clock.rs` — virtual time and the one RNG
`Clock { now: u64 }`, ticked explicitly. `SimRng` wraps one `StdRng`; every
random decision in the simulator draws from it. No component gets its own.

### 2. `network.rs` — the adversarial in-flight queue
In-flight messages in a `BTreeMap<(deliver_at, seq), Envelope>`: the `seq`
breaks ties deterministically where a `BTreeMap<u64, Vec<_>>` would force a
`Vec` ordering decision anyway. Per-message drop / delay / duplicate, plus
partitions as a node-group partition of the cluster. Reorder falls out of
per-message delay rather than needing its own mechanism.

### 3. `cluster.rs` — the driver
One `step()`: deliver due messages, `tick()` every node, drain each `Ready`,
persist to `MemStorage`, route outbound back into the network, apply committed
entries, then run `kv_raft::invariants::check_all`. Every step, no exceptions —
that is what M3.7 was built for.

### 4. Crash and restart
`crash(node)` must drop everything not in `RaftStorage`; `restart(node)`
rebuilds a `RaftNode` from persisted state alone. A node that comes back
remembering its in-memory state tests nothing.

`RaftNode` owns its storage (the M3.1 decision), so this needs
`RaftNode::into_storage(self) -> S` in kv-raft. That is the whole change:
crash = extract storage and drop the node, restart = `RaftNode::new(config,
storage)`.

### 5. `nemesis.rs` — randomised fault sequences
Partition / heal / crash / restart, drawn from the same seed.

### 6. Seed sweep
A `#[ignore]`d 100k-seed test plus a fast default subset, so `cargo nextest
run` stays quick and CI can opt into the long one.

---

## Gates

- A 3-node cluster elects exactly one leader within N ticks, over 10,000 seeds.
- Under 20% loss and random partitions, invariants never break across 100,000
  seeded runs.
- **A failing seed reproduces byte-identically.** Its own named test: run seed
  X twice, compare the full event trace. This is the foundation — if it does
  not hold, every other result in this milestone is unfalsifiable.
- Nemesis: kill the leader mid-replication; no committed entry is ever lost.

## Trace format

Comparing traces beats hashing them: a hash says "run 2 differed", a trace says
where. `Vec<TraceEvent>` with `TraceEvent` deriving `PartialEq` + `Debug`,
compared directly; the sweep can hash if volume demands it later.

## Expectation

The roadmap says to budget for M4 finding real bugs in M3, and that the sweep
should be expected to fail the first several times. A sweep that passes on its
first run is more likely to be miswired than correct — check that faults are
actually being injected before believing it.
