# M7 — Linearizable reads + client sessions

> **For agentic workers:** REQUIRED SUB-SKILL: `superpowers:executing-plans` or
> `superpowers:subagent-driven-development`. Steps use `- [ ]` checkboxes.

**Status:** planned, after M6 landed the first working cluster.

**Goal:** reads that are actually linearizable, and retries that do not
double-apply.

**Spec:** `docs/DESIGN.md` §1.8, §1.10; roadmap §M7.

---

## The gate dictates the order of work

> ✅ ReadIndex never returns stale data under a partition where a deposed
> leader still thinks it leads. **Write this test against the naive M6 local
> read first, watch it fail, then fix.** A test that has only ever passed
> proves nothing about the bug it claims to cover.

That is not a suggestion about rigour, it is a constraint on sequencing: the
test must exist and must **fail** before any ReadIndex code is written. So the
first task is not ReadIndex — it is the harness that can express "partition",
because nothing in the repo can today.

- `kv-sim` can partition, but it drives bare `RaftNode`s. It has no state
  machine and no `Get`, so it cannot observe a stale *read*.
- M6's end-to-end test drives real processes over loopback, which cannot be
  partitioned without root.

So M7 opens with an in-process cluster: real `Driver`s, real `RaftNode`s, real
Bitcask, wired to each other through channels instead of gRPC, with a
switchboard that can drop traffic between any two nodes. M9's membership tests
need the same thing, so this is not scaffolding built for one test.

## Global constraints

- `kv-raft` stays pure: no tokio, tonic, filesystem or clock reads. ReadIndex
  belongs there anyway — the quorum confirmation is a message exchange, so it
  is expressed as `Action`s like everything else.
- Test placement: `src/tests/<name>.rs`, declared in `src/tests/mod.rs`,
  reaching code by absolute `crate::` paths.
- TDD. `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets
  --all-features -- -D warnings`, `cargo nextest run --workspace`, plus both
  kv-sim sweeps, since tasks 3-4 change the Raft core.

---

## Task 1: a peer-link seam, so a cluster can live in one process

**Files:** modify `crates/kv-node/src/transport/mod.rs`,
`crates/kv-node/src/driver.rs`.

`Driver` holds `BTreeMap<NodeId, PeerClient>` — a concrete gRPC client. One
trait turns that into something a test can substitute.

```rust
/// How the driver reaches one peer. `PeerClient` is the real one; tests
/// substitute an in-memory link so a cluster can run in one process under a
/// partition that loopback sockets cannot express.
pub trait PeerLink: Send + Sync + 'static {
    fn try_send(&self, msg: Message) -> Result<(), SendError>;
}

impl PeerLink for PeerClient {
    fn try_send(&self, msg: Message) -> Result<(), SendError> {
        PeerClient::try_send(self, msg)
    }
}
```

`Driver::peers` becomes `BTreeMap<NodeId, Box<dyn PeerLink>>`. One dyn call per
outbound message, against a network round trip — not worth a generic parameter
that would infect every signature.

- [ ] **Step 1:** add the trait and the impl in `transport/mod.rs`.
- [ ] **Step 2:** change `Driver`'s field and `Driver::new`'s parameter.
- [ ] **Step 3:** `main.rs` boxes each `PeerClient`.
- [ ] **Step 4:** `cargo nextest run -p kv-node` — unchanged, all green.
- [ ] **Step 5:** commit: `M7: a peer-link seam so a cluster can run in-process`.

---

## Task 2: the in-process cluster harness

**Files:** create `crates/kv-node/src/tests/cluster.rs`; declare
`pub(crate) mod cluster;` in `crates/kv-node/src/tests/mod.rs` (the same way
kv-raft's `harness.rs` is declared).

The real transport is request/response: `PeerClient` sends a request and routes
the *reply* back into the driver's `peer_replies`. The in-memory link must
model that faithfully, or it tests a protocol the node does not speak. So a
`TestLink::try_send` does exactly what the gRPC path does:

1. build a `oneshot`,
2. push `Inbound { from: me, msg, reply }` into the target's inbox,
3. spawn a task that awaits the reply and pushes `(target, reply)` back into
   *my* `peer_replies`.

Both the request and the reply are checked against the partition table, because
a partition drops traffic in both directions.

```rust
/// Who can talk to whom. A partition is a set of one-way blocks; `partition`
/// installs both directions.
#[derive(Default)]
pub(crate) struct Switchboard {
    blocked: std::sync::Mutex<std::collections::BTreeSet<(NodeId, NodeId)>>,
}

impl Switchboard {
    pub(crate) fn allows(&self, from: NodeId, to: NodeId) -> bool {
        !self.blocked.lock().unwrap().contains(&(from, to))
    }

    /// Cuts `group` off from every node not in it, both directions.
    pub(crate) fn isolate(&self, group: &[NodeId], all: &[NodeId]) {
        let mut blocked = self.blocked.lock().unwrap();
        for &a in group {
            for &b in all {
                if !group.contains(&b) {
                    blocked.insert((a, b));
                    blocked.insert((b, a));
                }
            }
        }
    }

    pub(crate) fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
}
```

`Cluster::of_three()` builds three `NodeConfig`s over three tempdirs, spawns a
`Driver` per node with `TestLink`s for its peers, and exposes:

```rust
pub(crate) struct Cluster { /* … */ }
impl Cluster {
    pub(crate) async fn of_three() -> Cluster;
    /// Retries through NotLeader until some node accepts, then reports which.
    pub(crate) async fn put(&self, key: &[u8], value: &[u8]) -> NodeId;
    pub(crate) async fn get_from(&self, id: NodeId, key: &[u8]) -> ClientReply;
    pub(crate) async fn leader(&self) -> Option<NodeId>;
    pub(crate) fn switchboard(&self) -> &Arc<Switchboard>;
    /// Ticks pass in real time here, so waits are real waits. Deliberate:
    /// this harness exists to test the *node*, not the core — kv-sim already
    /// owns deterministic core testing, and duplicating its virtual clock here
    /// would mean two simulators to keep honest.
    pub(crate) async fn settle(&self, how_long: Duration);
}
```

- [ ] **Step 1:** write `cluster.rs`.
- [ ] **Step 2:** a smoke test — `of_three` elects exactly one leader, a put
      lands, every node reads it back. This must pass before it is trusted to
      report a failure.
- [ ] **Step 3:** a partition smoke test — isolate the leader, confirm the
      other two elect a new one within a few election timeouts. If this does
      not pass, the switchboard is not actually blocking and every later result
      is worthless. Check this before believing anything else.
- [ ] **Step 4:** commit: `M7: in-process cluster harness with a partitionable network`.

---

## Task 3: the failing test — a deposed leader serves a stale read

**Files:** create `crates/kv-node/src/tests/linearizability.rs`.

The roadmap puts this at `crates/kv-node/tests/linearizability.rs`; it goes in
`src/tests/` per the crate layout convention.

```rust
/// The M7 gate. This test must FAIL against M6's local read before ReadIndex
/// exists — that failure is the evidence the test is testing something.
///
/// Scenario: node A leads and holds k=v1. A is partitioned away. B and C elect
/// a new leader and write k=v2. A has heard nothing, so it still believes it
/// leads. A client that reaches A reads k.
///
/// M6 serves that read from A's local state and returns v1 — a value that was
/// overwritten before the read was issued. That is a linearizability
/// violation, not merely a stale cache: the write of v2 completed, and a
/// subsequent read returned v1.
#[tokio::test]
async fn a_deposed_leader_must_not_serve_a_stale_read() {
    let cluster = Cluster::of_three().await;
    let old_leader = cluster.put(b"k", b"v1").await;

    // Cut the leader off. The majority side keeps working.
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|n| *n != old_leader).collect();
    cluster.switchboard().isolate(&[old_leader], &[1, 2, 3]);

    // The two survivors elect a new leader and accept a write.
    cluster.settle(Duration::from_secs(2)).await;
    cluster.put_among(&others, b"k", b"v2").await;

    // The deposed leader still thinks it leads and has heard nothing.
    let reply = cluster.get_from(old_leader, b"k").await;

    match reply {
        // What M7 must produce: refuse, redirect, or return the current value
        // — anything but confidently serving the overwritten one.
        ClientReply::NotLeader { .. } => {}
        ClientReply::Value(Some(v)) if v == b"v2" => {}
        ClientReply::Value(Some(v)) if v == b"v1" => {
            panic!("stale read: the deposed leader served v1 after v2 was committed")
        }
        other => panic!("unexpected reply {other:?}"),
    }
}
```

- [ ] **Step 1:** write it and add `mod linearizability;`.
- [ ] **Step 2:** **run it and watch it fail** with the `stale read` panic.
      Record the output. If it passes here, the harness is not reproducing the
      partition and task 2 is not finished — do not proceed.
- [ ] **Step 3:** commit the failing test, marked `#[ignore]` with a comment
      naming the task that unignores it, so the tree stays green between
      commits while the evidence stays in history.

---

## Task 4: ReadIndex in `kv-raft`

**Files:** modify `crates/kv-raft/src/node.rs`, `message.rs`; create
`crates/kv-raft/src/tests/read_index.rs`. Modify `proto/raft.proto` and
`crates/kv-node/src/transport/convert.rs`.

### Why the wire needs one new field

ReadIndex is: record `commit_index`, confirm with a heartbeat quorum that we
still lead, then read. The confirmation must be evidence of leadership **at or
after** the moment the index was recorded. An `AppendEntriesResp` that was
already in flight when the read arrived proves leadership at some earlier
instant, and the leader could have been deposed in between. Counting it is a
stale read that appears only under a partition — precisely the bug this
milestone exists to remove.

There is no way to tell an old ack from a new one with the current message
shape, so the heartbeat carries a round number and the response echoes it:

```protobuf
// AppendEntriesRequest
optional uint64 read_round = 7;
// AppendEntriesResponse
optional uint64 read_round = 6;
```

`optional`, for the same reason the conflict hints are: absence is meaningful,
and a sentinel 0 would make round 0 and "no round" indistinguishable. M5's
proptest round-trip covers the new field automatically once it is added to the
`Message` variants; verify by mutation that dropping it fails that test.

### Rejecting a read the leader cannot yet serve

A leader whose `commit_index` still points into a previous term may be missing
entries it is required to have. Raft's answer is the no-op appended on
election: until that commits, the leader must not serve reads. So
`read_index` refuses while `storage.term(commit_index) != current_term`.

### Interface

```rust
pub enum ReadIndexError {
    NotLeader,
    /// The leader has not yet committed an entry in its own term, so its
    /// commit index may not reflect everything it is required to hold.
    NoQuorumInTerm,
}

impl RaftNode {
    pub fn read_index(&mut self, token: u64) -> Result<(), ReadIndexError>;
}
```

The confirmed read surfaces as `Ready::read_states` — the field M3 already
placed. Reads that are never confirmed simply never appear there; the node side
times them out.

- [ ] **Step 1:** write `crates/kv-raft/src/tests/read_index.rs` covering:
      a follower refuses; a leader with an uncommitted-term commit index
      refuses; a leader confirms after a quorum of acks **for the current
      round** and emits the `ReadState`; **an ack echoing an older round does
      not confirm** (the named regression for the reason the field exists); the
      recorded index is the commit index at request time, not at confirm time.
- [ ] **Step 2:** run them; confirm they fail for the expected reasons.
- [ ] **Step 3:** implement. Leader state gains `read_round: u64`,
      `pending_reads: Vec<(u64, LogIndex)>`, `round_acks: BTreeSet<NodeId>`.
      `read_index` bumps the round, clears the acks, records the pending read
      and broadcasts. `handle_append_entries_resp` counts an ack only when its
      echoed round equals the current one; at quorum, drains `pending_reads`
      into `Ready::read_states`. Stepping down clears all three.
- [ ] **Step 4:** `cargo nextest run -p kv-raft -p kv-sim`, then the 10k-seed
      election sweep. The core changed; the cheap sweep is the gate.
- [ ] **Step 5:** commit: `M7: ReadIndex with round-stamped quorum confirmation`.

---

## Task 5: serve reads through ReadIndex

**Files:** create `crates/kv-node/src/read_index.rs`; modify `driver.rs`,
`kv_service.rs`, `config.rs`.

The driver stops answering `Get` from local state. Instead:

1. allocate a token, call `node.read_index(token)`;
2. on `NotLeader`/`NoQuorumInTerm`, answer `NotLeader { hint }` immediately;
3. otherwise park the request in `pending_reads: BTreeMap<u64, PendingRead>`;
4. when a `ReadState { token, index }` appears in `Ready`, park it further
   until `last_applied >= index`, then read the engine and answer;
5. time out a read that is never confirmed, answering `NotLeader` — a
   partitioned leader must not leave the client hanging.

`last_applied` is the driver's own count of entries it has applied to the
`Engine`, not `RaftNode`'s: the core considers an entry applied when it hands
it over in `Ready::committed`, and the driver is what actually applies it.

- [ ] **Step 1:** unignore task 3's test. Run it. It must now pass.
- [ ] **Step 2:** the existing M6 driver and end-to-end tests must stay green —
      a read on a healthy leader still returns the value, and the
      three-process gate still passes.
- [ ] **Step 3:** commit: `M7: reads served through ReadIndex, never from local state`.

---

## Task 6: lease reads behind a flag

**Files:** modify `config.rs`, `driver.rs`.

`--lease-reads` (default **off**). When on, a leader that received acks from a
majority at time `T` serves reads locally until
`T + election_timeout - clock_drift_bound`, with no round trip.

Its correctness rests on bounded clock drift, which ReadIndex does not need.
That is exactly why it is opt-in, and the flag's help text and the README say
so rather than implying it is free. Off by default means the gate in task 3
keeps testing the safe path.

- [ ] **Step 1:** test that with the flag **on**, the task 3 scenario can be
      made to produce a stale read once the lease outlives the partition —
      documenting the trade rather than hiding it — and that with the lease
      expired it refuses.
- [ ] **Step 2:** implement, commit: `M7: opt-in lease reads, and what they cost`.

---

## Task 7: the session table, inside the state machine

**Files:** create `crates/kv-node/src/session.rs`; modify `command.rs`,
`driver.rs`, `kv_service.rs`, `crates/kv-client/src/lib.rs`, `proto/kv.proto`.

`(client_id, sequence_number) -> last_response`, keyed by client. **Inside** the
replicated state machine, not beside it: a session table held in driver memory
vanishes on leader failover, which is the only situation it exists for.

So it is part of what `Command` application writes, and it lives in the same
Bitcask instance as user data under a reserved key prefix (`\x00session/`),
which cannot collide with a user key because the client API forbids an empty
key... **verify that**: if user keys may begin with `\x00`, the prefix must be
escaped instead. Check before relying on it.

Application becomes: decode the command, look up `client_id`; if
`sequence_number <= last_seq`, return the cached response without re-applying;
otherwise apply, then record `(seq, response)`.

`Command` gains a `Cas { key, expected, new_value }` variant and every mutating
variant carries the `ClientContext` the proto already defines.

- [ ] **Step 1:** tests: a replayed `Put` applies once; a replayed `Cas`
      returns the *original* answer rather than re-evaluating (the `Cas` case
      is the one that matters — a re-evaluated Cas returns `swapped: false` the
      second time, which is a wrong answer, not just a wasted write); the
      session table survives a leader change.
- [ ] **Step 2:** implement, commit: `M7: session table inside the state machine`.

---

## Task 8: `Cas`, and the client half

**Files:** modify `kv_service.rs`, `crates/kv-client/src/lib.rs`,
`crates/kv-client/src/cli.rs`.

`KvService::cas` stops returning `unimplemented`. The client generates a random
`client_id` at construction and a monotonic `sequence_number` per request, and
**reuses the same context on a retry** — that is the entire point; a retry that
picks a fresh sequence number is a new request and will double-apply.

- [ ] **Step 1:** the gate: a `Cas` retried against a leader that committed it
      but died before replying applies exactly once.
- [ ] **Step 2:** CLI subcommand `cas <key> <expected> <new>`.
- [ ] **Step 3:** full verification: fmt, clippy, workspace tests, both sweeps.
- [ ] **Step 4:** update `CLAUDE.md` and `README.md` — reads are no longer
      deliberately stale, and that paragraph must go.
- [ ] **Step 5:** commit: `M7: Cas with exactly-once retry semantics`.

---

## Gates

- ✅ ReadIndex never returns stale data under a partition where a deposed
  leader still thinks it leads — **and the test failed first**, at task 3.
- ✅ A retried `Cas` applies exactly once.
- ✅ Workspace tests, clippy, fmt, both kv-sim sweeps.

## Deliberately not in M7

- **Follower reads** via ReadIndex forwarded to the leader (§1.10 calls it a
  stretch goal). Read scaling is not a problem this project has yet.
- **Session expiry.** The table grows without bound. Real systems lease
  sessions and reap them; here that is a `BTreeMap` that M8's snapshots will
  have to carry, and bounding it belongs with the snapshot work.
