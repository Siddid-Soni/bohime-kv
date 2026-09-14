# Bohime Implementation Roadmap — M1.6 through M13

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take Bohime from a working single-node Bitcask engine (M1.5, committed) to a sharded, Raft-replicated, crash-tested key-value store with a demo script that proves linearizability under fault injection (M13).

**Architecture:** Seven crates around one load-bearing boundary — `kv-raft` is a pure state machine that returns `Action`/`Ready` values and performs no I/O, no async, and no clock reads. `kv-node` executes those actions against tokio/tonic/disk; `kv-sim` executes the same actions against a virtual clock and a lossy in-memory network. Every correctness claim in this project is bought with that boundary, so no milestone below is allowed to breach it for convenience.

**Tech Stack:** Rust 2024 edition, stable toolchain, resolver 3. tokio 1 + tonic 0.13 + prost 0.13 (transport), prost-generated types in `kv-proto`, crc32fast (record integrity), bincode + serde (Raft log entries and snapshots), left-right 0.11 + arc-swap 1 (concurrency), proptest 1 + loom 0.7 + criterion 0.5 + tempfile 3 (testing), clap 4 (CLI), tracing 0.1.

**Spec:** `docs/DESIGN.md` — Part 1 is the concepts, Part 2 the architecture, Part 3 the milestone list with its ✅ acceptance criteria. This roadmap argues from Part 3; read both.

## How this plan is structured

This roadmap is the spine: one section per milestone, each with its goal, the exact files it touches, the interfaces it publishes to later milestones, its task decomposition, and its acceptance gate copied from `docs/DESIGN.md`.

Milestones with **a linked plan file** have been expanded into full step-level TDD plans (write test → watch it fail → implement → watch it pass → commit) and are ready to execute today:

- M1.6 → `docs/superpowers/plans/2026-09-15-m1.6-crash-safety.md`
- M1.7 → `docs/superpowers/plans/2026-09-15-m1.7-fsync-policy.md`
- M2 → `docs/superpowers/plans/2026-09-15-m2-raft-storage.md`

Milestones M3 onward are specified here to the level of files, signatures, task boundaries, and named tests — but **not** expanded to step level, deliberately. Their step-level detail depends on interface decisions that get made in M3.1 (the `Message`/`Action`/`Ready` shape) and again in M11 (the shard router). Writing that detail today would produce confident-looking instructions that are wrong by the time anyone reaches them. **Expand each into its own plan file at the start of that milestone**, using this section as its spec.

## Global Constraints

Copied from `docs/DESIGN.md` and the project's `CLAUDE.md`. Every task below implicitly includes this section.

- **Purity boundary:** `kv-raft` has zero dependencies on `tokio`, `tonic`, `std::fs`, `std::time`, or any RNG seeded from the environment. Timers are tick counters, not clocks. Violating this breaks M4 and is never an acceptable shortcut.
- **`kv-storage` knows nothing about Raft.** The Bitcask-backed `RaftStorage` impl therefore lives in `kv-node`, not `kv-storage` (see M2).
- **TDD is not optional.** Write the failing test, run it, confirm it fails *for the expected reason*, then implement. A milestone is done when its ✅ tests pass under `cargo nextest run`, not when the code compiles.
- **Test placement convention.** Unit tests are never inline. A source file with tests ends in:
  ```rust
  #[cfg(test)]
  #[path = "tests/<filename>.rs"]
  mod tests;
  ```
  with the test code in `src/tests/<filename>.rs` starting `use super::*;`. Integration tests that cross crates go in `tests/` at the workspace root.
- **Keydir mutation goes through `KeyDirIndex`.** Never touch `HashMapIndex.map` directly from `engine.rs`. That trait is the seam `left-right` swaps into at M11.5.
- **Every milestone is exactly one commit**, made only after all three CI gates pass locally:
  ```
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo nextest run --workspace
  ```
- **Commit message format:** `M<N>: <what landed>` (matching `M1.5: compaction + hint files`).
- **Fix the CI branch trigger.** `.github/workflows/ci.yml` triggers on push to `main`; the repo's default branch is `master`, so nothing has ever actually run in CI. Fix this in M1.6, the next commit to touch the repo.
- **Sub-milestones (`M3.1`, `M3.2`, …) may each be their own commit.** The parent milestone's ✅ gate must pass before moving to the next parent.

## Dependency order

```
M1.6 ──► M1.7 ──► M2 ──┐
                        ├──► M3 ──► M4 ──► M5 ──► M6 ──► M7 ──► M8 ──► M9 ──┐
                        │                                                    │
                        └────────────────────────────────────────────────────┤
                                                                             ▼
                                                        M10 ──► M11 ──► M11.5 ──► M12 ──► M13
```

M3 depends on M2 only for the `RaftStorage` trait definition, not its Bitcask impl — M3 develops against the in-memory impl exclusively. M1.7 is independently skippable if you want to reach Raft sooner; the fsync policy is a durability knob, not a correctness prerequisite for anything before M6. Everything else is a hard dependency.

## Stopping points

The milestone boundaries are placed so that stopping after **M6**, **M9**, or **M13** yields something coherent to show. If time runs short, stop at one of those, not between them.

---

## M1.6 — Crash safety  *(~1 session)*

**Detailed plan:** `docs/superpowers/plans/2026-09-15-m1.6-crash-safety.md`

**Goal:** A segment whose tail was torn by a crash must not make the engine permanently unopenable. Recover to the last intact record, discard the torn tail, and accept new writes at the right offset.

**Files:** Modify `crates/kv-storage/src/engine.rs`, `crates/kv-storage/src/tests/engine.rs`, `.github/workflows/ci.yml`. Revert the stray uncommitted comment deletions in `crates/kv-storage/src/record.rs` first — they fail `cargo fmt --check` on trailing whitespace.

**Interfaces produced:**
```rust
fn replay(file: &File, segment_id: SegmentId, index: &mut HashMapIndex) -> io::Result<u64>
```
returns the byte length of the valid prefix instead of `()`, so `open` can truncate.

**Tasks:**
1. Recover from a torn tail — `replay` stops at the first undecodable record, `open` truncates the active segment to the valid prefix and derives `active_offset` from it.
2. Make compaction crash-safe — the merged segment is written to `{id}.seg.tmp` and renamed, and a `compaction.manifest` (written tmp-then-renamed) records which old segment ids must be unlinked, so `open` can finish an interrupted compaction.
3. Harden hint files — atomic write via tmp-then-rename, and fall back to full replay instead of erroring when a hint file fails to parse.
4. Proptest the recovery invariant across arbitrary op sequences and truncation points.

**Three decisions, made** (these were the open questions from brainstorming; the rationale is recorded here so the executor doesn't have to relitigate them):

- **Only the highest-id segment may be truncated.** A crash can only tear the active segment. A torn *closed* segment means real corruption — bit rot or a partial disk write to data that was already durable — and silently discarding it would be data loss dressed up as recovery. Closed segments that fail to decode return `io::ErrorKind::InvalidData` naming the segment id and byte offset. This also matches the milestone's ✅ wording, which tests truncation of "the final record".
- **Torn hint files fall back to replay** rather than erroring. Once Task 2 lands, a torn hint is nearly unreachable — but "nearly" is not "never" (pre-existing data written by today's non-atomic `fs::write`, or disk corruption), and the fallback converts a fatal open into a slow one. Defense in depth, kept cheap.
- **Compaction atomicity is in scope for M1.6, not deferred.** Task 2 is not gold-plating; `apply_compaction` has a real crash-corruption bug today, described below. It is crash safety, which is what this milestone is. It gets its own commit so it can be reviewed and reverted independently.

**The compaction bug Task 2 fixes** (worth stating plainly, because it is not obvious): `apply_compaction` reuses the *smallest* retired segment id for the merged output, then deletes the other retired segments. Suppose segments `{3,4,5}` are retired and the merge is written to id 3. Crash after the merge is written but before 4 and 5 are unlinked, and the next `open` replays 3, then 4, then 5 in ascending order — so the *stale* records in 4 and 5 land last and win. Overwritten keys revert to old values and deleted keys resurrect. A marker file that `open` acts on before replaying makes the unlink step idempotent and closes this.

**Gate:**
- ✅ Engine recovers to the last intact record and accepts new writes; every truncation offset in the final record is tested.
- ✅ A write made after recovery survives a second reopen (this is the assertion that catches the `active_offset` bug — segments are opened `append(true)`, so without truncation the next write lands past the garbage and the stream is unparseable forever).
- ✅ Reopen after a simulated crash mid-compaction yields exactly the pre-compaction logical state.

**Note on what "crash" means here.** Nothing in the engine calls `fsync` yet, so these tests simulate a crash by truncating and corrupting files directly rather than by killing a process. M1.6 makes recovery correct for whatever reached the disk; M1.7 is what makes *what reaches the disk* controllable. Do not conflate the two.

---

## M1.7 — Configurable fsync policy + group commit  *(~1 session)*

**Detailed plan:** `docs/superpowers/plans/2026-09-15-m1.7-fsync-policy.md`

**Goal:** Make durability a deliberate, measured choice rather than an accident of the page cache, and produce the first real benchmark numbers in the project.

**Files:** Create `crates/kv-storage/src/config.rs`. Modify `crates/kv-storage/src/engine.rs`, `crates/kv-storage/src/lib.rs`, `crates/kv-storage/benches/engine.rs`, `README.md`. Create `crates/kv-storage/src/tests/config.rs`.

**Interfaces produced:**
```rust
pub enum FsyncPolicy { Never, EveryWrite, GroupCommit { max_batch: usize, max_delay: Duration } }
pub struct EngineConfig { pub max_segment_size: u64, pub fsync_policy: FsyncPolicy }
impl Default for EngineConfig { /* 64 MiB, FsyncPolicy::EveryWrite */ }
impl Engine {
    pub fn open_with_config(dir: impl AsRef<Path>, config: EngineConfig) -> io::Result<Self>;
    pub fn sync(&mut self) -> io::Result<()>;   // force a flush regardless of policy
}
```
`open_with_max_segment_size` is retained as a thin wrapper so the existing M1.4/M1.5 tests keep compiling unchanged.

**Tasks:**
1. Introduce `EngineConfig`/`FsyncPolicy` and thread them through `open`, keeping the old constructors working.
2. Implement `Never` and `EveryWrite`, with an explicit `sync()`.
3. Implement `GroupCommit`, tracking unsynced record count and elapsed time since the last sync; `put`/`delete` flush when either bound is crossed.
4. Criterion benchmarks across all three policies; record the numbers in the README with the hardware they were measured on.

**Decision:** `Instant::now()` inside `kv-storage` is fine. The no-clock-reads rule is a `kv-raft` rule and only a `kv-raft` rule — it exists to make M4's simulation deterministic, and the storage engine is never inside that loop.

**Gate:**
- ✅ Criterion benchmarks all three policies; the numbers are in the README, with the hardware they were measured on.
- ✅ Each policy issues the sync calls it promises, asserted against a sync counter on the engine — not merely "the fast one is faster". Note that a genuine durability demonstration needs power loss, not a process kill: the page cache survives `kill -9`, so no test in this repo can prove `EveryWrite` saves data that `Never` loses. Say so in the README rather than implying a guarantee the test does not establish.

---

## M2 — Raft persistent state  *(~1 session)*

**Detailed plan:** `docs/superpowers/plans/2026-09-15-m2-raft-storage.md`

**Goal:** Define the storage interface Raft needs, and prove one conformance suite passes against both an in-memory and a Bitcask-backed implementation — so M3 can develop entirely against the fast in-memory one without the production path silently drifting.

**Files:** Create `crates/kv-raft/src/storage.rs`, `crates/kv-raft/src/types.rs`, `crates/kv-raft/src/testing.rs`, `crates/kv-raft/src/tests/storage.rs`, `crates/kv-node/src/storage.rs`, `crates/kv-node/src/tests/storage.rs`. Modify `crates/kv-raft/src/lib.rs`, `crates/kv-raft/Cargo.toml`, `crates/kv-node/Cargo.toml`.

**Interfaces produced:**
```rust
pub type Term = u64;
pub type LogIndex = u64;
pub type NodeId = u64;

pub struct Entry { pub term: Term, pub index: LogIndex, pub command: Vec<u8> }
pub struct HardState { pub term: Term, pub voted_for: Option<NodeId>, pub commit_index: LogIndex }

pub trait RaftStorage {
    type Error: std::error::Error + Send + Sync + 'static;
    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error>;
    fn hard_state(&self) -> Result<HardState, Self::Error>;
    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error>;
    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error>;
    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error>;
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error>;
    fn last_index(&self) -> Result<LogIndex, Self::Error>;
    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error>;
}
```
plus `kv_raft::testing::assert_storage_conformance<S: RaftStorage>(make: impl Fn() -> S)`, exported behind a `testing` cargo feature.

**Decision — where each piece lives.** The trait and the in-memory impl go in `kv-raft`: a trait definition and a `BTreeMap` are pure, so the boundary holds. The Bitcask-backed impl goes in **`kv-node`**, not `kv-storage` — `CLAUDE.md` says `kv-storage` has no knowledge of Raft, and putting the impl there would make the storage engine depend on `kv-raft`, inverting the layering. `kv-node` already depends on both. The shared conformance suite lives in `kv-raft` behind a feature flag so `kv-node`'s tests can call it without `kv-raft` carrying test-only code into release builds.

**Tasks:**
1. `types.rs` — `Term`, `LogIndex`, `NodeId`, `Entry`, `HardState`, `Snapshot`, with serde derives.
2. `storage.rs` — the `RaftStorage` trait plus `MemStorage`, the `BTreeMap`-backed impl.
3. `testing.rs` — the conformance suite as a generic function, behind the `testing` feature.
4. `kv-node/src/storage.rs` — `BitcaskStorage`, mapping index → `{index:020}` key and hard state → a reserved key, with `last_index` cached in memory because Bitcask has no range scan.
5. Run the same conformance suite against both impls.

**Gate:**
- ✅ The same trait-conformance suite passes against both impls.
- ✅ `truncate_suffix(from)` then `append` leaves no ghost entries — specifically, `entries(from, last+1)` after the re-append returns only the new entries, and a reopen of `BitcaskStorage` agrees.
- ✅ Hard state survives reopen.

---

## M3 — Raft core, the pure state machine  *(~4-6 sessions — the heart of the project)*

**Expand into its own plan file before starting.** M3.1 is the milestone where the `Message`/`Action`/`Ready` shape gets fixed, and every later milestone's detail hangs off it. Write `docs/superpowers/plans/<date>-m3-raft-core.md` using this section as its spec, after M3.1 lands and the types are real.

**Goal:** A `RaftNode` that implements Raft figure 2 correctly and performs no I/O. It accepts ticks, messages, and proposals; it returns descriptions of work for someone else to do.

**Files:** Create `crates/kv-raft/src/node.rs`, `raft/log.rs`, `raft/election.rs`, `raft/replication.rs`, `raft/invariants.rs`, with tests in `crates/kv-raft/src/tests/`. Modify `crates/kv-raft/src/lib.rs`, `types.rs`.

Split by responsibility, not by layer: `node.rs` owns the role state machine and dispatch; `log.rs` owns index/term arithmetic and the consistency check; `election.rs` and `replication.rs` own the two message families. `node.rs` growing past ~600 lines is the signal that dispatch and behavior have tangled — split before continuing.

**Interfaces produced** (fixed at M3.1; everything downstream consumes these):
```rust
pub struct Config { pub id: NodeId, pub peers: Vec<NodeId>, pub election_timeout: u64, pub heartbeat_interval: u64 }

pub enum Message { RequestVote {..}, RequestVoteResp {..}, AppendEntries {..}, AppendEntriesResp {..}, InstallSnapshot {..}, InstallSnapshotResp {..} }

pub enum Action { Persist(Vec<Entry>), PersistHardState(HardState), Send { to: NodeId, msg: Message }, Apply { up_to: LogIndex }, Snapshot { at: LogIndex } }

pub struct Ready { pub entries: Vec<Entry>, pub hard_state: Option<HardState>, pub messages: Vec<(NodeId, Message)>, pub committed: Vec<Entry>, pub snapshot: Option<Snapshot> }

impl RaftNode {
    pub fn new(config: Config, storage: impl RaftStorage) -> Self;
    pub fn tick(&mut self) -> Vec<Action>;
    pub fn step(&mut self, msg: Message) -> Vec<Action>;
    pub fn propose(&mut self, cmd: Vec<u8>) -> Result<LogIndex, ProposeError>;
    pub fn ready(&mut self) -> Ready;
    pub fn role(&self) -> Role;
}
```

**Tasks (one commit each):**

- **M3.1 — Types.** `Term`, `LogIndex`, `NodeId`, `Message`, `Action`, `Ready`, `Config`, `Role`. No behavior. The test is that the types compose: construct a `Ready` with entries, a hard state, and two messages, and round-trip every `Message` variant through bincode.

  Decide here whether `RaftNode` owns its storage or borrows it. **Recommendation: own it, generic over `S: RaftStorage`.** Borrowing forces a lifetime through every downstream signature including the simulator's node table, for no gain.

- **M3.2 — Role state machine + `tick()`.** Timers are `u64` counters incremented by `tick`, never `Instant`.
  ✅ A follower receiving no heartbeat becomes a candidate after exactly `election_timeout` ticks; one that keeps receiving heartbeats never does, over 10,000 ticks.
  Randomized election timeout comes from a seed passed in `Config`, not from `rand::thread_rng` — thread RNG would destroy M4's reproducibility.

- **M3.3 — `RequestVote`.** Grant/deny rules, term rules, vote persistence.
  ✅ A candidate with a majority becomes leader; a node never votes twice in one term (including across a simulated restart, since the vote is in `HardState`); a higher term always causes step-down, in every role.

- **M3.4 — `AppendEntries`.** Consistency check, conflict truncation, conflict hint on rejection.
  ✅ Divergent-follower repair converges, table-driven from the paper's figure 7 — build all six of figure 7's follower logs as test fixtures and assert each converges to the leader's log.
  ✅ The conflict hint (`conflict_term`/`conflict_index`, already in `proto/raft.proto`) converges in ≤2 round trips on a log where naive `next_index` decrement takes N. Assert the round-trip *count*, not just the outcome — that number is the whole point of the optimization.

- **M3.5 — Commit index advancement + apply.**
  ✅ Commits at the majority `match_index`.
  ✅ **Explicitly refuses to commit a prior-term entry by count alone.** Build the paper's figure 8 scenario as a named test (`figure_8_stale_term_entry_is_not_committed_by_count`). This is the single most commonly botched rule in hand-rolled Raft; if this test does not exist by name, the milestone is not done.
  ✅ A new leader appends a no-op entry on election.

- **M3.6 — Election restriction.**
  ✅ A node with a stale log cannot win against an up-to-date voter.
  ✅ Proptest asserting Leader Completeness over randomly generated histories.

- **M3.7 — Invariant suite.** `invariants.rs` exposes `check_all(&[RaftNode]) -> Result<(), Violation>` covering election safety, log matching, leader completeness, and state machine safety. It must be callable after *any* `step`, because M4 calls it after every single one.
  ✅ Each invariant has a test that deliberately constructs a violating cluster state and confirms the checker catches it. An invariant checker that has never failed is not known to work.

**Gate:** all six sub-milestone ✅ criteria, plus the invariant suite runnable after any step.

**Risks:**
- The commit rule (M3.5) and the election restriction (M3.6) are where hand-rolled Raft implementations are usually wrong, and both failures are *silent* — the cluster works fine until a specific partition-and-recover sequence loses a committed write. This is exactly what M4 exists to find. Do not be reassured by M3's own tests passing.
- Resist adding batching, pipelining, or pre-vote here. They are M13 and M9 concerns; adding them now doubles the state space M4 has to search before M4 exists.

---

## M4 — Deterministic simulator  *(~2-3 sessions)*

**Expand into its own plan file before starting**, once M3's `Action` enum is final.

**Goal:** Run whole clusters under adversarial networks, thousands of seeds at a time, with any failure reproducible byte-for-byte from its seed alone. This is the milestone that makes every correctness claim in this project defensible.

**Files:** Create `crates/kv-sim/src/clock.rs`, `network.rs`, `cluster.rs`, `nemesis.rs`, `tests/simulation.rs`. Modify `crates/kv-sim/src/lib.rs`, `crates/kv-sim/Cargo.toml` (add `kv-raft`, `rand`, `proptest`).

**Interfaces produced:**
```rust
pub struct SimConfig { pub seed: u64, pub nodes: usize, pub drop_rate: f64, pub max_delay_ticks: u64, pub duplicate_rate: f64, pub reorder: bool }
pub struct Cluster { /* virtual clock, seeded StdRng, in-flight message queue, node table */ }
impl Cluster {
    pub fn new(config: SimConfig) -> Self;
    pub fn step(&mut self) -> Result<(), Violation>;   // one tick: deliver, tick, drain Ready, check invariants
    pub fn run(&mut self, ticks: u64) -> Result<(), Violation>;
    pub fn partition(&mut self, groups: &[&[NodeId]]);
    pub fn heal(&mut self);
    pub fn crash(&mut self, node: NodeId);   // drops all non-persisted state
    pub fn restart(&mut self, node: NodeId); // reopens from RaftStorage only
    pub fn leader(&self) -> Option<NodeId>;
}
```

**Tasks:**
1. Virtual clock + seeded `StdRng`. Every random decision in the simulator draws from this one RNG. **No `HashMap` iteration anywhere in the simulator's control flow** — Rust's `HashMap` iteration order is randomized per process and will silently destroy reproducibility. Use `BTreeMap` for the node table and the in-flight queue.
2. In-memory network with per-message drop / delay / duplicate / reorder, driven by the seeded RNG, plus arbitrary partitions expressed as a node-group partition of the cluster.
3. The cluster driver: deliver due messages, `tick()` every node, drain each `Ready`, persist to `MemStorage`, route messages back into the network, apply committed entries — then run the M3.7 invariant suite. Every step.
4. `crash(node)` drops everything not in `RaftStorage`; `restart(node)` rebuilds a `RaftNode` from persisted state alone. A node that comes back remembering its in-memory state is not testing anything.
5. The nemesis: randomized sequences of partition / heal / crash / restart, drawn from the same seed.
6. The seed sweep harness — a `#[ignore]`d test running 100k seeds, plus a fast default-run subset.

**Gate:**
- ✅ A 3-node cluster elects exactly one leader within N ticks, over 10,000 seeds.
- ✅ Under 20% loss and random partitions, invariants never break across 100,000 seeded runs.
- ✅ A failing seed reproduces byte-identically — assert this explicitly: run seed X twice, hash the full event trace, compare. This test is the foundation of the whole milestone and must exist as its own named test.
- ✅ Nemesis test: kill the leader mid-replication; no committed entry is ever lost.

**Risks:**
- **Reproducibility is easy to lose and hard to notice.** Beyond `HashMap` ordering: `SystemTime`, `thread_rng`, any `HashSet`, floating-point accumulation order, and `tokio` are all disqualifying. Add a CI check that `kv-sim` and `kv-raft` do not depend on `tokio` at all — `cargo tree -p kv-raft | grep -q tokio && exit 1`.
- Budget for M4 finding real bugs in M3. That is success, not schedule slip. The 100k-seed sweep should be expected to fail the first several times it runs.

---

## M5 — gRPC transport  *(~1-2 sessions)*

**Goal:** Carry `kv-raft`'s `Message` values between real processes, with backpressure and backoff that behave under a dead peer.

**Files:** Create `crates/kv-node/src/transport/mod.rs`, `transport/server.rs`, `transport/peer.rs`, `transport/convert.rs`, `crates/kv-node/src/tests/convert.rs`, `crates/kv-node/tests/transport.rs`. Modify `proto/raft.proto` (it is already a complete skeleton; fill gaps only as M3's `Message` demands), `crates/kv-node/Cargo.toml`.

**Interfaces produced:**
```rust
pub struct PeerClient { /* pooled tonic channel, bounded mpsc, reconnect state */ }
impl PeerClient {
    pub fn connect(addr: String, queue_depth: usize) -> Self;   // does not block on the peer being up
    pub fn try_send(&self, msg: Message) -> Result<(), SendError>;  // SendError::Full sheds load
}
pub struct RaftServer { inbox: mpsc::Sender<(NodeId, Message)> }  // impl kv_proto::raft::raft_service_server::RaftService
```
`convert.rs` holds `From`/`TryFrom` both ways between `kv_raft::Message` and the prost types. Keeping conversion in one file is what stops proto types leaking into `kv-raft`.

**Tasks:**
1. Conversions, tested first: proptest round-trip every `Message` variant through its proto representation and back. A lossy conversion here produces bugs that look like Raft bugs, which is the worst possible place to debug them.
2. `RaftService` server implementation, pushing into a bounded inbox.
3. `PeerClient` with a bounded outbound queue, per-RPC deadlines, and reconnect with exponential backoff and jitter.
4. Two-process integration test over a real socket.

**Gate:**
- ✅ Two processes exchange `RequestVote`/`AppendEntries` over real gRPC.
- ✅ A killed peer produces backoff, not a busy loop — assert the reconnect attempt *count* over a fixed window stays bounded; "it didn't spin" is not observable without a counter.
- ✅ A full outbound queue sheds load instead of growing unboundedly — assert queue depth stays at its bound while `try_send` returns `Full`.

**Decision:** Dropping a Raft message on a full queue is always safe — Raft's whole design assumes a lossy network, and M4 has already proven the core survives 20% loss. Blocking the driver loop on a slow peer is what is *not* safe. Never add an unbounded queue "temporarily".

---

## M6 — Single-shard node end to end  *(~2 sessions)*

**Goal:** The first real demo. Three processes, one Raft group, a client that can write and read, and a leader you can `kill -9`.

**Files:** Create `crates/kv-node/src/driver.rs`, `kv_service.rs`, `config.rs`, `crates/kv-client/src/lib.rs`, `crates/kv-client/src/cli.rs`, `crates/kv-node/tests/end_to_end.rs`. Modify `crates/kv-node/src/main.rs`, `crates/kv-client/src/main.rs`, `proto/kv.proto` (already a complete skeleton).

**Interfaces produced:**
```rust
pub struct Driver<S: RaftStorage> { node: RaftNode<S>, engine: Engine, /* ... */ }
impl Driver { pub async fn run(self) -> anyhow::Result<()>; }   // tick, drain Ready, persist, send, apply

pub struct Client { /* endpoints, leader hint, backoff */ }
impl Client {
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError>;
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError>;
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), ClientError>;
}
```

**Tasks:**
1. The driver loop: tick on an interval, drain `Ready`, **persist entries and hard state before sending anything** (§1.5 — this ordering is a correctness requirement, not an optimization), send messages, apply committed entries to the `Engine`.
2. `KvService` with `Put`/`Delete`/`Get` returning `NotLeader { leader_hint }` when not the leader.
3. `kv-client`: leader hint caching, retry with backoff on `NotLeader`.
4. `main.rs` config, `tracing-subscriber` init, `clap` args.
5. Three-process integration test including `kill -9` of the leader.

**Gate:**
- ✅ Three processes; `put k v` on the leader, `get k` on any node returns it.
- ✅ `kill -9` the leader, a new one is elected within ~1s, and the data is still there.

**Risk — the read at this milestone is deliberately wrong.** `Get` here reads local state and can return stale data. That is fine and expected at M6; M7 is where it gets fixed. Do not paper over it — write it down in the code as a comment naming M7, so it cannot be mistaken for a finished path.

---

## M7 — Linearizable reads + client sessions  *(~2 sessions)*

**Goal:** Reads that are actually linearizable, and retries that do not double-apply.

**Files:** Create `crates/kv-node/src/read_index.rs`, `crates/kv-node/src/session.rs`, `crates/kv-node/tests/linearizability.rs`. Modify `crates/kv-raft/src/node.rs` (ReadIndex is a Raft-core concern: the quorum confirmation is a message exchange, so it belongs behind `Action`), `kv_service.rs`, `crates/kv-client/src/lib.rs`.

**Interfaces produced:**
```rust
impl RaftNode {
    pub fn read_index(&mut self, token: u64) -> Vec<Action>;   // heartbeat quorum, then ReadState in Ready
}
pub struct ReadState { pub token: u64, pub index: LogIndex }
// in Ready: pub read_states: Vec<ReadState>
```

**Tasks:**
1. ReadIndex in `kv-raft`: record the commit index, confirm leadership with a heartbeat quorum, emit a `ReadState` once confirmed. Reject if the leader has not yet committed an entry in its own term.
2. Node side: wait for `last_applied >= read_state.index`, then serve the read.
3. Lease reads behind a config flag, off by default, documented as assuming bounded clock drift.
4. Session table **inside the state machine**, not beside it — `(client_id, sequence_number)` → last response. It must be part of the replicated state or it does not survive a leader change, which is the only case it exists for.
5. `Cas` in `KvService`.

**Gate:**
- ✅ ReadIndex never returns stale data under a partition where a deposed leader still thinks it leads. **Write this test against the naive M6 local read first, watch it fail, then fix.** A test that has only ever passed proves nothing about the bug it claims to cover.
- ✅ A retried `Cas` applies exactly once.

---

## M8 — Snapshots and compaction  *(~2 sessions)*

**Goal:** Stop the Raft log from growing forever, and let a node that has fallen too far behind catch up.

**Files:** Create `crates/kv-node/src/snapshot.rs`, `crates/kv-raft/src/snapshot.rs`. Modify `crates/kv-raft/src/node.rs`, `storage.rs` (`snapshot`/`apply_snapshot`, log truncation prefix), `crates/kv-node/src/transport/`, `driver.rs`.

**Interfaces produced:**
```rust
pub struct Snapshot { pub last_included_index: LogIndex, pub last_included_term: Term, pub data: Vec<u8> }
trait RaftStorage {
    fn compact(&mut self, up_to: LogIndex) -> Result<(), Self::Error>;   // discard the log prefix
    fn apply_snapshot(&mut self, snap: &Snapshot) -> Result<(), Self::Error>;
}
```

**Tasks:**
1. Snapshot the `Engine` state at an applied index. Bitcask already has compaction — the snapshot is a serialized scan of the keydir's live values at that index.
2. Log truncation: `compact(up_to)`, and `entries`/`term` correctly reporting "compacted away" instead of "missing" for a truncated prefix. Getting this distinction wrong causes a leader to believe a follower needs an impossible entry and loop forever.
3. `InstallSnapshot` as a client-streaming RPC with chunking (`proto/raft.proto` already declares it as `stream`).
4. Leader side: send `InstallSnapshot` when a follower's `next_index` has been compacted past.
5. M4 coverage: add snapshot-and-catch-up to the nemesis.

**Gate:**
- ✅ A node down long enough for the leader to compact past its `next_index` catches up via snapshot and rejoins.
- ✅ Restart-from-snapshot equals restart-from-full-log — same final state, asserted by comparing a full key/value dump.

---

## M9 — Membership change  *(~2 sessions)*

**Goal:** Grow and shrink a cluster under load without an unavailability window or a lost write.

**Files:** Create `crates/kv-raft/src/membership.rs`, `crates/kv-node/src/admin_service.rs`. Modify `crates/kv-raft/src/node.rs`, `types.rs` (`Entry` gains a `ConfChange` command kind), `crates/kv-node/src/driver.rs`, `proto/admin.proto` (already skeletoned with `AddNode`/`RemoveNode`).

**Tasks:**
1. Learner state: receives entries, does not vote, is not counted in quorum.
2. Catch-up tracking and promotion once the learner's `match_index` is within a threshold of the leader's.
3. Single-server add/remove as a log entry — the configuration takes effect **when appended, not when committed** (Ongaro's thesis ch. 4). Joint consensus is explicitly out of scope; single-server changes are sufficient and much smaller.
4. Leader removal triggers a clean handoff (`TimeoutNow` to the most up-to-date follower) rather than waiting out an election timeout.
5. `AdminService` wiring.
6. M4 coverage: membership churn under the nemesis.

**Gate:**
- ✅ Grow 3 → 5 under continuous writes with no unavailability window and no lost writes.
- ✅ Shrink 5 → 3.
- ✅ Removing the leader triggers a clean handoff.

**Risk:** Single-server membership change has a known unsafety if two changes are in flight at once. Enforce one at a time — reject a `ConfChange` proposal while an uncommitted one exists — and make that rejection a named test.

---

## M10 — Ring hashing + meta group  *(~2 sessions)*

**Goal:** A key → shard → replicas mapping that is stable, versioned, and itself linearizable.

**Files:** Create `crates/kv-ring/src/hash.rs`, `shard_map.rs`, `crates/kv-ring/src/tests/`, `crates/kv-node/src/meta.rs`. Modify `crates/kv-ring/src/lib.rs`, `crates/kv-ring/Cargo.toml` (add `arc-swap`, `serde`).

**Interfaces produced:**
```rust
pub const SHARD_COUNT: u32 = 256;
pub fn shard_for(key: &[u8]) -> ShardId;   // stable across processes and restarts — no DefaultHasher
pub struct ShardMap { pub version: u64, pub assignments: Vec<Vec<NodeId>> }  // indexed by ShardId
impl ShardMap { pub fn replicas(&self, shard: ShardId) -> &[NodeId]; }
pub type SharedMap = arc_swap::ArcSwap<ShardMap>;
```

**Tasks:**
1. `shard_for` over a fixed, explicitly-chosen hash. **Not `std::collections::hash_map::DefaultHasher`** — it is seeded per process, so the same key would map to different shards on different nodes. Use a fixed-seed xxhash or the existing `crc32fast`; write down which and why.
2. `ShardMap` type, versioned, serde-serializable (it is a Raft state machine's state).
3. Virtual-node assignment and rebalance computation as a pure function `rebalance(old: &ShardMap, nodes: &[NodeId]) -> ShardMap`.
4. The meta group: a second `RaftNode` whose state machine is the `ShardMap`.
5. Publish via `ArcSwap<ShardMap>`, read on every request.

**Gate:**
- ✅ Key→shard is stable across restarts — assert against a committed table of known key→shard pairs, so an accidental hash change fails CI loudly.
- ✅ Adding a node moves ≈`1/N` of shards and no more — assert the moved fraction is within a stated tolerance.
- ✅ The shard map is itself linearizable (it is Raft-replicated; run the M7 checker against it).

**Decision, already made in `docs/DESIGN.md` §1.15 — do not revisit:** the shard map uses `ArcSwap`, not `left-right`. A 256-entry map replaced wholesale has no natural oplog, so `left-right`'s `Absorb` would have nothing meaningful to absorb.

---

## M11 — Multi-Raft  *(~2-3 sessions)*

**Goal:** Many Raft groups per node, sharing one tick loop and one thread pool.

**Files:** Create `crates/kv-node/src/router.rs`, `shard_registry.rs`. Modify `crates/kv-node/src/driver.rs` (becomes multi-group), `kv_service.rs`, `transport/`, `crates/kv-client/src/lib.rs` (shard-map-aware routing).

**Tasks:**
1. Shard registry: `ShardId -> (RaftNode, Engine)`, one Bitcask directory per shard.
2. Router: look up the shard map from `ArcSwap`, dispatch the request to the local group or return `NotLeader`/`NotHosted` with a hint.
3. **One shared tick loop across all groups, not one thread per shard.** 256 shards × one OS thread each is the naive design this milestone exists to avoid; say so in the code comment.
4. Transport multiplexing: Raft messages carry a `shard_id` and are demultiplexed at the server.
5. Client-side shard map caching and invalidation.

**Gate:**
- ✅ 5 nodes × 3 replicas × 8 shards: writes to different shards land on different leaders and proceed concurrently.
- ✅ Killing a node degrades only the shards it led, and only briefly.

---

## M11.5 — Wait-free reads via `left-right`  *(~2 sessions)*

**Placed here deliberately:** before M11 there is no read concurrency to measure, so doing this earlier would be optimizing blind.

**Files:** Create `crates/kv-storage/src/index/left_right.rs`, `crates/kv-storage/src/tests/loom_index.rs`. Modify `crates/kv-storage/src/index.rs`, `engine.rs`, `crates/kv-node/src/driver.rs`, `read_index.rs`, `kv_service.rs`, `crates/kv-storage/benches/`, `README.md`.

**Tasks:**
1. `impl Absorb<KvOp> for KeyDir`; swap the `KeyDirIndex` impl behind the trait seam established at M1.3. No call site in `engine.rs` should need to change — if one does, the seam was wrong and that is worth knowing.
2. Apply loop becomes: `append()` every op in the committed batch → **one** `publish()` per batch → store `published_index` with `Ordering::Release`.
3. ReadIndex switches from waiting on `last_applied` to waiting on `published_index` (§1.15). This is the correctness trap the design document calls out: `last_applied` advances when the op is absorbed, `published_index` when it is *visible to readers*, and reading on the former returns stale data.
4. `ReadHandleFactory` in the tonic service struct; a per-task `ReadHandle`.
5. Keep the `RwLock<HashMap>` impl behind a config flag as the comparison arm.

**Gate:**
- ✅ **Stale-read regression test:** a `Get` issued after a `Put` returns must never miss it. **Write it against the naive `last_applied` wait first, watch it fail, then fix.** This is the whole point of the milestone.
- ✅ `absorb_first` and `absorb_second` leave both copies byte-identical after an arbitrary proptest-generated op sequence (`sync_with` correctness).
- ✅ Compaction `Relocate` ops flow through the single writer; a `loom` test over a small interleaving of apply + read + relocate.
- ✅ Criterion: read throughput vs. core count (1→20) for `RwLock<HashMap>`, `DashMap`, and left-right. **The scaling graph goes in the README** — it is the most legible performance artifact in the project.
- ✅ The full M4 simulation suite and the M7 linearizability tests still pass unchanged.

**Risk:** `loom` state spaces explode. Keep the loom test to two threads and three ops; if it takes more than a minute, the model is too big, not the machine too slow.

---

## M12 — Rebalancing + Merkle verification  *(~2-3 sessions)*

**Files:** Create `crates/kv-node/src/rebalance.rs`, `crates/kv-node/src/merkle.rs`, `crates/kv-node/src/tests/merkle.rs`. Modify `admin_service.rs`, `proto/admin.proto` (`VerifyShard` is already declared and returns `divergent_key_ranges`), `crates/kv-ring/src/shard_map.rs`.

**Tasks:**
1. Migration driver: learner-add → catch-up → promote → remove old, **one shard at a time, rate-limited**. Moving many shards at once is how a rebalance turns into an outage.
2. Merkle tree over each shard's keyspace at a given applied index — deterministic ordering, fixed fanout, computed from the `Engine`'s live keys.
3. `AdminService::VerifyShard`: compare roots across replicas, descend on mismatch, report divergent key ranges.

**Gate:**
- ✅ Add a node to a live 3-node cluster under load: data rebalances, no request fails, and every replica of every shard agrees on its Merkle root afterwards.
- ✅ Deliberately corrupt one replica's segment file → verification detects it and names the divergent key range.

---

## M13 — Proof, polish, and the resume artifact  *(~2-3 sessions)*

**Files:** Create `crates/kv-sim/src/linearizability.rs`, `scripts/demo.sh`, `docker-compose.yml`, `benches/cluster.rs`. Modify `README.md` substantially, `crates/kv-client/src/cli.rs`.

**Tasks:**
1. Linearizability checker (Wing-Gong / P-compositionality style) over recorded histories; run it against the real cluster under the nemesis, not only against the simulator.
2. Criterion benchmarks: throughput and latency vs. shard count, vs. fsync policy, with and without AppendEntries pipelining.
3. `kv-client` CLI polish; `docker-compose` or a shell script for a 5-node cluster.
4. README: architecture diagram, the benchmark table, the left-right scaling graph from M11.5, the seeded-simulation story, and **what is explicitly not supported and why** — no cross-shard transactions, no range scans, lease reads assume bounded clock drift. The non-goals section is what makes the rest credible.

**Gate:**
- ✅ `./scripts/demo.sh` brings up 5 nodes, drives load, kills nodes at random, and prints a passing linearizability verdict.
- ✅ Whole-system verification passes:
  ```bash
  cargo nextest run --workspace
  cargo test --release -p kv-sim -- --ignored     # 100k-seed sweep
  ./scripts/demo.sh
  cargo bench
  ```

---

## Self-review notes

Checked against `docs/DESIGN.md` Part 3: every milestone M1.6–M13 and every sub-milestone M3.1–M3.6 has a section, and every ✅ criterion in the spec appears in the corresponding **Gate**. Four things were added that the spec does not list, each flagged in place with its reason — compaction crash-atomicity and the hint-file fallback (M1.6, fixing a real crash-corruption bug in committed M1.5 code), the byte-identical-reproducibility test as its own named test (M4, since the spec asserts the property without testing it), and the M3.7 invariant-checker self-test (an invariant checker that has never failed is not known to work).

Type consistency checked across sections: `Term`/`LogIndex`/`NodeId`/`Entry`/`HardState`/`Snapshot` are defined once in M2 and used unchanged in M3, M8, and M10; `ShardId`/`ShardMap` are defined in M10 and used in M11 and M12; `RaftStorage` gains `compact`/`apply_snapshot` in M8 and that extension is stated there explicitly rather than silently assumed.

Placeholder scan: clean. Where a later milestone's detail is genuinely undetermined, the section says so and says what determines it, rather than writing "TBD".
