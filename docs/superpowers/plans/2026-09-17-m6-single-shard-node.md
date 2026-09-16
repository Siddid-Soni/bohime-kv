# M6 — Single-shard node end to end

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:executing-plans`
> or `superpowers:subagent-driven-development` to work this task-by-task. Steps
> use checkbox (`- [ ]`) syntax.

**Status:** planned. Expanded from the roadmap's M6 section now that M5 is
complete and the transport's real shapes (`Inbound`, `PeerClient`) are fixed
rather than sketched.

**Goal:** the first real demo. Three processes, one Raft group, a client that
writes and reads, and a leader you can `kill -9`.

**Spec:** `docs/DESIGN.md` §1.5, §1.13, §1.14 and Part 3 M6;
`docs/superpowers/plans/2026-09-15-bohime-roadmap.md` §M6.

**Architecture:** one tokio task owns the `RaftNode<BitcaskStorage>` and the
state-machine `Engine`, and is the only thing that touches either. Everything
else — the two gRPC services, the per-peer clients — talks to it over bounded
channels and waits on a `oneshot`. That keeps the Raft core single-threaded
and synchronous, which is the property M4 depends on, while the shell around
it is fully async.

---

## Global constraints

- `kv-raft` stays free of tokio, tonic, the filesystem and clock reads. CI
  enforces the tokio half. Tasks 1 and 2 touch `kv-raft`; both are pure.
- Test placement: `src/tests/<name>.rs`, declared in `src/tests/mod.rs`. No
  `crates/*/tests/` directories, no inline `mod tests`, no `use super::*` —
  tests reach code by absolute `crate::` paths.
- TDD, per micro-problem: write the failing test, run it, confirm it fails for
  the expected reason, then implement.
- `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D
  warnings` and `cargo nextest run --workspace` must all pass before the
  milestone is done.
- M6 is **one commit per task** here, squashed or kept as the tasks land; the
  milestone as a whole is what `docs/DESIGN.md` gates.

---

## The two core gaps this milestone exposed

Both were found reading M3 while planning, and both are real. They come first
because everything else is built on them.

### A single-node group can never commit

`try_advance_commit` is reachable from exactly one place —
`handle_append_entries_resp`'s success path (`node.rs:448`). A group with no
peers never receives one, so it elects itself leader (`start_election` already
handles the immediate win at `node.rs:271`), appends its no-op and every
client proposal, and commits **nothing, forever**.

That is a legitimate configuration — it is also the degenerate end of M9's
`shrink 5 → 3 → 1` — and it is what makes a driver test possible without
sockets or a three-node cluster. Fix: call `try_advance_commit` from
`become_leader` and `propose` as well. For any cluster of three or more this
is a no-op, because `count` starts at 1 for the leader and quorum is 2+; the
rule's own guard does the work.

### The core tracks no leader id

`NotLeader { leader_hint }` is the whole reason a client ever finds the
leader, and `RaftNode` has no field to answer it from. `voted_for` is not the
leader — it is who we voted for this term, which is frequently someone who
lost. A follower learns the leader only from the `leader_id` on an
`AppendEntries` it accepts.

---

## Task 1: `RaftNode::leader_id`

**Files:**
- Modify: `crates/kv-raft/src/node.rs`
- Create: `crates/kv-raft/src/tests/leader_hint.rs`
- Modify: `crates/kv-raft/src/tests/mod.rs`

**Produces:** `RaftNode::leader_id(&self) -> Option<NodeId>`

- [ ] **Step 1: Write the failing tests**

`crates/kv-raft/src/tests/leader_hint.rs`:

```rust
//! The leader hint (M6). `NotLeader { leader_hint }` is how a client finds
//! the leader, and the hint has to come from somewhere the core actually
//! knows: `voted_for` is not it — that is who we voted for, who often lost.

use crate::message::{Config, Message, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;
use crate::types::Entry;

fn config(id: u64, peers: Vec<u64>) -> Config {
    Config { id, peers, election_timeout: 10, heartbeat_interval: 2, seed: id }
}

#[test]
fn a_fresh_follower_knows_no_leader() {
    let node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    assert_eq!(node.leader_id(), None);
}

#[test]
fn accepting_append_entries_records_the_leader() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    node.step(
        2,
        Message::AppendEntries {
            term: 4,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );
    assert_eq!(node.leader_id(), Some(2));
}

#[test]
fn a_stale_leaders_append_is_not_believed() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    // Reach term 7 by hearing from the real leader.
    node.step(
        3,
        Message::AppendEntries {
            term: 7,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );
    // Node 2 still thinks it leads at term 4. Believing it would send every
    // client to a deposed leader.
    node.step(
        2,
        Message::AppendEntries {
            term: 4,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );
    assert_eq!(node.leader_id(), Some(3), "a lower-term append must not move the hint");
}

#[test]
fn winning_an_election_points_the_hint_at_ourselves() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    for _ in 0..25 {
        node.tick();
    }
    assert_eq!(node.role(), Role::Candidate);
    // A candidate has no leader to name.
    assert_eq!(node.leader_id(), None);

    node.step(2, Message::RequestVoteResp { term: node.current_term(), vote_granted: true });
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.leader_id(), Some(1));
}

#[test]
fn a_higher_term_clears_the_hint() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    node.step(
        2,
        Message::AppendEntries {
            term: 4,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    );
    assert_eq!(node.leader_id(), Some(2));

    // A new election is underway at a higher term; the old leader is gone and
    // we must stop directing clients to it.
    node.step(
        3,
        Message::RequestVote { term: 9, candidate_id: 3, last_log_index: 0, last_log_term: 0 },
    );
    assert_eq!(node.leader_id(), None, "a higher term means the old leader is stale");

    // Unused import guard: Entry keeps the test file honest if the shape moves.
    let _ = Entry { term: 1, index: 1, command: Vec::new() };
}
```

Add `mod leader_hint;` to `crates/kv-raft/src/tests/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-raft -E 'test(leader_hint)'`
Expected: compile error — `no method named leader_id found for struct RaftNode`.

- [ ] **Step 3: Implement**

In `crates/kv-raft/src/node.rs`, add to the struct (next to `voted_for`):

```rust
    /// Who we currently believe leads this term, for `NotLeader` hints (M6).
    /// Distinct from `voted_for`: that is who we voted for, who often lost.
    leader_id: Option<NodeId>,
```

Initialise `leader_id: None` in `new`. Then:

```rust
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
```

Set it in three places:
- `observe_higher_term`: `self.leader_id = None;`
- `start_election`: `self.leader_id = None;`
- `become_leader`: `self.leader_id = Some(self.config.id);`
- the accepted-`AppendEntries` arm of `step` (the `else` branch that already
  calls `reset_election_timer`): `self.leader_id = Some(leader_id);`

The rejected arm (`msg_term < self.current_term`) must **not** set it — that
is what `a_stale_leaders_append_is_not_believed` pins.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-raft` — all green, including the M3 suite.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-raft/src/node.rs crates/kv-raft/src/tests/
git commit -m "M6: RaftNode tracks the current leader for NotLeader hints"
```

---

## Task 2: a single-node group commits

**Files:**
- Modify: `crates/kv-raft/src/node.rs`
- Create: `crates/kv-raft/src/tests/single_node.rs`
- Modify: `crates/kv-raft/src/tests/mod.rs`

**Consumes:** nothing. **Produces:** no new API — a behaviour change.

- [ ] **Step 1: Write the failing test**

`crates/kv-raft/src/tests/single_node.rs`:

```rust
//! A group of one. Legitimate on its own (the M6 driver test runs one), and
//! the degenerate end of M9's shrink. Before M6, `try_advance_commit` was
//! reachable only from an AppendEntries response, which a group with no peers
//! never receives — so it elected itself and then committed nothing, forever.

use crate::message::{Config, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;

fn solo() -> RaftNode<MemStorage> {
    let config = Config {
        id: 1,
        peers: vec![],
        election_timeout: 10,
        heartbeat_interval: 2,
        seed: 1,
    };
    RaftNode::new(config, MemStorage::default())
}

#[test]
fn a_lone_node_elects_itself() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    assert_eq!(node.role(), Role::Leader);
}

#[test]
fn a_lone_leader_commits_its_own_no_op() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    // The no-op is index 1 and it is the only copy the quorum of one needs.
    assert_eq!(node.commit_index(), 1, "a group of one is its own majority");
}

#[test]
fn a_lone_leader_commits_and_applies_a_proposal() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    let _ = node.ready();

    let index = node.propose(b"set x 1".to_vec()).expect("the lone node leads");
    let ready = node.ready();

    assert_eq!(node.commit_index(), index);
    let applied: Vec<_> = ready.committed.iter().map(|e| e.command.clone()).collect();
    assert_eq!(applied, vec![b"set x 1".to_vec()], "it must reach the state machine");
}
```

Add `mod single_node;` to `crates/kv-raft/src/tests/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-raft -E 'test(single_node)'`
Expected: `a_lone_node_elects_itself` PASSES (that path already works); the
other two FAIL with `commit_index` 0. Confirm that split — if the election
test fails too, the cause is something other than the commit rule and the fix
below is the wrong one.

- [ ] **Step 3: Implement**

Two call sites in `crates/kv-raft/src/node.rs`. At the end of `become_leader`,
after `self.broadcast_heartbeats();`:

```rust
        // A group of one is its own majority: without this the lone leader
        // appends the no-op and never commits it, because the only other path
        // to the commit rule is an AppendEntries response that never arrives.
        // For any cluster of 3+ this is a no-op — `count` starts at 1 and
        // quorum is 2+, so the rule's own guard declines.
        self.try_advance_commit();
```

And at the end of `propose`, after the `send_append` loop and before
`Ok(index)`:

```rust
        self.try_advance_commit();
```

- [ ] **Step 4: Run to verify it passes**

```bash
cargo nextest run -p kv-raft
cargo nextest run -p kv-sim
```

Both must stay green — this changes when the commit rule runs, and kv-sim is
what would notice if it changed *what* it decides.

- [ ] **Step 5: Re-run the election sweep**

Run: `cargo nextest run -p kv-sim --run-ignored all -E 'test(election_sweep_full)'`
Expected: PASS, ~2s. This is a Raft-core change; the cheap sweep is the gate
that catches it. The 100k nemesis sweep runs once at the end of the milestone,
not per task (~80 min in debug).

- [ ] **Step 6: Commit**

```bash
git add crates/kv-raft/src/node.rs crates/kv-raft/src/tests/
git commit -m "M6: a single-node group commits its own entries"
```

---

## Task 3: the state machine command codec

**Files:**
- Create: `crates/kv-node/src/command.rs`
- Create: `crates/kv-node/src/tests/command.rs`
- Modify: `crates/kv-node/src/main.rs`, `crates/kv-node/src/tests/mod.rs`

`Entry::command` is opaque `Vec<u8>` — interpreting it is the state machine's
job. This is that interpretation, and it is the whole contract between the
log and the `Engine`.

**The trap:** a new leader appends a no-op whose command is `Vec::new()`
(`node.rs:287`). The apply path sees it like any other committed entry, so an
empty command must decode as "do nothing" rather than as an error or, worse,
as a `Put` of empty key and value.

**Produces:**
```rust
pub enum Command { Put { key: Vec<u8>, value: Vec<u8> }, Delete { key: Vec<u8> } }
impl Command {
    pub fn encode(&self) -> Vec<u8>;
    pub fn decode(bytes: &[u8]) -> Result<Option<Command>, CommandError>;  // Ok(None) == no-op
}
```

- [ ] **Step 1: Write the failing tests**

`crates/kv-node/src/tests/command.rs`:

```rust
use crate::command::Command;

#[test]
fn put_round_trips() {
    let cmd = Command::Put { key: b"k".to_vec(), value: b"v".to_vec() };
    let decoded = Command::decode(&cmd.encode()).unwrap();
    assert_eq!(decoded, Some(cmd));
}

#[test]
fn delete_round_trips() {
    let cmd = Command::Delete { key: b"k".to_vec() };
    let decoded = Command::decode(&cmd.encode()).unwrap();
    assert_eq!(decoded, Some(cmd));
}

/// The leader's no-op entry carries an empty command. It reaches the apply
/// path like any other committed entry, so it must decode as "nothing to do".
/// Treating it as an error would make every election poison the state machine.
#[test]
fn the_leaders_no_op_decodes_as_nothing() {
    assert_eq!(Command::decode(&[]).unwrap(), None);
}

/// The neighbouring trap: no real command may ever encode to zero bytes, or
/// it would be indistinguishable from the no-op and silently dropped.
#[test]
fn no_real_command_encodes_to_nothing() {
    let empty_put = Command::Put { key: Vec::new(), value: Vec::new() };
    assert!(!empty_put.encode().is_empty(), "an empty put must not collide with the no-op");
    let empty_delete = Command::Delete { key: Vec::new() };
    assert!(!empty_delete.encode().is_empty());
    assert_eq!(Command::decode(&empty_put.encode()).unwrap(), Some(empty_put));
}

#[test]
fn garbage_is_an_error_not_a_silent_no_op() {
    assert!(Command::decode(&[0xff, 0xff, 0xff, 0xff, 0xff]).is_err());
}
```

Add `mod command;` to `crates/kv-node/src/tests/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-node -E 'test(command)'`
Expected: compile error — `unresolved import crate::command`.

- [ ] **Step 3: Implement**

`crates/kv-node/src/command.rs`:

```rust
//! What a committed log entry means to the state machine (M6).
//!
//! `Entry::command` is opaque to Raft by design, so the interpretation lives
//! here. The one subtlety is the empty command: a new leader appends a no-op
//! in its own term (the figure-8 rule), and that entry is committed and
//! applied like any other. It must mean "do nothing" rather than be an error,
//! or every election would poison the state machine.
//!
//! bincode tags an enum variant with a u32, so no real command can encode to
//! zero bytes and collide with the no-op. There is a named test for that.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("undecodable command: {0}")]
    Encoding(#[from] bincode::Error),
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("a Command always serializes")
    }

    /// `Ok(None)` is the leader's no-op, not a failure.
    pub fn decode(bytes: &[u8]) -> Result<Option<Command>, CommandError> {
        if bytes.is_empty() {
            return Ok(None);
        }
        Ok(Some(bincode::deserialize(bytes)?))
    }
}
```

Add `mod command;` to `crates/kv-node/src/main.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-node -E 'test(command)'` — all five pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-node/src/command.rs crates/kv-node/src/tests/
git commit -m "M6: the state machine's command codec, no-op included"
```

---

## Task 4: `BitcaskStorage::sync`

**Files:**
- Modify: `crates/kv-node/src/storage.rs`
- Modify: `crates/kv-node/src/tests/storage.rs`

§1.5's ordering rule — entries and hard state durable **before** any message
goes out — is currently satisfied only by accident: `persist_entries` writes
through to storage eagerly (`node.rs:532`) and `EngineConfig`'s default policy
is `EveryWrite`, so each write fsyncs on the way in.

That is correct but invisible, and it is correct for a reason the driver does
not control. A future switch to `GroupCommit` for throughput would silently
break the guarantee with no test failing. So the driver calls `sync()`
explicitly before sending, and this is the method it calls. `Engine::sync` is
already a no-op when nothing is pending, so under `EveryWrite` it costs
nothing.

**Produces:** `BitcaskStorage::sync(&self) -> Result<(), BitcaskStorageError>`

- [ ] **Step 1: Write the failing test**

Append to `crates/kv-node/src/tests/storage.rs`:

```rust
/// The driver calls this before every send (§1.5). It must be safe to call
/// when nothing is pending, because most drains have nothing to persist.
#[test]
fn sync_is_callable_and_idempotent() {
    use kv_raft::storage::RaftStorage;
    let dir = tempfile::tempdir().unwrap();
    let mut s = BitcaskStorage::open(dir.path()).unwrap();
    s.append(&[kv_raft::Entry { term: 1, index: 1, command: b"x".to_vec() }]).unwrap();
    s.sync().unwrap();
    s.sync().unwrap();

    let reopened = BitcaskStorage::open(dir.path()).unwrap();
    assert_eq!(reopened.last_index().unwrap(), 1);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-node -E 'test(sync_is_callable)'`
Expected: `no method named sync found`.

- [ ] **Step 3: Implement**

In `crates/kv-node/src/storage.rs`, on `impl BitcaskStorage`:

```rust
    /// Flushes the Raft log to stable storage. The driver calls this after
    /// draining a `Ready` and **before** sending anything (§1.5: disk before
    /// network) — a vote must be durable before it is granted on the wire, or
    /// a crash lets the node vote twice in one term.
    ///
    /// A no-op when nothing is pending, so calling it on every drain is free.
    pub fn sync(&self) -> Result<(), BitcaskStorageError> {
        self.engine.borrow_mut().sync()?;
        Ok(())
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-node` — green.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-node/src/storage.rs crates/kv-node/src/tests/storage.rs
git commit -m "M6: BitcaskStorage::sync, the explicit disk-before-network point"
```

---

## Task 5: node configuration

**Files:**
- Create: `crates/kv-node/src/config.rs`
- Create: `crates/kv-node/src/tests/config.rs`
- Modify: `crates/kv-node/src/main.rs`, `crates/kv-node/src/tests/mod.rs`

**The trap:** `Config::seed` drives election-timeout randomisation. Give all
three processes the same seed and they draw the *same* timeout, time out on
the same tick, split the vote, and repeat — a livelock that looks exactly like
a network problem. The seed must be derived from the node id.

**Produces:**
```rust
pub struct NodeConfig {
    pub id: NodeId,
    pub listen: SocketAddr,
    pub peers: BTreeMap<NodeId, String>,   // id -> "http://host:port"
    pub data_dir: PathBuf,
    pub tick: Duration,
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
}
impl NodeConfig {
    pub fn raft_dir(&self) -> PathBuf;    // data_dir/raft
    pub fn state_dir(&self) -> PathBuf;   // data_dir/state
    pub fn raft_config(&self) -> kv_raft::Config;
}
pub struct Args { /* clap */ }
impl Args { pub fn into_config(self) -> anyhow::Result<NodeConfig>; }
```

- [ ] **Step 1: Write the failing tests**

`crates/kv-node/src/tests/config.rs`:

```rust
use crate::config::NodeConfig;

fn cfg(id: u64) -> NodeConfig {
    NodeConfig {
        id,
        listen: "127.0.0.1:7001".parse().unwrap(),
        peers: [(2, "http://127.0.0.1:7002".to_string())].into_iter().collect(),
        data_dir: std::path::PathBuf::from("/tmp/n"),
        tick: std::time::Duration::from_millis(20),
        election_timeout: 15,
        heartbeat_interval: 3,
    }
}

#[test]
fn the_raft_log_and_the_state_machine_get_separate_directories() {
    let c = cfg(1);
    assert_ne!(c.raft_dir(), c.state_dir(), "one Bitcask directory cannot hold both");
    assert!(c.raft_dir().starts_with(&c.data_dir));
    assert!(c.state_dir().starts_with(&c.data_dir));
}

/// Identical seeds make every node draw the identical election timeout, so
/// they all campaign on the same tick, split the vote, and do it again. The
/// cluster then never elects anyone, and it looks like a network fault.
#[test]
fn nodes_get_different_election_seeds() {
    assert_ne!(cfg(1).raft_config().seed, cfg(2).raft_config().seed);
    assert_ne!(cfg(2).raft_config().seed, cfg(3).raft_config().seed);
}

#[test]
fn the_raft_config_carries_the_peers_but_not_ourselves() {
    let c = cfg(1);
    let raft = c.raft_config();
    assert_eq!(raft.id, 1);
    assert_eq!(raft.peers, vec![2]);
    assert_eq!(raft.cluster_size(), 2);
}

/// Well below the election timeout, or a healthy leader is deposed by its own
/// followers between heartbeats.
#[test]
fn the_heartbeat_is_well_inside_the_election_timeout() {
    let c = cfg(1);
    assert!(c.heartbeat_interval * 3 <= c.election_timeout);
}
```

Add `mod config;` to `crates/kv-node/src/tests/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-node -E 'test(config)'`
Expected: `unresolved import crate::config`.

- [ ] **Step 3: Implement**

`crates/kv-node/src/config.rs`:

```rust
//! Node configuration and CLI (M6).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use kv_raft::NodeId;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub listen: SocketAddr,
    /// Peer id -> gRPC endpoint. Excludes ourselves.
    pub peers: BTreeMap<NodeId, String>,
    pub data_dir: PathBuf,
    /// Wall-clock length of one Raft logical tick.
    pub tick: Duration,
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
}

impl NodeConfig {
    /// The Raft log. Separate from the state machine: they are two Bitcask
    /// instances with independent key spaces, and sharing a directory would
    /// have entry keys and user keys colliding in one keydir.
    pub fn raft_dir(&self) -> PathBuf {
        self.data_dir.join("raft")
    }

    pub fn state_dir(&self) -> PathBuf {
        self.data_dir.join("state")
    }

    pub fn raft_config(&self) -> kv_raft::Config {
        kv_raft::Config {
            id: self.id,
            peers: self.peers.keys().copied().collect(),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            // Derived from the id, never a constant: identical seeds make
            // every node draw the identical timeout, campaign on the same
            // tick, split the vote and repeat. That livelock is silent and
            // looks like a network fault.
            seed: self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "kv-node", about = "A Bohime storage node")]
pub struct Args {
    /// This node's id within the Raft group.
    #[arg(long)]
    pub id: NodeId,

    /// Address to serve RaftService and KvService on.
    #[arg(long, default_value = "127.0.0.1:7001")]
    pub listen: SocketAddr,

    /// A peer, as `id=endpoint`. Repeat once per peer.
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<(NodeId, String)>,

    /// Directory for the Raft log and the state machine.
    #[arg(long)]
    pub data_dir: PathBuf,

    #[arg(long, default_value_t = 20)]
    pub tick_ms: u64,

    /// Election timeout in ticks; the real one is drawn from [t, 2t).
    #[arg(long, default_value_t = 15)]
    pub election_timeout: u64,

    /// Ticks between leader heartbeats. Must stay well under the election
    /// timeout or a healthy leader is deposed between its own heartbeats.
    #[arg(long, default_value_t = 3)]
    pub heartbeat_interval: u64,
}

fn parse_peer(s: &str) -> Result<(NodeId, String), String> {
    let (id, addr) = s.split_once('=').ok_or_else(|| format!("expected id=endpoint, got {s:?}"))?;
    let id: NodeId = id.parse().map_err(|_| format!("bad peer id {id:?}"))?;
    Ok((id, addr.to_string()))
}

impl Args {
    pub fn into_config(self) -> anyhow::Result<NodeConfig> {
        let mut peers = BTreeMap::new();
        for (id, addr) in self.peers {
            if id == self.id {
                anyhow::bail!("node {id} cannot be its own peer");
            }
            if peers.insert(id, addr).is_some() {
                anyhow::bail!("peer {id} given twice");
            }
        }
        Ok(NodeConfig {
            id: self.id,
            listen: self.listen,
            peers,
            data_dir: self.data_dir,
            tick: Duration::from_millis(self.tick_ms),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
        })
    }
}
```

Add `mod config;` to `crates/kv-node/src/main.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-node -E 'test(config)'` — four pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-node/src/config.rs crates/kv-node/src/tests/
git commit -m "M6: node config and CLI args, with per-node election seeds"
```

---

## Task 6: the driver loop

**Files:**
- Create: `crates/kv-node/src/driver.rs`
- Create: `crates/kv-node/src/tests/driver.rs`
- Modify: `crates/kv-node/src/main.rs`, `crates/kv-node/src/tests/mod.rs`

The heart of the milestone. One task owns the `RaftNode` and the state-machine
`Engine` and is the only thing that touches either; everything else reaches it
over a bounded channel and waits on a `oneshot`.

**Consumes:** `Command` (task 3), `BitcaskStorage::sync` (task 4),
`NodeConfig` (task 5), `RaftNode::leader_id` (task 1), and from M5:
`transport::server::Inbound { from, msg, reply }`, `transport::peer::PeerClient`.

**Produces:**
```rust
pub enum ClientOp { Get { key: Vec<u8> }, Put { key: Vec<u8>, value: Vec<u8> }, Delete { key: Vec<u8> } }
pub enum ClientReply { Value(Option<Vec<u8>>), Applied, NotLeader { hint: Option<NodeId> } }
pub struct ClientRequest { pub op: ClientOp, pub reply: oneshot::Sender<ClientReply> }

pub struct Driver { /* … */ }
impl Driver {
    pub fn new(
        config: &NodeConfig,
        node: RaftNode<BitcaskStorage>,
        engine: Engine,
        peers: BTreeMap<NodeId, PeerClient>,
        inbox: mpsc::Receiver<Inbound>,
        peer_replies: mpsc::Receiver<(NodeId, Message)>,
        requests: mpsc::Receiver<ClientRequest>,
    ) -> Self;
    pub async fn run(mut self) -> anyhow::Result<()>;
}
```

### The four orderings that matter

1. **Disk before network.** Every drain is: `sync()`, then answer the inbound
   RPC's `oneshot`, then push to peers, then apply. A vote that reaches the
   wire before it reaches the disk lets a crash produce a second vote in the
   same term, which breaks election safety.
2. **The inbound reply is a send.** M5's `Inbound` carries a `oneshot` because
   the proto is request/response. That reply is the *first* message in
   `ready.messages` addressed back to `from` — inbound is only ever
   `RequestVote` or `AppendEntries`, and each produces exactly one response to
   its sender. It is subject to rule 1 like any other send.
3. **A pending client request is keyed by index *and* term.** If the entry
   that finally commits at that index carries a different term, ours was
   overwritten by a new leader and never committed. Answering `Applied` there
   is a lost write reported as a success — the single worst bug available in
   this milestone.
4. **Losing leadership fails everything pending.** Entries proposed as leader
   may never commit once we step down, and the client would otherwise hang
   until its deadline.

### Restart re-applies

`RaftNode::new` sets `last_applied = 0` and takes `commit_index` from
`HardState`, so a restart re-applies everything up to the commit index into
the state machine. That is safe here because `Put`/`Delete` are idempotent,
and it is *why* the state machine may only ever contain idempotent operations
until M8's snapshots persist an applied index. Write this down in the code.

- [ ] **Step 1: Write the failing tests**

`crates/kv-node/src/tests/driver.rs`:

```rust
//! The driver, exercised as a group of one: no sockets, no peers, and the
//! whole tick/persist/send/apply cycle still runs. Task 2 in kv-raft is what
//! makes a lone node commit, so this is a real end-to-end path, not a stub.

use std::collections::BTreeMap;
use std::time::Duration;

use kv_raft::RaftNode;
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::config::NodeConfig;
use crate::driver::{ClientOp, ClientReply, ClientRequest, Driver};
use crate::storage::BitcaskStorage;

struct Harness {
    requests: mpsc::Sender<ClientRequest>,
    _dir: tempfile::TempDir,
}

impl Harness {
    fn solo() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: 1,
            listen: "127.0.0.1:0".parse().unwrap(),
            peers: BTreeMap::new(),
            data_dir: dir.path().to_path_buf(),
            tick: Duration::from_millis(5),
            election_timeout: 4,
            heartbeat_interval: 1,
        };
        std::fs::create_dir_all(config.raft_dir()).unwrap();
        std::fs::create_dir_all(config.state_dir()).unwrap();

        let node = RaftNode::new(
            config.raft_config(),
            BitcaskStorage::open(config.raft_dir()).unwrap(),
        );
        let engine = Engine::open(config.state_dir()).unwrap();

        let (_inbox_tx, inbox) = mpsc::channel(8);
        let (_replies_tx, replies) = mpsc::channel(8);
        let (requests, requests_rx) = mpsc::channel(8);

        let driver = Driver::new(&config, node, engine, BTreeMap::new(), inbox, replies, requests_rx);
        tokio::spawn(driver.run());
        // Leak the senders so the driver's select arms never see a closed
        // channel and shut down mid-test.
        std::mem::forget((_inbox_tx, _replies_tx));

        Self { requests, _dir: dir }
    }

    async fn call(&self, op: ClientOp) -> ClientReply {
        let (reply, wait) = oneshot::channel();
        self.requests.send(ClientRequest { op, reply }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), wait).await.expect("driver answers").unwrap()
    }
}

#[tokio::test]
async fn a_put_is_committed_then_readable() {
    let h = Harness::solo();
    let reply = h
        .call(ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() })
        .await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");

    let reply = h.call(ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(Some(v)) if v == b"v"), "got {reply:?}");
}

#[tokio::test]
async fn a_delete_removes_the_key() {
    let h = Harness::solo();
    h.call(ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }).await;
    h.call(ClientOp::Delete { key: b"k".to_vec() }).await;

    let reply = h.call(ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(None)), "got {reply:?}");
}

#[tokio::test]
async fn a_missing_key_reads_as_none() {
    let h = Harness::solo();
    let reply = h.call(ClientOp::Get { key: b"absent".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(None)), "got {reply:?}");
}

/// Everything the client was told was `Applied` must survive losing the
/// process, because that is the only promise a write ever made.
#[tokio::test]
async fn applied_writes_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    // Written as one config so both runs open the same directories.
    let config = NodeConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().unwrap(),
        peers: BTreeMap::new(),
        data_dir: dir.path().to_path_buf(),
        tick: Duration::from_millis(5),
        election_timeout: 4,
        heartbeat_interval: 1,
    };
    std::fs::create_dir_all(config.raft_dir()).unwrap();
    std::fs::create_dir_all(config.state_dir()).unwrap();

    async fn spawn(config: &NodeConfig) -> mpsc::Sender<ClientRequest> {
        let node = RaftNode::new(
            config.raft_config(),
            BitcaskStorage::open(config.raft_dir()).unwrap(),
        );
        let engine = Engine::open(config.state_dir()).unwrap();
        let (inbox_tx, inbox) = mpsc::channel(8);
        let (replies_tx, replies) = mpsc::channel(8);
        let (requests, requests_rx) = mpsc::channel(8);
        let driver =
            Driver::new(config, node, engine, BTreeMap::new(), inbox, replies, requests_rx);
        tokio::spawn(driver.run());
        std::mem::forget((inbox_tx, replies_tx));
        requests
    }

    async fn call(tx: &mpsc::Sender<ClientRequest>, op: ClientOp) -> ClientReply {
        let (reply, wait) = oneshot::channel();
        tx.send(ClientRequest { op, reply }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), wait).await.expect("answered").unwrap()
    }

    let first = spawn(&config).await;
    let reply = call(&first, ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Applied));
    drop(first);

    let second = spawn(&config).await;
    let reply = call(&second, ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(Some(v)) if v == b"v"), "got {reply:?}");
}

/// A node that does not lead must refuse the write and name who does, rather
/// than accept it locally. Three configured peers that never answer means
/// this node never wins an election.
#[tokio::test]
async fn a_follower_refuses_a_write_instead_of_applying_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = NodeConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().unwrap(),
        peers: [(2, "http://127.0.0.1:1".to_string()), (3, "http://127.0.0.1:2".to_string())]
            .into_iter()
            .collect(),
        data_dir: dir.path().to_path_buf(),
        tick: Duration::from_millis(5),
        election_timeout: 4,
        heartbeat_interval: 1,
    };
    std::fs::create_dir_all(config.raft_dir()).unwrap();
    std::fs::create_dir_all(config.state_dir()).unwrap();

    let node =
        RaftNode::new(config.raft_config(), BitcaskStorage::open(config.raft_dir()).unwrap());
    let engine = Engine::open(config.state_dir()).unwrap();
    let (inbox_tx, inbox) = mpsc::channel(8);
    let (replies_tx, replies) = mpsc::channel(8);
    let (requests, requests_rx) = mpsc::channel(8);
    let driver = Driver::new(&config, node, engine, BTreeMap::new(), inbox, replies, requests_rx);
    tokio::spawn(driver.run());
    std::mem::forget((inbox_tx, replies_tx));

    let (reply, wait) = oneshot::channel();
    requests
        .send(ClientRequest {
            op: ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() },
            reply,
        })
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), wait).await.unwrap().unwrap();
    assert!(matches!(reply, ClientReply::NotLeader { .. }), "got {reply:?}");
}
```

Add `mod driver;` to `crates/kv-node/src/tests/mod.rs`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-node -E 'test(driver)'`
Expected: `unresolved import crate::driver`.

- [ ] **Step 3: Implement**

`crates/kv-node/src/driver.rs`:

```rust
//! The driver loop (M6): the one task that owns the Raft node and the state
//! machine, and the only thing that touches either.
//!
//! Everything else in the process — the two gRPC services, the per-peer
//! clients — reaches it over a bounded channel and waits on a `oneshot`.
//! That is what keeps `RaftNode` single-threaded and synchronous, which is
//! the property M4's determinism rests on, while the shell around it is fully
//! async.
//!
//! Every drain runs in one order, and the order is a correctness requirement
//! rather than an optimisation (§1.5): **sync the log, then send, then
//! apply.** A vote that reaches the wire before it reaches the disk lets a
//! crash produce a second vote in the same term, and election safety is gone.

use std::collections::BTreeMap;
use std::time::Duration;

use kv_raft::{LogIndex, Message, NodeId, ProposeError, RaftNode, Role, Term};
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::command::Command;
use crate::config::NodeConfig;
use crate::storage::BitcaskStorage;
use crate::transport::peer::PeerClient;
use crate::transport::server::Inbound;

#[derive(Debug)]
pub enum ClientOp {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub enum ClientReply {
    Value(Option<Vec<u8>>),
    Applied,
    NotLeader { hint: Option<NodeId> },
}

#[derive(Debug)]
pub struct ClientRequest {
    pub op: ClientOp,
    pub reply: oneshot::Sender<ClientReply>,
}

/// A client waiting on the entry it proposed.
struct Pending {
    /// The term the entry was proposed in. If the entry that finally commits
    /// at this index carries a different term, a new leader overwrote ours
    /// and it never committed — answering `Applied` there would report a lost
    /// write as a success.
    term: Term,
    reply: oneshot::Sender<ClientReply>,
}

pub struct Driver {
    node: RaftNode<BitcaskStorage>,
    /// The state machine. A second Bitcask instance, in its own directory.
    engine: Engine,
    peers: BTreeMap<NodeId, PeerClient>,
    inbox: mpsc::Receiver<Inbound>,
    peer_replies: mpsc::Receiver<(NodeId, Message)>,
    requests: mpsc::Receiver<ClientRequest>,
    pending: BTreeMap<LogIndex, Pending>,
    tick: Duration,
}

impl Driver {
    pub fn new(
        config: &NodeConfig,
        node: RaftNode<BitcaskStorage>,
        engine: Engine,
        peers: BTreeMap<NodeId, PeerClient>,
        inbox: mpsc::Receiver<Inbound>,
        peer_replies: mpsc::Receiver<(NodeId, Message)>,
        requests: mpsc::Receiver<ClientRequest>,
    ) -> Self {
        Self {
            node,
            engine,
            peers,
            inbox,
            peer_replies,
            requests,
            pending: BTreeMap::new(),
            tick: config.tick,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.tick);
        // A stalled drain must not make the loop try to catch up on missed
        // ticks in a burst: that would fire several elections' worth of
        // timeout in one pass.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let mut answer: Option<(NodeId, oneshot::Sender<Message>)> = None;

            tokio::select! {
                _ = ticker.tick() => {
                    self.node.tick();
                }
                Some(Inbound { from, msg, reply }) = self.inbox.recv() => {
                    self.node.step(from, msg);
                    answer = Some((from, reply));
                }
                Some((peer, msg)) = self.peer_replies.recv() => {
                    self.node.step(peer, msg);
                }
                Some(request) = self.requests.recv() => {
                    self.handle_request(request);
                }
                else => return Ok(()),
            }

            self.drain(answer)?;
        }
    }

    fn handle_request(&mut self, ClientRequest { op, reply }: ClientRequest) {
        let command = match op {
            // The read is deliberately local and therefore deliberately
            // **stale**: a deposed leader that has not yet heard about the
            // election will happily serve its old value. That is expected at
            // M6 and is fixed at M7 with ReadIndex — do not mistake this for
            // a finished path.
            ClientOp::Get { key } => {
                let value = self.engine.get(&key).ok().flatten();
                let _ = reply.send(ClientReply::Value(value));
                return;
            }
            ClientOp::Put { key, value } => Command::Put { key, value },
            ClientOp::Delete { key } => Command::Delete { key },
        };

        match self.node.propose(command.encode()) {
            Ok(index) => {
                self.pending.insert(index, Pending { term: self.node.current_term(), reply });
            }
            Err(ProposeError::NotLeader) => {
                let _ = reply.send(ClientReply::NotLeader { hint: self.node.leader_id() });
            }
        }
    }

    /// Executes one `Ready`. See the module header for why the order is what
    /// it is.
    fn drain(&mut self, answer: Option<(NodeId, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
        let mut ready = self.node.ready();

        // 1. Disk. `RaftNode` already wrote through to storage inside
        //    `step`/`propose`; this is what makes it durable, and it must
        //    happen before anything leaves this process.
        if !ready.entries.is_empty() || ready.hard_state.is_some() {
            self.node.storage().sync()?;
        }

        // 2. The inbound RPC's reply, which is a send like any other. Inbound
        //    is only ever RequestVote or AppendEntries and each produces
        //    exactly one response addressed back to its sender, so taking the
        //    first such message is exact rather than a heuristic.
        if let Some((from, channel)) = answer {
            if let Some(i) = ready.messages.iter().position(|(to, _)| *to == from) {
                let (_, msg) = ready.messages.remove(i);
                let _ = channel.send(msg);
            }
        }

        // 3. Everything else, shed on a full queue (M5's decision).
        for (to, msg) in ready.messages {
            if let Some(peer) = self.peers.get(&to) {
                let _ = peer.try_send(msg);
            }
        }

        // 4. Apply.
        for entry in ready.committed {
            match Command::decode(&entry.command) {
                // The leader's no-op. Committed and applied like any entry,
                // and it means nothing to the state machine.
                Ok(None) => {}
                Ok(Some(Command::Put { key, value })) => {
                    self.engine.put(&key, &value)?;
                }
                Ok(Some(Command::Delete { key })) => {
                    self.engine.delete(&key)?;
                }
                Err(e) => {
                    // A committed entry we cannot decode means the log and
                    // this binary disagree about the state machine. Applying
                    // past it would diverge replicas silently.
                    anyhow::bail!("undecodable committed entry at index {}: {e}", entry.index);
                }
            }

            if let Some(pending) = self.pending.remove(&entry.index) {
                let reply = if pending.term == entry.term {
                    ClientReply::Applied
                } else {
                    // Our entry was overwritten by a later leader before it
                    // committed. The write did not happen.
                    ClientReply::NotLeader { hint: self.node.leader_id() }
                };
                let _ = pending.reply.send(reply);
            }
        }

        // 5. If we are no longer the leader, nothing still pending will ever
        //    commit under us. Answering now beats making the client wait for
        //    its deadline.
        if self.node.role() != Role::Leader && !self.pending.is_empty() {
            let hint = self.node.leader_id();
            for (_, pending) in std::mem::take(&mut self.pending) {
                let _ = pending.reply.send(ClientReply::NotLeader { hint });
            }
        }

        Ok(())
    }
}
```

This needs one accessor on `RaftNode`, since the driver must reach its storage
to sync it. Add to `crates/kv-raft/src/node.rs`:

```rust
    /// Borrow the storage, for a driver that must flush it (§1.5). Read-only:
    /// mutating the log behind the core's back would desynchronise
    /// `last_index`.
    pub fn storage(&self) -> &S {
        &self.storage
    }
```

Add `mod driver;` to `crates/kv-node/src/main.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-node -E 'test(driver)'` — five pass.

If `a_put_is_committed_then_readable` hangs, the cause is almost certainly
task 2: check `node.commit_index()` advances for a lone node before looking
anywhere else.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-raft/src/node.rs crates/kv-node/src/driver.rs crates/kv-node/src/tests/
git commit -m "M6: the driver loop — tick, persist, send, apply"
```

---

## Task 7: `KvService`

**Files:**
- Create: `crates/kv-node/src/kv_service.rs`
- Modify: `proto/kv.proto`, `crates/kv-node/src/main.rs`

**Consumes:** `ClientRequest`/`ClientOp`/`ClientReply` from task 6.

`GetResponse` has no `not_leader` case in practice at M6 — reads are served
locally from any node, which is exactly the gate ("get k on any node returns
it") and exactly the staleness M7 fixes. The field stays in the proto because
M7 will use it.

`proto/kv.proto` needs one addition: `GetResponse` must distinguish "no such
key" from "key holds an empty value". `optional bytes value` already does
that — `None` is absent, `Some(vec![])` is an empty value. Verify the
generated type is `Option<Vec<u8>>` and leave the proto alone if so.

- [ ] **Step 1: Write the failing test**

Extend `crates/kv-node/src/tests/driver.rs` with a service-level test that
serves `KvService` over a loopback socket and drives a real solo `Driver`
behind it — same shape as M5's `serve` helper in `tests/transport.rs`:

```rust
#[tokio::test]
async fn kv_service_puts_and_gets_over_a_real_socket() {
    use kv_proto::kv::kv_service_client::KvServiceClient;
    use kv_proto::kv::kv_service_server::KvServiceServer;
    use kv_proto::kv::{GetRequest, PutRequest};

    let h = Harness::solo();
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let service = crate::kv_service::KvApi::new(h.requests.clone());
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServiceServer::new(service))
            .serve(addr)
            .await
    });

    let mut client = loop {
        match KvServiceClient::connect(format!("http://127.0.0.1:{port}")).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };

    let resp = client
        .put(PutRequest { ctx: None, key: b"k".to_vec(), value: b"v".to_vec() })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.not_leader.is_none(), "the lone node leads");

    let resp = client.get(GetRequest { key: b"k".to_vec() }).await.unwrap().into_inner();
    assert_eq!(resp.value, Some(b"v".to_vec()));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-node -E 'test(kv_service_puts)'`
Expected: `unresolved import crate::kv_service`.

- [ ] **Step 3: Implement**

`crates/kv-node/src/kv_service.rs`:

```rust
//! The client-facing gRPC service (M6). Every handler turns its request into
//! a `ClientRequest`, hands it to the driver and waits on a `oneshot` — the
//! driver is the only thing allowed to touch the Raft node or the state
//! machine.

#![allow(clippy::result_large_err)]

use kv_proto::kv as pb;
use kv_proto::kv::kv_service_server::KvService;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::driver::{ClientOp, ClientReply, ClientRequest};

pub struct KvApi {
    requests: mpsc::Sender<ClientRequest>,
}

impl KvApi {
    pub fn new(requests: mpsc::Sender<ClientRequest>) -> Self {
        Self { requests }
    }

    /// A full queue is shed as `resource_exhausted` rather than awaited, for
    /// the same reason the Raft inbox sheds (M5): blocking here would let a
    /// slow driver pin every inbound connection.
    async fn call(&self, op: ClientOp) -> Result<ClientReply, Status> {
        let (reply, wait) = oneshot::channel();
        self.requests
            .try_send(ClientRequest { op, reply })
            .map_err(|_| Status::resource_exhausted("client request queue full"))?;
        wait.await.map_err(|_| Status::unavailable("node is shutting down"))
    }
}

fn not_leader(hint: Option<u64>) -> pb::NotLeader {
    // 0 is not a valid node id, so it doubles as "no leader known". Node ids
    // start at 1 throughout — see `NodeConfig`.
    pb::NotLeader { leader_hint: hint.unwrap_or(0) }
}

#[tonic::async_trait]
impl KvService for KvApi {
    /// **Deliberately stale (M6).** This reads local state, so a deposed
    /// leader that has not yet learned it lost will serve its old value. The
    /// gate for this milestone is "get k on any node returns it", and M7
    /// replaces this with ReadIndex. Do not mistake it for a finished path.
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        match self.call(ClientOp::Get { key: request.into_inner().key }).await? {
            ClientReply::Value(value) => Ok(Response::new(pb::GetResponse { value, not_leader: None })),
            other => Err(Status::internal(format!("driver answered a get with {other:?}"))),
        }
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let req = request.into_inner();
        match self.call(ClientOp::Put { key: req.key, value: req.value }).await? {
            ClientReply::Applied => Ok(Response::new(pb::PutResponse { not_leader: None })),
            ClientReply::NotLeader { hint } => {
                Ok(Response::new(pb::PutResponse { not_leader: Some(not_leader(hint)) }))
            }
            other => Err(Status::internal(format!("driver answered a put with {other:?}"))),
        }
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        match self.call(ClientOp::Delete { key: request.into_inner().key }).await? {
            ClientReply::Applied => Ok(Response::new(pb::DeleteResponse { not_leader: None })),
            ClientReply::NotLeader { hint } => {
                Ok(Response::new(pb::DeleteResponse { not_leader: Some(not_leader(hint)) }))
            }
            other => Err(Status::internal(format!("driver answered a delete with {other:?}"))),
        }
    }

    /// Compare-and-swap needs the session table to be idempotent under retry,
    /// which is M7. Refused rather than half-implemented: a `Cas` that
    /// double-applies on retry is worse than one that is absent.
    async fn cas(
        &self,
        _request: Request<pb::CasRequest>,
    ) -> Result<Response<pb::CasResponse>, Status> {
        Err(Status::unimplemented("Cas arrives at M7, with the session table"))
    }
}
```

Add `mod kv_service;` to `crates/kv-node/src/main.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-node` — green.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-node/src/kv_service.rs crates/kv-node/src/tests/driver.rs crates/kv-node/src/main.rs
git commit -m "M6: KvService over the driver, with NotLeader hints"
```

---

## Task 8: wire `main.rs`

**Files:**
- Modify: `crates/kv-node/src/main.rs`, `crates/kv-node/src/transport/mod.rs`

- [ ] **Step 1: Implement**

Both services share one listener — there is no reason to make an operator
manage two ports for one process.

```rust
//! `kv-node`: the impure shell binding kv-storage + kv-raft to real
//! tokio/tonic I/O (M6).

mod command;
mod config;
mod driver;
mod kv_service;
mod storage;
mod transport;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use clap::Parser;
use kv_proto::kv::kv_service_server::KvServiceServer;
use kv_proto::raft::raft_service_server::RaftServiceServer;
use kv_raft::RaftNode;
use kv_storage::Engine;
use tokio::sync::mpsc;

use crate::config::Args;
use crate::driver::Driver;
use crate::kv_service::KvApi;
use crate::storage::BitcaskStorage;
use crate::transport::peer::{PeerClient, PeerConfig};
use crate::transport::server::RaftServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Args::parse().into_config()?;
    std::fs::create_dir_all(config.raft_dir())?;
    std::fs::create_dir_all(config.state_dir())?;

    let node = RaftNode::new(config.raft_config(), BitcaskStorage::open(config.raft_dir())?);
    let engine = Engine::open(config.state_dir())?;

    let (inbox_tx, inbox) = mpsc::channel(256);
    let (replies_tx, replies) = mpsc::channel(256);
    let (requests_tx, requests) = mpsc::channel(256);

    let mut peers = BTreeMap::new();
    for (&id, addr) in &config.peers {
        peers.insert(
            id,
            PeerClient::connect(id, addr.clone(), PeerConfig::default(), replies_tx.clone()),
        );
    }

    tracing::info!(id = config.id, listen = %config.listen, peers = peers.len(), "kv-node starting");

    let driver = Driver::new(&config, node, engine, peers, inbox, replies, requests);
    tokio::spawn(async move {
        if let Err(e) = driver.run().await {
            tracing::error!(error = %e, "driver stopped");
        }
    });

    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServer::new(inbox_tx)))
        .add_service(KvServiceServer::new(KvApi::new(requests_tx)))
        .serve(config.listen)
        .await?;
    Ok(())
}
```

Delete the `#![allow(dead_code)]` from `crates/kv-node/src/transport/mod.rs`:
the driver now constructs everything in there, and the allow was explicitly
scoped "until M6". Keep the `result_large_err` allow.

- [ ] **Step 2: Verify it builds clean and runs**

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p kv-node -- --id 1 --listen 127.0.0.1:7001 --data-dir /tmp/bohime-n1
```

Expected: a lone node logs `kv-node starting`, elects itself within a second
and idles. `Ctrl-C` to stop. Remove `/tmp/bohime-n1` afterwards.

- [ ] **Step 3: Commit**

```bash
git add crates/kv-node/src/main.rs crates/kv-node/src/transport/mod.rs
git commit -m "M6: wire the node binary — one listener, both services"
```

---

## Task 9: `kv-client`

**Files:**
- Create: `crates/kv-client/src/lib.rs`, `crates/kv-client/src/cli.rs`
- Modify: `crates/kv-client/src/main.rs`, `crates/kv-client/Cargo.toml`,
  `Cargo.toml`, `crates/kv-client/src/tests/mod.rs`
- Create: `crates/kv-client/src/tests/client.rs`

`kv-client` becomes lib + bin. The `#[cfg(test)] mod tests;` moves from
`main.rs` to `lib.rs`, per the crate layout convention.

Add `kv-client = { path = "crates/kv-client" }` to the workspace
`[workspace.dependencies]`; it is missing today. `kv-client` needs
`thiserror.workspace = true`.

**Produces:**
```rust
pub struct Client { /* endpoints by id, cached leader, backoff */ }
impl Client {
    pub fn new(endpoints: Vec<(NodeId, String)>) -> Self;
    pub fn leader(&self) -> Option<NodeId>;              // last node that accepted a write
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError>;
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError>;
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), ClientError>;
}
pub enum ClientError { NoReachableNode, Rpc(tonic::Status), Transport(tonic::transport::Error) }
```

`leader()` is what the end-to-end test uses to decide which process to
`kill -9`. That avoids needing an admin RPC just to run the gate.

**Retry policy.** On `NotLeader { leader_hint }`: if the hint names a known
node, go there next; otherwise round-robin. On a transport error: round-robin
past the dead node. Bounded attempts (say 20) with a short backoff between
rounds, so a cluster with no leader fails rather than hangs.

- [ ] **Step 1: Write the failing test**

`crates/kv-client/src/tests/client.rs`:

```rust
use kv_client::{Client, ClientError};

#[tokio::test]
async fn an_unreachable_cluster_fails_rather_than_hanging() {
    let mut client = Client::new(vec![
        (1, "http://127.0.0.1:1".to_string()),
        (2, "http://127.0.0.1:2".to_string()),
    ]);
    let result =
        tokio::time::timeout(std::time::Duration::from_secs(20), client.put(b"k", b"v")).await;
    assert!(result.is_ok(), "it must give up, not hang");
    assert!(matches!(result.unwrap(), Err(ClientError::NoReachableNode)));
}

#[tokio::test]
async fn a_fresh_client_knows_no_leader() {
    let client = Client::new(vec![(1, "http://127.0.0.1:1".to_string())]);
    assert_eq!(client.leader(), None);
}
```

Replace `crates/kv-client/src/tests/mod.rs` contents with `mod client;` and
`mod wiring;`.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo nextest run -p kv-client`
Expected: `unresolved import kv_client` / no lib target.

- [ ] **Step 3: Implement**

`crates/kv-client/src/lib.rs`:

```rust
//! The smart client (M6): find the leader, follow its hints, retry.
//!
//! A write only the leader can accept, and the client does not know who that
//! is — so every call is a small search. `NotLeader { leader_hint }` turns it
//! from a scan into one redirect in the common case, and the cached leader
//! makes the *next* call start in the right place.
//!
//! The retry budget is bounded on purpose: a cluster with no quorum has no
//! leader to find, and a client that waits forever for one is
//! indistinguishable from a hung client.

use std::collections::BTreeMap;
use std::time::Duration;

use kv_proto::kv::kv_service_client::KvServiceClient;
use kv_proto::kv::{DeleteRequest, GetRequest, PutRequest};
use tonic::transport::Channel;

pub type NodeId = u64;

const RETRY_ROUNDS: usize = 20;
const RETRY_PAUSE: Duration = Duration::from_millis(50);

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no reachable node could serve the request")]
    NoReachableNode,
    #[error("rpc failed: {0}")]
    Rpc(#[from] tonic::Status),
}

pub struct Client {
    endpoints: BTreeMap<NodeId, String>,
    /// Connected channels, kept so a retry does not redial. tonic's `Channel`
    /// is cheap to clone and reconnects on its own.
    channels: BTreeMap<NodeId, Channel>,
    /// The last node that accepted a write. Also what the M6 end-to-end gate
    /// uses to decide which process to `kill -9`.
    leader: Option<NodeId>,
}

impl Client {
    pub fn new(endpoints: Vec<(NodeId, String)>) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
            channels: BTreeMap::new(),
            leader: None,
        }
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The order to try nodes in: the cached leader first, then everyone else.
    fn candidates(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.endpoints.keys().copied().collect();
        if let Some(leader) = self.leader {
            ids.retain(|id| *id != leader);
            ids.insert(0, leader);
        }
        ids
    }

    async fn channel(&mut self, id: NodeId) -> Option<Channel> {
        if let Some(c) = self.channels.get(&id) {
            return Some(c.clone());
        }
        let endpoint = self.endpoints.get(&id)?;
        let channel = Channel::from_shared(endpoint.clone())
            .ok()?
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_secs(2))
            .connect()
            .await
            .ok()?;
        self.channels.insert(id, channel.clone());
        Some(channel)
    }

    /// Drops a channel that failed, so the next attempt redials rather than
    /// reusing a socket to a process that is gone.
    fn forget(&mut self, id: NodeId) {
        self.channels.remove(&id);
        if self.leader == Some(id) {
            self.leader = None;
        }
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        // Reads are served locally by any node at M6, so the first node that
        // answers wins. That is also why this read can be stale — M7.
        for _ in 0..RETRY_ROUNDS {
            for id in self.candidates() {
                let Some(channel) = self.channel(id).await else { continue };
                match KvServiceClient::new(channel)
                    .get(GetRequest { key: key.to_vec() })
                    .await
                {
                    Ok(resp) => return Ok(resp.into_inner().value),
                    Err(_) => self.forget(id),
                }
            }
            tokio::time::sleep(RETRY_PAUSE).await;
        }
        Err(ClientError::NoReachableNode)
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        self.write(|client| {
            let (key, value) = (key.to_vec(), value.to_vec());
            Box::pin(async move {
                client.put(PutRequest { ctx: None, key, value }).await.map(|r| {
                    r.into_inner().not_leader.map(|n| n.leader_hint)
                })
            })
        })
        .await
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        self.write(|client| {
            let key = key.to_vec();
            Box::pin(async move {
                client.delete(DeleteRequest { ctx: None, key }).await.map(|r| {
                    r.into_inner().not_leader.map(|n| n.leader_hint)
                })
            })
        })
        .await
    }

    /// One write, retried until a node accepts it or the budget runs out.
    ///
    /// `call` returns `Ok(None)` when the node applied it, `Ok(Some(hint))`
    /// when it refused as a non-leader, and `Err` on a transport failure.
    async fn write<F>(&mut self, mut call: F) -> Result<(), ClientError>
    where
        F: for<'a> FnMut(
            &'a mut KvServiceClient<Channel>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Option<u64>, tonic::Status>> + Send + 'a>,
        >,
    {
        let mut next: Option<NodeId> = self.leader;

        for _ in 0..RETRY_ROUNDS {
            let order = match next.take() {
                Some(hinted) if self.endpoints.contains_key(&hinted) => vec![hinted],
                _ => self.candidates(),
            };

            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                let mut client = KvServiceClient::new(channel);
                match call(&mut client).await {
                    Ok(None) => {
                        self.leader = Some(id);
                        return Ok(());
                    }
                    // Refused as a non-leader. A hint of 0 means the node
                    // knows of no leader either, so fall back to scanning.
                    Ok(Some(hint)) => {
                        if hint != 0 && self.endpoints.contains_key(&hint) {
                            next = Some(hint);
                        }
                        self.leader = None;
                    }
                    Err(_) => self.forget(id),
                }
            }
            tokio::time::sleep(RETRY_PAUSE).await;
        }
        Err(ClientError::NoReachableNode)
    }
}

#[cfg(test)]
mod tests;
```

If the `for<'a> FnMut` bound above fights the borrow checker, collapse `put`
and `delete` into one enum-dispatched private method instead of a closure —
the retry logic is what matters here, not the abstraction over it.

`crates/kv-client/src/cli.rs`: clap with `--peer id=endpoint` (repeatable,
reusing the same `id=endpoint` shape as `kv-node`) and subcommands
`get <key>`, `put <key> <value>`, `delete <key>`:

```rust
use clap::{Parser, Subcommand};

use crate::{Client, NodeId};

#[derive(Debug, Parser)]
#[command(name = "kv-client", about = "A Bohime client")]
pub struct Args {
    /// A node, as `id=endpoint`. Repeat once per node.
    #[arg(long = "peer", value_parser = parse_peer, required = true)]
    pub peers: Vec<(NodeId, String)>,
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Debug, Subcommand)]
pub enum CliCommand {
    Get { key: String },
    Put { key: String, value: String },
    Delete { key: String },
}

fn parse_peer(s: &str) -> Result<(NodeId, String), String> {
    let (id, addr) = s.split_once('=').ok_or_else(|| format!("expected id=endpoint, got {s:?}"))?;
    let id: NodeId = id.parse().map_err(|_| format!("bad node id {id:?}"))?;
    Ok((id, addr.to_string()))
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let mut client = Client::new(args.peers);
    match args.command {
        CliCommand::Get { key } => match client.get(key.as_bytes()).await? {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => {
                eprintln!("(nil)");
                std::process::exit(1);
            }
        },
        CliCommand::Put { key, value } => {
            client.put(key.as_bytes(), value.as_bytes()).await?;
            println!("OK");
        }
        CliCommand::Delete { key } => {
            client.delete(key.as_bytes()).await?;
            println!("OK");
        }
    }
    Ok(())
}
```

`crates/kv-client/src/main.rs` becomes the thin binary:

```rust
use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kv_client::cli::run(kv_client::cli::Args::parse()).await
}
```

and `lib.rs` gains `pub mod cli;`. The `#[cfg(test)] mod tests;` moves from
`main.rs` to `lib.rs` — a binary and a library in one crate are two separate
crate roots, and the tests belong to the library.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo nextest run -p kv-client` — green.

- [ ] **Step 5: Commit**

```bash
git add crates/kv-client Cargo.toml
git commit -m "M6: kv-client library and CLI with leader-hint retry"
```

---

## Task 10: the gate — three processes and a `kill -9`

**Files:**
- Create: `crates/kv-node/src/tests/end_to_end.rs`
- Modify: `crates/kv-node/src/tests/mod.rs`, `crates/kv-node/Cargo.toml`

Add `kv-client = { workspace = true }` to `kv-node`'s `[dev-dependencies]`.
No cycle: `kv-client` depends on `kv-proto` and `kv-ring` only.

The roadmap places this at `crates/kv-node/tests/end_to_end.rs`. It goes in
`src/tests/` instead — the crate layout convention in `CLAUDE.md` has no
`crates/*/tests/` directories, and this test needs nothing public.

Locating the binary: `CARGO_BIN_EXE_*` is set for integration tests only, so
use `assert_cmd::cargo::cargo_bin("kv-node")`, which resolves from the test
executable's own target directory and works from a unit test.

- [ ] **Step 1: Write the failing test**

```rust
//! M6's gate: three real processes, a real socket, a real `kill -9`.
//!
//! Timeouts here are generous on purpose. This test asserts that a leader is
//! elected and that data survives, not how fast a loaded CI box gets there —
//! a tight bound would make it flaky, and a flaky gate is worse than none.

use std::process::{Child, Command};
use std::time::Duration;

use kv_client::Client;

struct Node {
    id: u64,
    child: Child,
    _dir: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn spawn_cluster(ports: &[u16]) -> Vec<Node> {
    let bin = assert_cmd::cargo::cargo_bin("kv-node");
    ports
        .iter()
        .enumerate()
        .map(|(i, &port)| {
            let id = i as u64 + 1;
            let dir = tempfile::tempdir().unwrap();
            let mut cmd = Command::new(&bin);
            cmd.arg("--id")
                .arg(id.to_string())
                .arg("--listen")
                .arg(format!("127.0.0.1:{port}"))
                .arg("--data-dir")
                .arg(dir.path());
            for (j, &peer_port) in ports.iter().enumerate() {
                if j != i {
                    cmd.arg("--peer").arg(format!("{}=http://127.0.0.1:{peer_port}", j + 1));
                }
            }
            Node { id, child: cmd.spawn().expect("kv-node starts"), _dir: dir }
        })
        .collect()
}

fn endpoints(ports: &[u16]) -> Vec<(u64, String)> {
    ports
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64 + 1, format!("http://127.0.0.1:{p}")))
        .collect()
}

#[tokio::test]
async fn three_processes_replicate_and_survive_killing_the_leader() {
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let mut cluster = spawn_cluster(&ports);
    let mut client = Client::new(endpoints(&ports));

    // The client retries through NotLeader and connection refusals, so a
    // successful put is also the signal that an election finished.
    client.put(b"alpha", b"one").await.expect("the cluster elects a leader and accepts a write");

    // Every node serves the value: M6 reads are local, which is the point of
    // "get k on any node returns it".
    for (id, endpoint) in endpoints(&ports) {
        let mut single = Client::new(vec![(id, endpoint)]);
        let value = single.get(b"alpha").await.expect("a read from every node");
        assert_eq!(value, Some(b"one".to_vec()), "node {id} did not have the write");
    }

    let leader = client.leader().expect("the write named a leader");
    let position = cluster.iter().position(|n| n.id == leader).expect("the leader is ours");

    // kill -9: no shutdown hook, no flush, nothing but what reached the disk.
    let mut victim = cluster.remove(position);
    victim.child.kill().unwrap();
    victim.child.wait().unwrap();
    drop(victim);

    let survivors = endpoints(&ports)
        .into_iter()
        .filter(|(id, _)| *id != leader)
        .collect::<Vec<_>>();
    let mut client = Client::new(survivors.clone());

    // A new leader, and the old leader's committed write is still there.
    let value = client.get(b"alpha").await.expect("a read after the kill");
    assert_eq!(value, Some(b"one".to_vec()), "a committed write did not survive the leader");

    client.put(b"beta", b"two").await.expect("the survivors elect a new leader and accept a write");
    for (id, endpoint) in survivors {
        let mut single = Client::new(vec![(id, endpoint)]);
        assert_eq!(single.get(b"beta").await.unwrap(), Some(b"two".to_vec()), "node {id}");
    }
}
```

Add `mod end_to_end;` to `crates/kv-node/src/tests/mod.rs`.

- [ ] **Step 2: Run it**

Run: `cargo nextest run -p kv-node -E 'test(three_processes)'`

If it fails, the order to check: is a leader elected at all (run three nodes
by hand and read the logs); does the client's `NotLeader` retry actually
follow the hint; does the survivors' election complete inside the client's
retry budget.

- [ ] **Step 3: Full verification**

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo nextest run -p kv-sim --run-ignored all -E 'test(election_sweep_full)'
```

All must pass. Then the 100k nemesis sweep once, because tasks 1 and 2
changed the Raft core (~80 min in debug):

```bash
cargo nextest run --release -p kv-sim --run-ignored all -E 'test(sweep_full)'
```

- [ ] **Step 4: Update the docs**

`CLAUDE.md` "Current state" — M5 and M6 are done; `kv-node` is no longer
transport-only; `kv-client` is no longer a placeholder. `README.md` still
says "Status: in progress (M2 of M13)"; it is M6, which is the first
milestone with a demo worth describing. Note both sweep gates' new passing
commit.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "M6: three-process end-to-end gate, kill -9 included"
```

---

## Gates

- ✅ Three processes; `put k v` on the leader, `get k` on any node returns it.
- ✅ `kill -9` the leader, a new one is elected, and the data is still there.
- ✅ `cargo nextest run --workspace`, clippy `-D warnings`, fmt check.
- ✅ Both kv-sim sweeps still pass — tasks 1 and 2 touched the Raft core.

## Deliberately not in M6

- **Reads are stale.** `Get` reads local state, so a deposed leader serves old
  values. M7's ReadIndex fixes it, and §M7 requires writing that test against
  *this* implementation first and watching it fail.
- **`Cas` returns `unimplemented`.** It needs the session table to survive a
  retry, which is M7.
- **No snapshots**, so a restart re-applies the whole log to the state
  machine. Safe only because `Put`/`Delete` are idempotent. M8.
- **No membership change**: the peer set is fixed at startup by CLI flags. M9.
- **One Raft group, one shard.** The ring, the shard map and multi-Raft are
  M10/M11.
