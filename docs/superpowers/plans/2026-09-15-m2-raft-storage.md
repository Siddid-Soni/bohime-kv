# M2 — Raft Persistent State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define the storage interface Raft needs, and prove one conformance suite passes against both an in-memory and a Bitcask-backed implementation — so M3 can develop entirely against the fast in-memory one without the production path silently drifting away from it.

**Architecture:** The `RaftStorage` trait and its `BTreeMap`-backed `MemStorage` live in `kv-raft` — a trait definition and a map are pure, so the no-I/O boundary holds. The Bitcask-backed impl lives in **`kv-node`**, because `kv-storage` must not know Raft exists and `kv-node` is the only crate that already depends on both. The conformance suite is a generic function in `kv-raft` behind a `testing` cargo feature, so both crates' tests run the identical assertions.

**Tech Stack:** Rust 2024, `serde` + `bincode` (entry and hard-state encoding), `thiserror` (error types), `kv-storage`'s `Engine` (the Bitcask backing store), `tempfile` + `proptest` (tests).

**Spec:** `docs/DESIGN.md` §M2 and §1.5 ("What must be on disk before you reply"); roadmap section M2 in `docs/superpowers/plans/2026-09-15-bohime-roadmap.md`.

## Global Constraints

- **`kv-raft` gains no dependency on `tokio`, `tonic`, `std::fs`, or `std::time` in this milestone or any later one.** After Task 2, `cargo tree -p kv-raft` must not list `tokio`. This is the boundary M4 is built on.
- **`kv-storage` gains no dependency on `kv-raft`.** If you find yourself adding one, the Bitcask storage impl has drifted into the wrong crate.
- Tests live in `src/tests/<filename>.rs`, never inline.
- Gates before every commit: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo nextest run --workspace`.
- Commit messages: `M2: <what landed>`.

## Log index conventions — fix these now, they leak into every later milestone

- **Log indices start at 1.** Index 0 is the sentinel for "before the log begins": `term(0)` is `Ok(Some(0))` and `last_index()` on an empty log is `Ok(0)`. This makes `prev_log_index = 0` in `AppendEntries` mean "no predecessor" without a special case, which is what the paper assumes.
- **`entries(lo, hi)` is half-open `[lo, hi)`**, matching Rust range convention. Every off-by-one in M3.4's consistency check traces back to this being ambiguous, so it is stated here and asserted in the conformance suite.
- **`append` requires contiguity.** The first entry must be at `last_index() + 1`; a gap is a programming error and returns `StorageError::Gap`, not a silent hole.
- **`truncate_suffix(from)` removes every entry with index `>= from`**, inclusive of `from`. It is a no-op when `from > last_index()`.

## File Structure

- Create `crates/kv-raft/src/types.rs` — `Term`, `LogIndex`, `NodeId`, `Entry`, `HardState`, `Snapshot`.
- Create `crates/kv-raft/src/storage.rs` — the `RaftStorage` trait, `StorageError`, `MemStorage`.
- Create `crates/kv-raft/src/testing.rs` — the conformance suite, behind the `testing` feature.
- Create `crates/kv-raft/src/tests/storage.rs`, `crates/kv-node/src/storage.rs`, `crates/kv-node/src/tests/storage.rs`.
- Modify `crates/kv-raft/src/lib.rs`, `crates/kv-raft/Cargo.toml`, `crates/kv-node/src/main.rs`, `crates/kv-node/Cargo.toml`.

Three small files rather than one `storage.rs`, because `testing.rs` is compiled conditionally and `types.rs` is consumed by every later module in the crate.

---

## Task 1: Core types

**Files:**
- Create: `crates/kv-raft/src/types.rs`, `crates/kv-raft/src/tests/types.rs`
- Modify: `crates/kv-raft/src/lib.rs`, `crates/kv-raft/Cargo.toml`

**Interfaces produced** (M3, M8, and M10 all consume these unchanged):
```rust
pub type Term = u64;
pub type LogIndex = u64;
pub type NodeId = u64;
pub struct Entry { pub term: Term, pub index: LogIndex, pub command: Vec<u8> }
pub struct HardState { pub term: Term, pub voted_for: Option<NodeId>, pub commit_index: LogIndex }
pub struct Snapshot { pub last_included_index: LogIndex, pub last_included_term: Term, pub data: Vec<u8> }
```

- [ ] **Step 1: Write the failing test**

Create `crates/kv-raft/src/tests/types.rs`:

```rust
use super::*;

#[test]
fn entry_round_trips_through_bincode() {
    let entry = Entry { term: 7, index: 42, command: vec![1, 2, 3] };
    let encoded = bincode::serialize(&entry).unwrap();
    let decoded: Entry = bincode::deserialize(&encoded).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn hard_state_round_trips_including_absent_vote() {
    for voted_for in [None, Some(3u64)] {
        let hs = HardState { term: 9, voted_for, commit_index: 12 };
        let decoded: HardState = bincode::deserialize(&bincode::serialize(&hs).unwrap()).unwrap();
        assert_eq!(hs, decoded);
    }
}

#[test]
fn default_hard_state_is_term_zero_no_vote() {
    // A node that has never voted must be distinguishable from one that voted
    // for node 0 — this is why voted_for is an Option and not a sentinel.
    let hs = HardState::default();
    assert_eq!(hs.term, 0);
    assert_eq!(hs.voted_for, None);
    assert_eq!(hs.commit_index, 0);
}

#[test]
fn snapshot_round_trips() {
    let snap = Snapshot { last_included_index: 100, last_included_term: 5, data: vec![9; 64] };
    let decoded: Snapshot = bincode::deserialize(&bincode::serialize(&snap).unwrap()).unwrap();
    assert_eq!(snap, decoded);
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo nextest run -p kv-raft
```

Expected: compile error — none of these types exist and `kv-raft` has no dependencies at all today.

- [ ] **Step 3: Add dependencies**

In `crates/kv-raft/Cargo.toml`:

```toml
[dependencies]
serde = { workspace = true, features = ["derive"] }
bincode.workspace = true
thiserror.workspace = true
tracing.workspace = true

[dev-dependencies]
proptest.workspace = true

[features]
# Exposes the RaftStorage conformance suite so other crates (kv-node) can run
# the identical assertions against their own impls. Off by default so test-only
# code never reaches a release build.
testing = []
```

Note what is **not** here: no `tokio`, no `tonic`, no `rand`. Randomized election timeouts arrive at M3.2 seeded from `Config`, not from a crate-level RNG.

- [ ] **Step 4: Write `types.rs`**

```rust
//! The vocabulary of the Raft core (M3.1). No behavior — these are the types
//! `RaftStorage`, `Message`, `Action`, and `Ready` are all expressed in.
//!
//! Log indices start at 1. Index 0 is the sentinel meaning "before the log
//! begins": `term(0)` is 0 and an empty log's `last_index()` is 0, so
//! `prev_log_index = 0` in an AppendEntries means "no predecessor" without a
//! special case anywhere.

use serde::{Deserialize, Serialize};

pub type Term = u64;
pub type LogIndex = u64;
pub type NodeId = u64;

/// One entry in the replicated log. `command` is opaque here — interpreting
/// it is the state machine's job, not Raft's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

/// The state that must survive a crash for Raft to stay safe (§1.5). Losing
/// `voted_for` lets a node vote twice in one term, which breaks election
/// safety — so this is written before any vote is granted, not after.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    pub term: Term,
    /// `None` means "has not voted this term", which is distinct from having
    /// voted for node 0. A sentinel value here would be a real bug.
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
}

/// A state machine snapshot replacing the log prefix up to and including
/// `last_included_index`. Produced and consumed at M8.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub data: Vec<u8>,
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
```

In `crates/kv-raft/src/lib.rs`, keep the existing module doc comment, delete the `crate_wires_up` placeholder test, and add:

```rust
pub mod storage;
pub mod types;

#[cfg(feature = "testing")]
pub mod testing;

pub use types::{Entry, HardState, LogIndex, NodeId, Snapshot, Term};
```

(`storage` and `testing` land in Tasks 2 and 3; add those lines then if you prefer a compiling intermediate state.)

- [ ] **Step 5: Run to verify it passes**

```bash
cargo nextest run -p kv-raft
cargo tree -p kv-raft | grep -q tokio && echo "BOUNDARY VIOLATION" || echo "boundary intact"
```

Expected: four tests PASS; "boundary intact".

- [ ] **Step 6: Commit**

```bash
cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/kv-raft/
git commit -m "M2: Raft core types (Term, Entry, HardState, Snapshot)"
```

---

## Task 2: The `RaftStorage` trait and `MemStorage`

**Files:**
- Create: `crates/kv-raft/src/storage.rs`, `crates/kv-raft/src/tests/storage.rs`
- Modify: `crates/kv-raft/src/lib.rs`

**Interfaces:**
- Consumes: Task 1's types.
- Produces: the `RaftStorage` trait, `StorageError`, `MemStorage`. M3 develops against `MemStorage` exclusively; M4's simulator uses it for every node.

- [ ] **Step 1: Write the failing tests**

Create `crates/kv-raft/src/tests/storage.rs`. These are `MemStorage`-specific smoke tests; the shared conformance suite arrives in Task 3.

```rust
use super::*;

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: vec![index as u8] }
}

#[test]
fn empty_log_reports_index_zero_and_term_zero() {
    let s = MemStorage::default();
    assert_eq!(s.last_index().unwrap(), 0);
    assert_eq!(s.term(0).unwrap(), Some(0), "index 0 is the before-the-log sentinel");
    assert_eq!(s.term(1).unwrap(), None);
    assert_eq!(s.entries(1, 1).unwrap(), vec![]);
}

#[test]
fn append_then_read_back_is_half_open() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 2)]).unwrap();

    assert_eq!(s.last_index().unwrap(), 3);
    assert_eq!(s.entries(1, 3).unwrap(), vec![entry(1, 1), entry(2, 1)]);
    assert_eq!(s.entries(2, 4).unwrap(), vec![entry(2, 1), entry(3, 2)]);
    assert_eq!(s.term(3).unwrap(), Some(2));
}

#[test]
fn append_with_a_gap_is_rejected() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1)]).unwrap();

    // A hole in the log is a programming error, not something to paper over —
    // Raft's log matching property depends on contiguity.
    assert!(matches!(s.append(&[entry(3, 1)]), Err(StorageError::Gap { expected: 2, got: 3 })));
}

#[test]
fn truncate_suffix_then_append_leaves_no_ghost_entries() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]).unwrap();

    s.truncate_suffix(2).unwrap();
    assert_eq!(s.last_index().unwrap(), 1);
    assert_eq!(s.term(2).unwrap(), None);

    s.append(&[entry(2, 5)]).unwrap();
    assert_eq!(s.last_index().unwrap(), 2, "the old index 3 must not reappear");
    assert_eq!(s.term(2).unwrap(), Some(5));
    assert_eq!(s.term(3).unwrap(), None);
    assert_eq!(s.entries(1, 10).unwrap(), vec![entry(1, 1), Entry { term: 5, index: 2, command: vec![2] }]);
}

#[test]
fn truncate_past_the_end_is_a_noop() {
    let mut s = MemStorage::default();
    s.append(&[entry(1, 1)]).unwrap();
    s.truncate_suffix(99).unwrap();
    assert_eq!(s.last_index().unwrap(), 1);
}

#[test]
fn hard_state_round_trips() {
    let mut s = MemStorage::default();
    assert_eq!(s.hard_state().unwrap(), HardState::default());

    let hs = HardState { term: 4, voted_for: Some(2), commit_index: 1 };
    s.save_hard_state(&hs).unwrap();
    assert_eq!(s.hard_state().unwrap(), hs);
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo nextest run -p kv-raft
```

Expected: compile error — `MemStorage`, `StorageError`, and the trait methods do not exist.

- [ ] **Step 3: Write `storage.rs`**

```rust
//! The persistence interface Raft needs, and an in-memory implementation.
//!
//! The trait is deliberately narrow: Raft needs to append to a log, read a
//! range back, ask a single entry's term, discard a divergent suffix, and
//! persist the hard state. Everything else — where the bytes live, whether
//! they are fsynced, how they are encoded — is the implementor's problem.
//!
//! `MemStorage` is what M3 and M4 run against: fast, deterministic, and with
//! no filesystem anywhere near the pure core. The production Bitcask-backed
//! impl lives in `kv-node`, because `kv-storage` must not know Raft exists.

use std::collections::BTreeMap;

use crate::types::{Entry, HardState, LogIndex, Snapshot, Term};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("append would leave a gap: expected first index {expected}, got {got}")]
    Gap { expected: LogIndex, got: LogIndex },
    #[error("entries are not contiguous at index {at}")]
    NotContiguous { at: LogIndex },
    #[error("backing store error: {0}")]
    Backend(String),
}

pub trait RaftStorage {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persists the state that must survive a crash for Raft to stay safe:
    /// current term, the vote in it, and the commit index. Callers must
    /// complete this before acting on the state it records — granting a vote
    /// before persisting it is how a node votes twice in one term.
    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error>;
    fn hard_state(&self) -> Result<HardState, Self::Error>;

    /// Appends contiguous entries starting at `last_index() + 1`.
    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error>;

    /// Entries in the half-open range `[lo, hi)`. Indices outside the log are
    /// silently skipped rather than erroring, so a caller can ask for more
    /// than exists.
    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error>;

    /// The term at `idx`, or `None` if no such entry exists. `term(0)` is
    /// always `Some(0)` — the before-the-log sentinel.
    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error>;

    /// Discards every entry with index `>= from`. A no-op if `from` is past
    /// the end. This is what M3.4's conflict resolution calls when a
    /// follower's log diverges from the leader's.
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error>;

    /// The highest index in the log, or 0 if the log is empty.
    fn last_index(&self) -> Result<LogIndex, Self::Error>;

    /// The most recent snapshot, if any. Written at M8; `None` until then.
    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error>;
}

#[derive(Debug, Default)]
pub struct MemStorage {
    entries: BTreeMap<LogIndex, Entry>,
    hard_state: HardState,
    snapshot: Option<Snapshot>,
}

impl RaftStorage for MemStorage {
    type Error = StorageError;

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error> {
        self.hard_state = *hs;
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState, Self::Error> {
        Ok(self.hard_state)
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let expected = self.last_index()? + 1;
        if first.index != expected {
            return Err(StorageError::Gap { expected, got: first.index });
        }
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(StorageError::NotContiguous { at: pair[1].index });
            }
        }
        for entry in entries {
            self.entries.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error> {
        Ok(self.entries.range(lo..hi).map(|(_, e)| e.clone()).collect())
    }

    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error> {
        if idx == 0 {
            return Ok(Some(0));
        }
        Ok(self.entries.get(&idx).map(|e| e.term))
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        self.entries.retain(|&idx, _| idx < from);
        Ok(())
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.entries.keys().next_back().copied().unwrap_or(0))
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(self.snapshot.clone())
    }
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod tests;
```

- [ ] **Step 4: Run to verify it passes**

```bash
cargo nextest run -p kv-raft
```

Expected: all six PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
git add crates/kv-raft/
git commit -m "M2: RaftStorage trait and in-memory implementation"
```

---

## Task 3: The shared conformance suite

**Files:**
- Create: `crates/kv-raft/src/testing.rs`
- Modify: `crates/kv-raft/src/lib.rs`, `crates/kv-raft/Cargo.toml` (`testing` feature added in Task 1)

**Interfaces:**
- Produces:
  ```rust
  pub trait StorageHarness {
      type Storage: RaftStorage;
      fn create(&mut self) -> Self::Storage;                      // fresh and empty
      fn reopen(&mut self, s: Self::Storage) -> Self::Storage;    // drop, reopen the same backing store
  }
  pub fn assert_storage_conformance<H: StorageHarness>(harness: &mut H);
  ```
  Task 5 runs this against `BitcaskStorage`.

**Why a harness and not just a factory:** durability assertions need to drop a store and reopen the *same* backing bytes, which a `Fn() -> S` cannot express. `MemStorage`'s `reopen` is the identity, which is honest — in-memory storage has no reopen — and the point of the suite is that both impls answer the same questions, not that both are durable.

- [ ] **Step 1: Write `testing.rs`**

This task inverts the usual order: the suite *is* the test, so there is no separate failing test to write first. It fails immediately in Task 5 if `BitcaskStorage` is wrong, which is the red this task exists to produce.

```rust
//! A conformance suite for `RaftStorage`, shared by every implementation.
//!
//! Behind the `testing` feature so it never reaches a release build. The point
//! is that `MemStorage` (which M3 and M4 run against) and `BitcaskStorage`
//! (which production runs against) answer identical questions identically —
//! a divergence between them would show up as a Raft bug that only reproduces
//! on real hardware, which is the worst kind to debug.

use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, LogIndex, Term};

pub trait StorageHarness {
    type Storage: RaftStorage;
    /// A fresh, empty store.
    fn create(&mut self) -> Self::Storage;
    /// Drop `s` and reopen the same backing store. For in-memory impls this
    /// is the identity; for durable ones it must actually round-trip disk.
    fn reopen(&mut self, s: Self::Storage) -> Self::Storage;
}

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: format!("cmd-{index}").into_bytes() }
}

/// Runs every `RaftStorage` requirement against `harness`. Panics on the first
/// violation, naming which requirement failed.
pub fn assert_storage_conformance<H: StorageHarness>(harness: &mut H) {
    empty_log_conventions(harness);
    append_and_range_reads(harness);
    contiguity_is_enforced(harness);
    truncate_suffix_leaves_no_ghosts(harness);
    hard_state_survives_reopen(harness);
    log_survives_reopen(harness);
    truncation_survives_reopen(harness);
}

fn empty_log_conventions<H: StorageHarness>(h: &mut H) {
    let s = h.create();
    assert_eq!(s.last_index().unwrap(), 0, "empty log's last_index must be 0");
    assert_eq!(s.term(0).unwrap(), Some(0), "index 0 is the before-the-log sentinel");
    assert_eq!(s.term(1).unwrap(), None, "no entry at 1 in an empty log");
    assert!(s.entries(1, 100).unwrap().is_empty());
    assert!(s.snapshot().unwrap().is_none());
}

fn append_and_range_reads<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 2)]).unwrap();

    assert_eq!(s.last_index().unwrap(), 3);
    assert_eq!(s.term(3).unwrap(), Some(2));
    assert_eq!(s.entries(1, 3).unwrap(), vec![entry(1, 1), entry(2, 1)], "entries is half-open");
    assert_eq!(s.entries(3, 4).unwrap(), vec![entry(3, 2)]);
    assert_eq!(s.entries(1, 999).unwrap().len(), 3, "reading past the end must not error");
    assert!(s.entries(5, 5).unwrap().is_empty(), "an empty range is empty, not an error");
}

fn contiguity_is_enforced<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1)]).unwrap();
    assert!(s.append(&[entry(3, 1)]).is_err(), "a gap in the log must be rejected");
    assert_eq!(s.last_index().unwrap(), 1, "a rejected append must change nothing");
}

fn truncate_suffix_leaves_no_ghosts<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&[entry(1, 1), entry(2, 1), entry(3, 1), entry(4, 1)]).unwrap();

    s.truncate_suffix(3).unwrap();
    assert_eq!(s.last_index().unwrap(), 2);
    assert_eq!(s.term(3).unwrap(), None);
    assert_eq!(s.term(4).unwrap(), None);

    s.append(&[entry(3, 9)]).unwrap();
    assert_eq!(s.last_index().unwrap(), 3, "the truncated index 4 must not reappear");
    assert_eq!(s.term(3).unwrap(), Some(9));
    assert_eq!(s.term(4).unwrap(), None, "ghost entry from before the truncation");
    assert_eq!(s.entries(1, 999).unwrap().len(), 3);

    s.truncate_suffix(999).unwrap();
    assert_eq!(s.last_index().unwrap(), 3, "truncating past the end is a no-op");
}

fn hard_state_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    assert_eq!(s.hard_state().unwrap(), HardState::default());

    let hs = HardState { term: 11, voted_for: Some(4), commit_index: 7 };
    s.save_hard_state(&hs).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.hard_state().unwrap(), hs, "a lost vote lets a node vote twice in one term");
}

fn log_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    let written: Vec<Entry> = (1..=50).map(|i| entry(i, 1 + i / 10)).collect();
    s.append(&written).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.last_index().unwrap(), 50);
    assert_eq!(s.entries(1, 51).unwrap(), written);
    assert_eq!(s.term(50).unwrap(), Some(6));
}

fn truncation_survives_reopen<H: StorageHarness>(h: &mut H) {
    let mut s = h.create();
    s.append(&(1..=20).map(|i| entry(i, 1)).collect::<Vec<_>>()).unwrap();
    s.truncate_suffix(11).unwrap();
    s.append(&[entry(11, 7)]).unwrap();

    let s = h.reopen(s);
    assert_eq!(s.last_index().unwrap(), 11, "truncated entries must not come back on reopen");
    assert_eq!(s.term(11).unwrap(), Some(7));
    assert_eq!(s.term(12).unwrap(), None);
}
```

- [ ] **Step 2: Run the suite against `MemStorage`**

Add to `crates/kv-raft/src/tests/storage.rs`:

```rust
#[cfg(feature = "testing")]
#[test]
fn mem_storage_satisfies_the_conformance_suite() {
    struct Harness;
    impl crate::testing::StorageHarness for Harness {
        type Storage = MemStorage;
        fn create(&mut self) -> MemStorage {
            MemStorage::default()
        }
        fn reopen(&mut self, s: MemStorage) -> MemStorage {
            // In-memory storage has no reopen; the identity is the honest
            // answer. The durability assertions are carried by BitcaskStorage.
            s
        }
    }
    crate::testing::assert_storage_conformance(&mut Harness);
}
```

Enable the feature for the crate's own tests by adding to `crates/kv-raft/Cargo.toml`:

```toml
[dev-dependencies]
kv-raft = { path = ".", features = ["testing"] }
```

If that self-dependency is awkward, run it explicitly instead: `cargo nextest run -p kv-raft --features testing`, and add that invocation to `.github/workflows/ci.yml` as a fourth step.

```bash
cargo nextest run -p kv-raft --features testing
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all -- --check && cargo clippy --workspace --all-targets --all-features -- -D warnings
git add crates/kv-raft/ .github/workflows/ci.yml
git commit -m "M2: shared RaftStorage conformance suite"
```

---

## Task 4: `BitcaskStorage` in `kv-node`

**Files:**
- Create: `crates/kv-node/src/storage.rs`, `crates/kv-node/src/tests/storage.rs`
- Modify: `crates/kv-node/src/main.rs`, `crates/kv-node/Cargo.toml`

**Interfaces:**
- Consumes: `RaftStorage`, `Entry`, `HardState` from `kv-raft`; `Engine`, `EngineConfig`, `FsyncPolicy` from `kv-storage`.
- Produces: `pub struct BitcaskStorage` with `pub fn open(dir: impl AsRef<Path>) -> io::Result<Self>`.

**Key layout** (Bitcask has no range scan, so the key space has to carry the structure):

| key | value |
|---|---|
| `b"\x00hard_state"` | bincode `HardState` |
| `b"\x00log_meta"` | bincode `LogMeta { last_index }` |
| `b"e" + format!("{index:020}")` | bincode `Entry` |

The `\x00` prefix keeps metadata out of the entry key space; entries are zero-padded so their keys sort, which costs nothing now and matters if a scan is ever added.

**Why `last_index` is a persisted record rather than derived:** Bitcask's keydir is in memory but `Engine` does not expose it, and a range scan does not exist. Persisting it is also *more* crash-correct than deriving it: if a crash lands between appending entries and updating `log_meta`, the stale `last_index` hides entries that were never acknowledged to anyone — precisely the right outcome. A later append overwrites them, and Bitcask's later-record-wins replay makes that safe.

- [ ] **Step 1: Write the failing test**

Create `crates/kv-node/src/tests/storage.rs`:

```rust
use super::*;

#[test]
fn bitcask_storage_satisfies_the_conformance_suite() {
    struct Harness {
        dir: tempfile::TempDir,
        generation: usize,
    }
    impl kv_raft::testing::StorageHarness for Harness {
        type Storage = BitcaskStorage;

        fn create(&mut self) -> BitcaskStorage {
            // Each `create` gets its own subdirectory so the suite's cases
            // cannot leak state into one another.
            self.generation += 1;
            let path = self.dir.path().join(format!("gen-{}", self.generation));
            BitcaskStorage::open(path).unwrap()
        }

        fn reopen(&mut self, s: BitcaskStorage) -> BitcaskStorage {
            let path = s.path().to_path_buf();
            drop(s);
            BitcaskStorage::open(path).unwrap()
        }
    }

    let mut harness = Harness { dir: tempfile::tempdir().unwrap(), generation: 0 };
    kv_raft::testing::assert_storage_conformance(&mut harness);
}

#[test]
fn entries_are_readable_after_a_reopen_without_rewriting_hard_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = BitcaskStorage::open(dir.path()).unwrap();
        s.append(&[Entry { term: 3, index: 1, command: b"x".to_vec() }]).unwrap();
    }

    let s = BitcaskStorage::open(dir.path()).unwrap();
    assert_eq!(s.last_index().unwrap(), 1);
    assert_eq!(s.term(1).unwrap(), Some(3));
    assert_eq!(s.hard_state().unwrap(), HardState::default());
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo nextest run -p kv-node
```

Expected: compile error — `BitcaskStorage` does not exist and `kv-node` depends on neither `kv-raft` nor `kv-storage` yet.

- [ ] **Step 3: Add dependencies**

In `crates/kv-node/Cargo.toml`:

```toml
[dependencies]
kv-raft.workspace = true
kv-storage.workspace = true
serde = { workspace = true, features = ["derive"] }
bincode.workspace = true
thiserror.workspace = true
tracing.workspace = true

[dev-dependencies]
kv-raft = { workspace = true, features = ["testing"] }
tempfile.workspace = true
```

`kv-storage` is already in `[workspace.dependencies]`; `kv-raft` is too. No change to the workspace manifest is needed.

- [ ] **Step 4: Write `storage.rs`**

```rust
//! Bitcask-backed `RaftStorage` (M2).
//!
//! This lives in `kv-node`, not `kv-storage`, on purpose: `kv-storage` must
//! not know Raft exists, and putting the impl there would invert the layering.
//! `kv-node` already depends on both, so it is the natural seam.
//!
//! Bitcask has no range scan, so the key space carries the structure:
//! `\x00hard_state` and `\x00log_meta` hold the two singletons, and each entry
//! lives at `e{index:020}`. `last_index` is persisted rather than derived —
//! and that is also the more crash-correct choice, since a crash between the
//! entry appends and the meta update simply hides entries that were never
//! acknowledged.

use std::path::{Path, PathBuf};

use kv_raft::storage::RaftStorage;
use kv_raft::types::{Entry, HardState, LogIndex, Snapshot, Term};
use kv_storage::Engine;
use serde::{Deserialize, Serialize};

const HARD_STATE_KEY: &[u8] = b"\x00hard_state";
const LOG_META_KEY: &[u8] = b"\x00log_meta";

fn entry_key(index: LogIndex) -> Vec<u8> {
    format!("e{index:020}").into_bytes()
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
struct LogMeta {
    last_index: LogIndex,
}

#[derive(Debug, thiserror::Error)]
pub enum BitcaskStorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encoding: {0}")]
    Encoding(#[from] bincode::Error),
    #[error("append would leave a gap: expected first index {expected}, got {got}")]
    Gap { expected: LogIndex, got: LogIndex },
    #[error("entries are not contiguous at index {at}")]
    NotContiguous { at: LogIndex },
}

pub struct BitcaskStorage {
    engine: Engine,
    path: PathBuf,
    last_index: LogIndex,
}

impl BitcaskStorage {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BitcaskStorageError> {
        let path = dir.as_ref().to_path_buf();
        let mut engine = Engine::open(&path)?;
        let last_index = match engine.get(LOG_META_KEY)? {
            Some(bytes) => bincode::deserialize::<LogMeta>(&bytes)?.last_index,
            None => 0,
        };
        Ok(Self { engine, path, last_index })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn put_meta(&mut self) -> Result<(), BitcaskStorageError> {
        let encoded = bincode::serialize(&LogMeta { last_index: self.last_index })?;
        self.engine.put(LOG_META_KEY, &encoded)?;
        Ok(())
    }
}

impl RaftStorage for BitcaskStorage {
    type Error = BitcaskStorageError;

    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), Self::Error> {
        let encoded = bincode::serialize(hs)?;
        self.engine.put(HARD_STATE_KEY, &encoded)?;
        Ok(())
    }

    fn hard_state(&self) -> Result<HardState, Self::Error> {
        // `Engine::get` takes &mut self because it seeks the segment file.
        // Storage's own borrow is shared here, so hard state is cached in the
        // struct if this turns out to matter; for now, read it through a
        // short-lived reopen-free path.
        unimplemented!("see Step 5 — the &self/&mut self mismatch is resolved there")
    }

    fn append(&mut self, entries: &[Entry]) -> Result<(), Self::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let expected = self.last_index + 1;
        if first.index != expected {
            return Err(BitcaskStorageError::Gap { expected, got: first.index });
        }
        for pair in entries.windows(2) {
            if pair[1].index != pair[0].index + 1 {
                return Err(BitcaskStorageError::NotContiguous { at: pair[1].index });
            }
        }

        for entry in entries {
            let encoded = bincode::serialize(entry)?;
            self.engine.put(&entry_key(entry.index), &encoded)?;
        }
        self.last_index = entries.last().expect("checked non-empty").index;
        self.put_meta()
    }

    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error> {
        unimplemented!("see Step 5")
    }

    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error> {
        unimplemented!("see Step 5")
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), Self::Error> {
        if from > self.last_index {
            return Ok(());
        }
        for index in from..=self.last_index {
            self.engine.delete(&entry_key(index))?;
        }
        self.last_index = from.saturating_sub(1);
        self.put_meta()
    }

    fn last_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.last_index)
    }

    fn snapshot(&self) -> Result<Option<Snapshot>, Self::Error> {
        Ok(None) // Written at M8.
    }
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod tests;
```

- [ ] **Step 5: Resolve the `&self` / `&mut self` mismatch**

`Engine::get` takes `&mut self` (it seeks and reads the segment file), but `RaftStorage`'s read methods take `&self`. This is a real interface decision, not a detail to paper over — **make it deliberately, here, before M3 builds on either shape.**

Take option A unless profiling later says otherwise:

- **A (recommended): wrap the engine in `RefCell`.** `BitcaskStorage` is owned by a single `RaftNode` and never shared across threads, so interior mutability costs nothing and keeps `RaftStorage`'s read methods `&self` — which is what M3's borrow patterns want, since `RaftNode` will hold storage while handing out read results.
  ```rust
  pub struct BitcaskStorage {
      engine: std::cell::RefCell<Engine>,
      path: PathBuf,
      last_index: LogIndex,
  }
  ```
  Then `self.engine.borrow_mut().get(...)`. Add `// Single-threaded by construction: one BitcaskStorage per Raft group, owned by its RaftNode.` above the field.

- **B: change the trait's read methods to `&mut self`.** Simpler types, but it forces `&mut` through every M3 call site that only wants to read a term, which will fight the borrow checker inside `RaftNode`.

Having chosen A, implement the three `unimplemented!` methods:

```rust
    fn hard_state(&self) -> Result<HardState, Self::Error> {
        match self.engine.borrow_mut().get(HARD_STATE_KEY)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(HardState::default()),
        }
    }

    fn entries(&self, lo: LogIndex, hi: LogIndex) -> Result<Vec<Entry>, Self::Error> {
        let hi = hi.min(self.last_index + 1);
        let mut out = Vec::new();
        let mut engine = self.engine.borrow_mut();
        for index in lo.max(1)..hi {
            if let Some(bytes) = engine.get(&entry_key(index))? {
                out.push(bincode::deserialize(&bytes)?);
            }
        }
        Ok(out)
    }

    fn term(&self, idx: LogIndex) -> Result<Option<Term>, Self::Error> {
        if idx == 0 {
            return Ok(Some(0));
        }
        if idx > self.last_index {
            return Ok(None);
        }
        match self.engine.borrow_mut().get(&entry_key(idx))? {
            Some(bytes) => Ok(Some(bincode::deserialize::<Entry>(&bytes)?.term)),
            None => Ok(None),
        }
    }
```

Update `open`, `put_meta`, `save_hard_state`, `append`, and `truncate_suffix` to go through `self.engine.borrow_mut()` as well.

- [ ] **Step 6: Wire the module into `main.rs`**

```rust
mod storage;

pub use storage::BitcaskStorage;
```

Delete the `crate_wires_up` placeholder test from `crates/kv-node/src/main.rs`.

- [ ] **Step 7: Run to verify it passes**

```bash
cargo nextest run -p kv-node
```

Expected: both tests PASS, including the full conformance suite. If `truncation_survives_reopen` fails, the `log_meta` update in `truncate_suffix` is missing or out of order — that is exactly the case the suite exists to catch.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all -- --check && cargo clippy --workspace --all-targets --all-features -- -D warnings
git add crates/kv-node/
git commit -m "M2: Bitcask-backed RaftStorage, passing the shared conformance suite"
```

---

## Task 5: Proptest the two implementations against each other

**Files:**
- Create: `crates/kv-node/src/tests/storage_equivalence.rs` (referenced from `crates/kv-node/src/storage.rs`)

**Interfaces:** Consumes everything above. Produces nothing.

The conformance suite checks the cases someone thought of. This checks the ones nobody did: any sequence of appends and truncations must leave both implementations reporting identical logs. This is the test that catches a divergence before M3 spends six sessions blaming Raft for it.

- [ ] **Step 1: Write the test**

```rust
use super::*;
use kv_raft::storage::MemStorage;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum Op {
    Append(u64, u64),   // (count, term)
    Truncate(u64),      // absolute index
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1u64..5, 1u64..4).prop_map(|(n, t)| Op::Append(n, t)),
        (1u64..20).prop_map(Op::Truncate),
    ]
}

proptest! {
    #[test]
    fn bitcask_and_mem_storage_agree(ops in proptest::collection::vec(op_strategy(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();
        let mut bitcask = BitcaskStorage::open(dir.path()).unwrap();
        let mut mem = MemStorage::default();

        for op in &ops {
            match *op {
                Op::Append(count, term) => {
                    let start = mem.last_index().unwrap() + 1;
                    let entries: Vec<Entry> = (start..start + count)
                        .map(|index| Entry { term, index, command: vec![term as u8] })
                        .collect();
                    // Both must accept or both must reject.
                    let a = bitcask.append(&entries).is_ok();
                    let b = mem.append(&entries).is_ok();
                    prop_assert_eq!(a, b, "append acceptance diverged at {:?}", op);
                }
                Op::Truncate(from) => {
                    bitcask.truncate_suffix(from).unwrap();
                    mem.truncate_suffix(from).unwrap();
                }
            }

            prop_assert_eq!(bitcask.last_index().unwrap(), mem.last_index().unwrap());
            let hi = mem.last_index().unwrap() + 1;
            prop_assert_eq!(bitcask.entries(1, hi).unwrap(), mem.entries(1, hi).unwrap());
        }

        // And the agreement must survive a reopen, which is where a
        // persisted-metadata bug would finally show itself.
        let hi = mem.last_index().unwrap() + 1;
        let expected = mem.entries(1, hi).unwrap();
        drop(bitcask);
        let reopened = BitcaskStorage::open(dir.path()).unwrap();
        prop_assert_eq!(reopened.entries(1, hi).unwrap(), expected);
    }
}
```

Reference it from `crates/kv-node/src/storage.rs`:

```rust
#[cfg(test)]
#[path = "tests/storage_equivalence.rs"]
mod storage_equivalence;
```

- [ ] **Step 2: Run it**

```bash
cargo nextest run -p kv-node bitcask_and_mem_storage_agree
```

Expected: PASS. A counterexample here is a real divergence — proptest will have shrunk it to a minimal op sequence. Fix the implementation; do not relax the property.

- [ ] **Step 3: Full gate and commit**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
git add crates/kv-node/
git commit -m "M2: proptest BitcaskStorage against MemStorage as an oracle"
```

- [ ] **Step 4: Update the README status line**

`README.md` says "Status: in progress (M1.x of M13)". Update it to M2 and mention that the Raft storage layer exists while the Raft core does not yet.

```bash
git add README.md
git commit -m "docs: README status reflects M2"
```

---

## Acceptance

- ✅ The same conformance suite passes against both `MemStorage` and `BitcaskStorage` — the milestone's central claim.
- ✅ `truncate_suffix` then `append` leaves no ghost entries, asserted in both impls and again after a reopen.
- ✅ Hard state survives reopen, including the distinction between "has not voted" and "voted for node 0".
- ✅ `bitcask_and_mem_storage_agree` passes over proptest's default case count, including the post-reopen comparison.
- ✅ `cargo tree -p kv-raft` lists no `tokio` — the purity boundary M3 and M4 depend on is intact.
- ✅ `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo nextest run --workspace` all clean.

**Decisions this milestone locks in, which M3 onward must not silently change:** log indices start at 1 with 0 as the before-the-log sentinel; `entries` is half-open; `append` demands contiguity; `RaftStorage`'s read methods take `&self` with interior mutability in the durable impl. Each is written into the trait's doc comments so the next person meets them before the code.
