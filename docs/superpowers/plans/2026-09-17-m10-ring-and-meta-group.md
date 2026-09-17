# M10 — Ring hashing and the meta group

`docs/DESIGN.md` §1.11 and the M10 milestone are the source of truth. This
file records the decisions that milestone left open, and the order the work
goes in.

## What M10 delivers

- `kv-ring`: a pure, dependency-free placement library — key→shard, a ring of
  physical nodes with virtual nodes, and a versioned `ShardMap`.
- A **meta group**: a second Raft group, in the same process, whose state
  machine holds the shard map.
- The map published to request handlers through `ArcSwap<ShardMap>`, and
  readable linearizably over `AdminService`.

Not in M10: routing requests by the map (M11), moving data when it changes
(M12). With one data group there is nothing to route to yet.

## Decisions

### Placement: the ring computes, the meta group records

Physical nodes sit on a hash ring, ~128 virtual nodes each. A shard hashes to
a ring position; walking clockwise and collecting `RF` **distinct physical**
nodes gives that shard's replica set, which is that shard's Raft group.

Key→shard is `hash(key) % num_shards` and never touches the ring. That is what
makes it stable across restarts and across membership changes — M10's first ✅.

The ring is a pure function; its materialized output is a versioned `ShardMap`
replicated by the meta group. Deriving placement locally on each node instead
would break during the window where nodes learn of a membership change at
different instants: two nodes compute different rings, disagree about who owns
shard 37, and a client routed by one writes to a group the other does not
consider authoritative. Split brain, but for placement. The replicated map also
gives M12 somewhere to write "shard 37 is mid-migration".

### The hash must be stable, so we own it

`std::collections::hash_map::DefaultHasher` documents that its output may
change between Rust releases. A toolchain upgrade would silently remap every
key to a different shard — data loss that looks like a bug in Raft. So
`kv-ring` carries its own: FNV-1a 64 with an fmix64 finalizer, ~12 lines, no
dependency, pinned by golden test vectors. The finalizer is what makes the low
bits safe for `% num_shards`; raw FNV-1a mixes them weakly.

### Both numbers are the operator's, but only at bootstrap

`--shards` (default 256) and `--replication-factor` (default 3) seed **version
1** of the map and nothing else. After that the replicated map owns them, the
same way the log owns membership once it exists (`initial_learner` has this
exact shape today). A node whose argv disagrees with the replicated map
**refuses to start** rather than misroute every key: `hash % 256` and
`hash % 512` disagree about nearly every key, so a mismatch is unrecoverable
corruption, not a warning.

- `RF = 1` is legal (single-node development).
- `RF = 2` is legal with a startup warning: it tolerates zero failures and is
  *less* available than RF=1, since either node being down stops the group.
  What it buys is durability — the data survives a lost disk. That is a real
  trade someone may want deliberately.
- `RF > node count` is **refused at startup**. The walk cannot collect RF
  distinct nodes, and a config that cannot be satisfied should fail loudly
  rather than quietly under-replicate.

### The meta group is independent of the data cluster

Its membership is the founding `--peer` set and does not grow when the data
cluster does — the same way TiKV's Placement Driver stays 3 nodes regardless
of how many TiKV nodes exist. The ring's node set comes from the *data*
group's `ClusterConfig`; the meta group merely records it.

### Hosting: a second `Driver`, group-tagged wire

`RaftService` today cannot tell two groups apart. All four RPCs gain a
`uint32 group`, `RaftServer` routes to a per-group inbox, and the meta group
is a second `Driver` instance over `meta-raft/` + `meta-state/`. `Driver` is
reused unchanged; M11 generalizes hosting to N groups.

Group ids: **0 is reserved and rejected**, 1 is the meta group, shard groups
are `shard_id + 2`. proto3 defaults an absent field to 0, so a message that
lost its group id is an explicit error instead of a silent misroute — the same
reasoning that made the conflict hints `optional` at M5.

### The state machine is a versioned register

The map is one bincode blob under reserved `\x00shardmap`, joining the
`\x00session/` and `\x00peer/` key spaces. It is mutated with M7's `Cas`:
the proposer computes the next map with `kv-ring` and proposes
`Cas { expected: current, new }`. Two racing proposers are resolved by the
Cas, not by a lock. `Driver::apply` needs no new branch, and a linearizable
read of the map is an ordinary ReadIndex `Get`.

### Reconciliation composes over existing channels

A `MetaReconciler` task on every node, needing **no change to `Driver`**:

1. read the *data* driver's `ClusterStatus` (existing `AdminOp`) for the node set,
2. read this node's local meta map,
3. if this node leads the meta group and the two disagree, compute the next
   map and propose the `Cas` through the meta driver's request channel.

Only the meta leader proposes; the `Cas` makes a race harmless. The same task
publishes into `ArcSwap<ShardMap>`.

Two reads, deliberately different:

- **Cache publication** — `AdminOp::LocalShardMap`, reads local applied state,
  stale by at most the apply lag. Every node serves it, including followers.
  That is what "read on every request" in the design means.
- **Linearizable** — `AdminService::GetShardMap { linearizable: true }` goes
  through the meta driver's ReadIndex and redirects on a follower. This is
  M10's third ✅.

## Order of work

Strict TDD, one commit per step, per the project workflow.

- **M10.1** `kv-ring`: stable hash + `shard_for`. Golden vectors; distribution.
- **M10.2** `kv-ring`: `Ring`, vnodes, `place()` collecting RF distinct nodes.
  RF > N refused here, as a typed error.
- **M10.3** `kv-ring`: `ShardMap`, versioning, codec, and the movement
  invariant — proptest that adding a node leaves each shard's replica set
  either **unchanged**, or the old set with the new node inserted and the last
  element dropped. No shard may be reshuffled between two nodes that both
  remain. That is a far stronger claim than the ≈`1/N` statistical bound, and
  it is the real content of "and no more".
- **M10.4** The wire: `group` on all four RPCs, per-group inbox routing,
  group 0 rejected.
- **M10.5** The second `Driver`: config, data dirs, `main.rs` wiring, both
  groups electing independently.
- **M10.6** Bootstrap: the meta leader creates version 1 once; a racing
  proposal loses the `Cas`; argv disagreeing with the stored map refuses
  startup.
- **M10.7** Reconciliation and `ArcSwap` publication.
- **M10.8** `GetShardMap`, both reads, follower redirect.
- **M10.9** `tests::ring::the_m10_gate` — key→shard stable across a restart;
  adding a node moves ≈`1/N` of shards and no more; a deposed meta leader
  cannot serve a stale map.

## ✅ criteria (from DESIGN.md)

- Key→shard is stable across restarts.
- Adding a node moves ≈`1/N` of shards and no more.
- The shard map is itself linearizable.

---

## What landed

All nine steps, all three ✅ criteria, `tests::ring::the_m10_gate`. 355 tests
pass on both read engines; fmt and clippy `-D warnings` clean.

Two things came out differently from the plan above, both worth knowing.

**`kv-ring` grew a dependency on `kv-raft`**, for `NodeId` alone. The
alternative was a second `type NodeId = u64` in a second crate, and two names
for one concept is worse than an edge in the dependency graph — especially an
edge pointing at a crate that is itself pure. CI's tokio guard now covers
`kv-ring` for the same reason it covers `kv-raft`: M11's simulator has to be
able to call placement.

**The "≈1/N" criterion is about replica slots, not shards.** The gate's first
draft compared the number of shards a new node appears in against
`num_shards / N`, and failed at 205 of 256 against a "fair share" of 64. The
implementation was right and the arithmetic was wrong: with RF=3 on four nodes
every shard holds three of the four, so a new node necessarily appears in about
three quarters of all shards. That number measures nothing. What moves is one
replica **slot** per shard the node joins, out of `num_shards * rf` — 205 of
768, which is the 1/4 the criterion asks for. The comment in the gate says so,
because the wrong reading is the intuitive one.

Also worth recording: the plan called the meta group's membership "the founding
`--peer` set", and that is what shipped, but the consequence was not spelled
out. A node admitted after bootstrap is not a meta-group member, so it has no
local copy of the map to publish. Today nothing routes by the map, so nothing
notices. M11 must either admit late nodes to the meta group as permanent
**learners** (replicating the map, never voting, which is what M9's learner
support already makes cheap) or have them read the map over RPC. The learner
route is the better one and is why learner `match_index` tracking exists.

## Not done, deliberately

- Nothing routes requests by the map. M11.
- The meta group does not grow with the data cluster (see above).
- `Rebalance`/`VerifyShard` still answer `unimplemented`. M12.
