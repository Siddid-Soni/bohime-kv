# Bohime

A sharded, Raft-replicated key-value store in Rust over gRPC, kept as small as
it can be: one node per shard replica, a single-file Bitcask, Raft with elections and log replication
only, and fixed hash sharding. About 830 lines of Rust in one crate.

The full-featured version (snapshots, membership changes, a meta group,
shard migration, wait-free reads, a deterministic simulator) lives on the
[`testing/experimental`](../../tree/testing/experimental) branch.

## Quick start

Needs a stable Rust toolchain and `protoc`.

```
cargo build --release
B=target/release/bohime
N="--node 1=127.0.0.1:7001 --node 2=127.0.0.1:7002 --node 3=127.0.0.1:7003"

$B serve --id 1 $N --data-dir d1 &
$B serve --id 2 $N --data-dir d2 &
$B serve --id 3 $N --data-dir d3 &

export BOHIME_NODES=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
$B put color blue     # OK
$B get color          # blue
$B del color          # OK
$B get color          # (not found), exit code 1
```

### `serve` flags

| flag | default | meaning |
|---|---|---|
| `--id` | required | this node's id (ids start at 1) |
| `--node id=host:port` | required | every node in the cluster, this one included |
| `--rf` | 3 | replicas per shard; the node count must be a multiple of it |
| `--data-dir` | required | where this node's data file lives |
| `--tick-ms` | 50 | Raft tick; heartbeats go out every tick |

Every node must get the same `--node` list and `--rf`. Placement
is computed from them and never changes.

## How it works

```
client ──Kv.Call──▶ any node ──redirect──▶ shard leader

     shard 0                  shard 1                  shard 2
 ┌──────┬──────┬──────┐  ┌──────┬──────┬──────┐  ┌──────┬──────┬──────┐
 │node 1│node 2│node 3│  │node 4│node 5│node 6│  │node 7│node 8│node 9│
 └──────┴──────┴──────┘  └──────┴──────┴──────┘  └──────┴──────┴──────┘
   one Raft group           one Raft group           one Raft group
   each node: one loop, one data file, fsync per batch
```

- **Sharding.** Each node is one replica of one shard. The sorted node ids
  are cut into runs of `rf`, one shard per run, so 9 nodes at RF 3 make 3
  shards. A key's shard is `fnv1a(key) % shards`; a node that gets a key of
  another shard redirects to one of that shard's nodes.
- **Replication.** Every shard is its own Raft group over its `rf` nodes, so
  shards share nothing and scale by adding `rf` nodes. Raft itself
  (`src/raft.rs`) does no I/O: it changes state and queues messages, and the
  node persists and sends them.
- **Reads go through the log**, the same as writes. A read is answered once
  its entry commits and is applied, so it is linearizable.
- **Durability.** A node writes its new log entries and vote as one
  batch and fsyncs it on a separate thread, meanwhile taking in more
  requests for the next batch. Only when the fsync returns does it send that
  batch's messages and answer its clients. Nothing is acknowledged before it
  is on disk.
- **Storage.** One append-only file per node. Each record is
  `crc32 | key_len | value_len | key | value`, and a `value_len` of
  `u32::MAX` marks a delete. An in-memory index maps each key to its latest
  value. Raft state, log entries and user data share the file under
  different key prefixes.

### Conflicts

- **Concurrent writes to one key.** The shard leader orders them in its log;
  the later entry wins on every replica. There are no timestamps or vector
  clocks.
- **Replicas whose logs disagree.** The follower rejects an append that does
  not match its log and tells the leader where to back up to. It then drops
  its conflicting uncommitted entries and takes the leader's. The leader's
  log always wins, and a node cannot become leader unless its log is at
  least as up to date as a majority's.

### Failures

- **Detection** is Raft's timers only. The leader sends a heartbeat every
  tick (50ms). A follower that hears nothing for a random 10-20 ticks
  (0.5-1s) starts an election.
- **A crashed node** loses nothing it acknowledged. If it led its shard, the
  shard elects a new leader among its remaining nodes, as long as a majority
  of them is up.
- **When a node comes back**, it cuts off any half-written record at the end
  of its file and reloads its term, vote and log. The leader then
  sends it the entries it missed, up to 256 per heartbeat, and it rebuilds
  its data by replaying the log from the start.
- **A client** tries any node, follows redirects to the leader, and retries
  another node on timeout. A retried put or delete may apply twice, which is
  harmless because both are idempotent.

## Left out on purpose

- No log compaction or snapshots: the data file and the log grow forever,
  and a restart replays the whole log.
- No adding or removing nodes, and no rebalancing.
- No exactly-once retries (put and delete are idempotent, so none are
  needed), and no compare-and-swap.
- No pre-vote: a node that was cut off comes back with a higher term and
  forces a short, unneeded election.

## Layout

```
proto/bohime.proto   the Kv and Raft services and their messages
src/bitcask.rs       storage
src/raft.rs          Raft for one group
src/node.rs          placement, the driver loop, persistence, gRPC
src/client.rs        redirect-following client
src/main.rs          the CLI
src/tests/           unit tests and whole-cluster tests over loopback gRPC
```

## Test

```
cargo nextest run          # or cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt -- --check
```
