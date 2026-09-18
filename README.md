# Bohime

A sharded, Raft-replicated key-value store in Rust, over gRPC.

**Status: in progress (M12.1 of M13; M12.2 and M12.5 not started).** A cluster
shards its keyspace across 256 independent Raft groups, replicated 3-way by
default; a node can be added live and receives its share of shards by
migration; reads are linearizable; `kill -9` on any leader loses nothing; and
a retried compare-and-swap applies exactly once.

Implemented and tested: the Bitcask storage engine (record codec, append-only
segments, crash-safe reopen via log replay, segment rotation, compaction with
hint files, torn-tail truncation, a crash-safe compaction commit manifest, a
configurable fsync policy with group commit, and a wait-free `left-right`
keydir alongside the original locked one); the Raft core as a pure state
machine with no I/O, no async and no clock reads, including snapshots
(`InstallSnapshot`, log truncation) and membership changes (learner → voter
promotion, single-server removal); a deterministic simulator that runs whole
clusters under packet loss, partitions and crashes across 100,000 seeds and
reproduces any failure byte-for-byte from its seed; a gRPC transport with
bounded per-peer queues, per-RPC deadlines, reconnect-with-backoff and
batched multi-group messages; ReadIndex for linearizable reads (plus an
opt-in zero-round-trip lease-read path); a replicated session table for
exactly-once retries; consistent hashing over a versioned shard map, agreed
by a dedicated meta Raft group; one tick loop driving every shard group a
node replicates; and a migration driver that moves shards onto a newly
admitted node (learner-add → catch-up → promote → remove-old, rate-limited)
without failing in-flight requests.

Ahead: Merkle verification for rebalance correctness (M12.2), AppendEntries
pipelining (M12.5), then M13. `docs/KNOWN-ISSUES.md` tracks what's built but
not yet correct in every case — notably, a replica removed by a rebalance
does not yet reclaim its shard's disk (§8).

### On reads

A read goes through **ReadIndex** (§1.10): the leader records its commit index,
confirms with a heartbeat quorum that it still leads, waits until the state
machine has applied that far, and only then answers. One network round trip,
no disk write.

The confirmation is the whole point, and it was built the way the plan demands
— the test was written first against M6's naive local read and watched to fail:

```
stale read: node 3 served v1 after v2 was committed on 1
```

A leader that has been partitioned away does not know it. Nothing tells it, and
Raft leaders do not step down on their own. Reading its local state therefore
returns whatever it last applied, which may have been overwritten minutes ago
on the majority side. That is a linearizability violation, not a stale cache.

The cost is that only the leader serves reads; followers redirect. `--lease-reads`
trades that round trip away — a recently confirmed leader answers locally — and
is **off by default**, because its correctness rests on bounded clock drift,
which ReadIndex does not assume. What that buys and what it costs both have
tests.

### Try it

```
cargo build
P="--peer 1=http://127.0.0.1:7521 --peer 2=http://127.0.0.1:7522 --peer 3=http://127.0.0.1:7523"
for i in 1 2 3; do
  ./target/debug/kv-node --id $i --listen 127.0.0.1:752$i $P --data-dir /tmp/bohime/n$i &
done

./target/debug/kv-client $P put greeting hello       # OK
./target/debug/kv-client $P get greeting            # hello

./target/debug/kv-client $P cas counter 1           # create if absent
./target/debug/kv-client $P cas counter 2 --expected 1
./target/debug/kv-client $P cas counter 9 --expected 1   # (not swapped), exit 1
```

Every node takes the same `--peer` flags; only `--id` and `--listen` differ.
The client finds the leader from the `NotLeader` hints and caches it. Every
node also bootstraps the same 256-shard, RF-3 map by default (`--shards`,
`--replication-factor`, `--vnodes` are bootstrap-only — see `CLAUDE.md`); a
key routes to its shard's own Raft group under the hood, invisibly to the
client above.

### Adding a node, and rebalancing

There is no admin CLI yet (`docs/DESIGN.md`'s "known-wrong-on-purpose" list);
membership and rebalance calls go straight through the gRPC `AdminService`
(`proto/admin.proto`), e.g. with `grpcurl`:

```
grpcurl -plaintext -d '{"node_id": 4, "address": "127.0.0.1:7524"}' \
  127.0.0.1:7521 bohime.admin.AdminService/AddNode
```

A joining node still needs `--peer` for the cluster as it stands, plus
`--join`, and is admitted as a learner before being promoted to a voter. Once
it's a voter, each shard's leader reconciles its own group against the
published map on its own; `Rebalance` just nudges that pass to run now rather
than waiting for its periodic sweep, and `RebalanceResponse.diverging_shards`
tells you whether *this node's* leadership is done, not the cluster's.

This README will grow into the real project overview (architecture diagram,
benchmark table, explicit non-goals) as milestones land — see M13.

## Layout

- `crates/kv-proto` — generated gRPC/protobuf types
- `crates/kv-storage` — Bitcask storage engine
- `crates/kv-raft` — pure Raft consensus core (no I/O)
- `crates/kv-ring` — consistent hashing + shard map
- `crates/kv-node` — the node binary (tokio/tonic shell)
- `crates/kv-client` — smart client library + CLI
- `crates/kv-sim` — deterministic simulator + fault injection

## Development

```
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

## Benchmarks

Four benches exist, all Criterion-based. Three so far have caught the
benchmark measuring itself rather than the system — see
`docs/KNOWN-ISSUES.md` §7 before trusting a number that predates this
section, and read a bench file's own doc comment before quoting its numbers
elsewhere: that comment is the source of truth, this table is a summary of it.

### Storage micro-benchmarks (`cargo bench -p kv-storage`)

- `--bench engine -- fsync_policy` — 1,000 sequential `put`s of a small value.
- `--bench read_scaling` — `Engine::get` under 1-20 concurrent readers, plus a
  cold-cache single-read arm.
- `--bench index_scaling` — the keydir alone (`RwLock<HashMap>` vs. `DashMap`
  vs. `left-right`) under concurrent reads, with and without a writer.

`read_scaling` and `index_scaling` write to `CARGO_TARGET_TMPDIR` (real disk)
on purpose; `engine`'s fsync arm below runs on tmpfs (`/tmp`), where
`fsync`/`fdatasync` return without doing anything asked of a real disk — a
deliberate choice for this arm because it isolates the syscall's own cost, not
a mistake, but do not compare it against the other two benches' numbers.

Measured on a 12th Gen Intel i9-12900H, 6 P-cores + 8 E-cores, Linux
7.2.3-arch1-3:

| policy (`engine`, tmpfs) | time / 1k writes | writes / sec | loss window on machine crash |
|---|---|---|---|
| `Never` | ~499µs | ~2.00M | everything not yet flushed by the OS |
| `GroupCommit { max_records: 100, max_delay: 10ms }` | ~522µs | ~1.92M | ≤100 records or ≤10ms |
| `EveryWrite` | ~664µs | ~1.51M | none |

The loss-window column is an argument from the code, not a measured result —
demonstrating it would mean cutting power to the machine, since `kill -9`
proves nothing while the page cache outlives the process.

`read_scaling` (real disk, `Engine::get`, 1→20 threads): 0.90 → 6.5 Melem/s,
**7.2×**, flat from 16 threads on — the ceiling is one shared `struct file`
per segment inside the kernel, not the keydir, not this crate. `index_scaling`
measures what `left-right` buys once that ceiling is removed from the
comparison; see the file's own doc comment for the three arms' numbers, which
move as `kv-storage` changes.

### Cluster throughput (`cargo bench -p kv-node --bench cluster`)

Spawns real `kv-node` processes and drives them over real gRPC with the real
client — the only bench here that measures what a caller actually gets,
writes/sec and latency, rather than one crate in isolation. Every run needs a
control arm (see the bench file's own doc comment for why); the defaults run
both a write and a read sweep across shard counts.

```
cargo bench -p kv-node --bench cluster                                    # defaults
BOHIME_BENCH_SHARDS=1,8,64 BOHIME_BENCH_CLIENTS=64,128,256,512 \
  cargo bench -p kv-node --bench cluster
BOHIME_BENCH_OPS=400 cargo bench -p kv-node --bench cluster    # per client, overrides TOTAL_OPS
BOHIME_BENCH_ARMS=get cargo bench -p kv-node --bench cluster   # reads only
BOHIME_BENCH_FSYNC=every-write cargo bench -p kv-node --bench cluster
BOHIME_BENCH_DIR=/tmp cargo bench -p kv-node --bench cluster   # tmpfs control only, not a result
```

Same machine, btrfs on NVMe, RF 3, 32 concurrent clients, 256-byte
pseudo-random values, writes/sec:

| shards | `EveryWrite` (before group commit) | `group-commit` (after) | tmpfs control |
|---|---|---|---|
| 1 | 58 | 91 | 121 |
| 8 | 88 | 237 | 1068 |
| 64 | 82 | 137 | 1897 |

The "before" column is flat — sharding alone did not multiply write
throughput; every shard's log landed on the same `fdatasync`-limited device.
Group commit is the lever that moved it, not shard count. Sweeping client
count at 64 shards instead of shard count is flat across an 8× concurrency
range (2992-3543 op/s at 64-512 clients) with p50 growing linearly (14→78ms)
— a saturated system, not a collapse; an earlier reading of that same sweep
as "congestion collapse" was the harness sharing one cluster and doing
unequal work per arm, fixed in `7a19b60`. Do not publish a number from this
bench without its control arms alongside it.
