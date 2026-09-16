# Bohime

A sharded, Raft-replicated key-value store in Rust, over gRPC.

**Status: in progress (M6 of M13).** The first end-to-end demo works: three
processes form a Raft group, a client writes through the leader, every node
serves the read, and `kill -9` on the leader loses nothing.

Implemented and tested: the Bitcask storage engine (record codec, append-only
segments, crash-safe reopen via log replay, segment rotation, compaction with
hint files, torn-tail truncation, a crash-safe compaction commit manifest, and
a configurable fsync policy with group commit); the Raft core as a pure state
machine with no I/O, no async and no clock reads; a deterministic simulator
that runs whole clusters under packet loss, partitions and crashes across
100,000 seeds and reproduces any failure byte-for-byte from its seed; a gRPC
transport with bounded queues, per-RPC deadlines and reconnect-with-backoff;
and the node binary and client that turn all of it into a working key-value
store.

Ahead: linearizable reads and client sessions (M7), snapshots (M8), membership
change (M9), then the sharding work — consistent hashing over 256 shards with
one independent Raft group each (M10-M12).

**Reads are deliberately stale until M7.** `Get` at M6 is served from local
state, so a follower that has not yet applied the latest commit will answer
with the previous value. That is visible from the CLI if you look for it, and
M7's ReadIndex is what fixes it — the plan requires writing that test against
this implementation first and watching it fail.

### Try it

```
cargo build
P="--peer 1=http://127.0.0.1:7521 --peer 2=http://127.0.0.1:7522 --peer 3=http://127.0.0.1:7523"
for i in 1 2 3; do
  ./target/debug/kv-node --id $i --listen 127.0.0.1:752$i $P --data-dir /tmp/bohime/n$i &
done

./target/debug/kv-client $P put greeting hello        # OK
./target/debug/kv-client --peer 2=http://127.0.0.1:7522 get greeting   # hello
```

Every node takes the same `--peer` flags; only `--id` and `--listen` differ.

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

### Storage: fsync policy

1,000 sequential `put`s of a small value, `cargo bench -p kv-storage -- fsync_policy`.
Measured on 12th Gen Intel i9-12900H, tmpfs (`/tmp`), Linux 7.2.3-arch1-3.

| policy | time / 1k writes | writes / sec | loss window on machine crash |
|---|---|---|---|
| `Never` | ~499µs | ~2.00M | everything not yet flushed by the OS |
| `GroupCommit { max_records: 100, max_delay: 10ms }` | ~522µs | ~1.92M | ≤100 records or ≤10ms |
| `EveryWrite` | ~664µs | ~1.51M | none |

Note on what these numbers do and do not prove: the benchmark measures the
cost of the `fsync` calls, and the loss-window column is an argument from the
code, not a measured result. Demonstrating it would require cutting power to
the machine — a `kill -9` proves nothing here, because the page cache
outlives the process. (These particular numbers were taken on tmpfs, where
`sync_data` is cheap; expect a far wider gap on a real disk.)
