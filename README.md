# Bohime

A sharded, Raft-replicated key-value store in Rust, over gRPC.

**Status: in progress (M2 of M13).** The Bitcask storage engine (record
codec, append-only segments, crash-safe reopen via log replay, segment
rotation, compaction with hint files, torn-tail truncation and a
crash-safe compaction commit manifest, configurable fsync policy with group
commit) and the Raft persistent-state layer (`RaftStorage` trait with
in-memory and Bitcask-backed impls behind one conformance suite) are
implemented and tested; the Raft core itself is next. Design and milestone
sharding, and the gRPC layer are still ahead. Design and milestone plan live
in `docs/DESIGN.md` — architecture is multi-Raft (consistent hashing over
256 shards, each an independent from-scratch Raft group), storage is a
from-scratch Bitcask-style log-structured engine, and correctness is checked
with a deterministic simulator, property-based tests, and a linearizability
checker under fault injection.

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
