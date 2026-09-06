# Bohime

A sharded, Raft-replicated key-value store in Rust, over gRPC.

**Status: in progress (M1.5 of M13).** The Bitcask storage engine (record
codec, append-only segments, crash-safe reopen via log replay, segment
rotation, compaction with hint files) is implemented and tested; Raft,
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
