//! Bitcask-style log-structured storage engine (plan §1.13).
//!
//! Implementation begins at M1: record codec (M1.1), the append-only engine
//! (M1.2), keydir rebuild behind a `KeyDirIndex` trait (M1.3), segment
//! rotation (M1.4), compaction + hint files (M1.5), crash recovery (M1.6),
//! and configurable fsync policy (M1.7).
//!
//! M11.5 swapped the keydir behind that trait for `left-right`, which is what
//! lets a reader resolve a key from another thread while the writer goes on
//! applying: see `index/left_right.rs` for the primitive and [`ReadView`] for
//! the reader's half of the engine.
mod config;
mod engine;
mod index;
mod record;
#[cfg(test)]
mod tests;
pub use config::{EngineConfig, FsyncPolicy, IndexKind};
pub use engine::{Engine, ReadHold, ReadView, ReadViewFactory, ValueRef};
