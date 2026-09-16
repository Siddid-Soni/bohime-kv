//! Every test in this crate. No source file carries a `mod tests;` tail — the
//! whole tree is declared here and hung off `lib.rs` in one place, so adding a
//! test file is one line in this file and nothing else.
//!
//! Tests are not child modules of the code they exercise, so they reach it by
//! absolute `crate::` paths and anything they touch is at least `pub(crate)`.

pub(crate) mod harness;

mod append_entries;
mod commit;
mod conformance;
mod election_restriction;
mod invariants;
mod leader_hint;
mod message;
mod node_tick;
mod request_vote;
mod single_node;
mod storage;
mod types;
