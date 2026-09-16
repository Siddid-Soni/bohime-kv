//! gRPC transport for the Raft core (M5): the impure shell that carries
//! `kv_raft::Message` values between processes.
//!
//! `allow(dead_code)` scoped to this module: kv-node is a binary crate and the
//! driver loop that wires the transport into a `RaftNode` is M6, so until then
//! only the tests construct these. Scoped here rather than crate-wide so real
//! dead code elsewhere still fails CI.
#![allow(dead_code)]
// Every fallible call here returns `tonic::Status`, which is ~176 bytes and so
// trips `result_large_err`. Boxing it would mean unwrapping at every tonic
// boundary for no benefit; kv-proto carries the same allow for its generated
// code. Scoped to the transport, where tonic actually lives.
#![allow(clippy::result_large_err)]

pub mod convert;
pub mod peer;
pub mod server;
