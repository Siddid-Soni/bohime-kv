//! Generated gRPC/protobuf types for Bohime, compiled from `proto/*.proto`
//! by `build.rs`. This crate is codegen only — no hand-written logic.

// tonic-build's generated client methods return `tonic::Status` as the Err
// variant, which clippy flags as large since it's `include!`d inline into
// this crate rather than compiled as a separate one. Nothing to fix here —
// it's tonic's own generated code.
#![allow(clippy::result_large_err)]

pub mod raft {
    tonic::include_proto!("bohime.raft");
}

pub mod kv {
    tonic::include_proto!("bohime.kv");
}

pub mod admin {
    tonic::include_proto!("bohime.admin");
}

#[cfg(test)]
mod tests;
