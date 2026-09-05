//! Generated gRPC/protobuf types for FerroKV, compiled from `proto/*.proto`
//! by `build.rs`. This crate is codegen only — no hand-written logic.

// tonic-build's generated client methods return `tonic::Status` as the Err
// variant, which clippy flags as large since it's `include!`d inline into
// this crate rather than compiled as a separate one. Nothing to fix here —
// it's tonic's own generated code.
#![allow(clippy::result_large_err)]

pub mod raft {
    tonic::include_proto!("ferrokv.raft");
}

pub mod kv {
    tonic::include_proto!("ferrokv.kv");
}

pub mod admin {
    tonic::include_proto!("ferrokv.admin");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_types_are_reachable() {
        // Smoke test: if codegen ran (M0's CI gate), these types exist and are
        // constructible with their defaults.
        let _ = raft::RequestVoteRequest::default();
        let _ = kv::GetRequest::default();
        let _ = admin::ClusterStatusRequest::default();
    }
}
