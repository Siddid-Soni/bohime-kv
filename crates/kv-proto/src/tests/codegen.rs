use crate::{admin, kv, raft};

#[test]
fn generated_types_are_reachable() {
    // Smoke test: if codegen ran (M0's CI gate), these types exist and are
    // constructible with their defaults.
    let _ = raft::RequestVoteRequest::default();
    let _ = kv::GetRequest::default();
    let _ = admin::ClusterStatusRequest::default();
}
