//! M7's gate.
//!
//! This test must **fail** against M6's local read before ReadIndex exists.
//! That failure is the evidence the test tests something: a test that has only
//! ever passed proves nothing about the bug it claims to cover.

use std::time::Duration;

use kv_raft::NodeId;

use crate::driver::ClientReply;
use crate::tests::cluster::{ALL, Cluster};

/// Node A leads and holds `k=v1`. A is partitioned away. B and C elect a new
/// leader and write `k=v2`. A has heard nothing, so it still believes it
/// leads — Raft leaders do not step down on their own when they lose contact.
/// A client that reaches A now reads `k`.
///
/// M6 serves that read from A's local state and returns `v1`: a value that was
/// overwritten *before* the read was issued. That is a linearizability
/// violation, not a stale cache — the write of `v2` completed, and a read that
/// began afterwards returned `v1`.
/// Against M6's local read this failed with:
///
/// ```text
/// stale read: node 3 served v1 after v2 was committed on 1
/// ```
///
/// It passes now because the deposed leader can no longer confirm a read
/// quorum, so it refuses instead of answering.
#[tokio::test]
async fn a_deposed_leader_must_not_serve_a_stale_read() {
    let cluster = Cluster::of_three();
    let old_leader = cluster.put(b"k", b"v1").await;
    let survivors: Vec<NodeId> = ALL.into_iter().filter(|n| *n != old_leader).collect();

    // Cut the leader off. The majority side keeps working.
    cluster.switchboard().isolate(&[old_leader], &ALL);
    cluster.settle(Duration::from_secs(1)).await;

    // The two survivors elect a new leader and accept a write.
    let new_leader = cluster.put_among(&survivors, b"k", b"v2").await;
    assert_ne!(new_leader, old_leader, "the write must land on the majority side");

    // The deposed leader still thinks it leads and has heard nothing.
    let reply = cluster
        .try_call(old_leader, crate::driver::ClientOp::get(b"k"), Duration::from_secs(3))
        .await;

    match reply {
        // Refusing, redirecting, or declining to answer are all acceptable:
        // none of them tells the client something untrue.
        None | Some(ClientReply::NotLeader { .. }) => {}
        Some(ClientReply::Value(Some(v))) if v == b"v2" => {}
        Some(ClientReply::Value(Some(v))) if v == b"v1" => {
            panic!("stale read: node {old_leader} served v1 after v2 was committed on {new_leader}")
        }
        other => panic!("unexpected reply {other:?}"),
    }
}
