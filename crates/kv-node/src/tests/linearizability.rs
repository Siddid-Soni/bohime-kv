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

/// What lease reads cost, stated as a test rather than as a caveat in a doc
/// comment.
///
/// With `--lease-reads` a leader that was confirmed recently serves reads with
/// no round trip. Inside that window a partition is invisible to it: it has
/// not been contradicted, so it answers from local state — and can hand back a
/// value the majority side has already overwritten. ReadIndex cannot do this,
/// because it confirms per read.
///
/// The trade is sound when clock drift is genuinely bounded, and that is
/// exactly the assumption ReadIndex declines to make. Hence opt-in, and hence
/// this test: the cost should be demonstrable, not merely disclosed.
#[tokio::test]
async fn a_lease_read_can_be_stale_inside_its_window() {
    let cluster = Cluster::of_three_with(true);
    let old_leader = cluster.put(b"k", b"v1").await;
    let survivors: Vec<NodeId> = ALL.into_iter().filter(|n| *n != old_leader).collect();

    // Establish the lease: one confirmed read is what renews it.
    assert!(matches!(
        cluster.get_from(old_leader, b"k").await,
        ClientReply::Value(Some(ref v)) if v == b"v1"
    ));

    cluster.switchboard().isolate(&[old_leader], &ALL);
    cluster.put_among(&survivors, b"k", b"v2").await;

    // Inside the lease window the deposed leader answers from local state. It
    // may return the stale v1 — that is the documented hazard. It may also
    // have had its lease lapse already, which is fine; what must never happen
    // is an answer that is neither.
    match cluster.get_from(old_leader, b"k").await {
        ClientReply::Value(Some(ref v)) if v == b"v1" => {}
        ClientReply::NotLeader { .. } => {}
        other => panic!("unexpected reply {other:?}"),
    }

    // Once the lease lapses it must stop answering: the fallback is ReadIndex,
    // which cannot confirm a quorum from the minority side.
    cluster.settle(Duration::from_secs(2)).await;
    let reply = cluster
        .try_call(old_leader, crate::driver::ClientOp::get(b"k"), Duration::from_secs(3))
        .await;
    assert!(
        matches!(reply, None | Some(ClientReply::NotLeader { .. })),
        "an expired lease must fall back to ReadIndex, got {reply:?}"
    );
}
