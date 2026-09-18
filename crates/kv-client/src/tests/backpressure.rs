//! What the client does to a node that is busy rather than broken
//! (2026-09-18).
//!
//! `kv_service.rs` sheds a full request queue as `ResourceExhausted` instead
//! of awaiting it, and a saturated driver can take longer than the client's
//! deadline to answer. Neither of those means the connection is bad or that
//! the node has stopped being the leader — but the client had one error arm
//! for every `tonic::Status`, and it called `forget`, which drops the cached
//! channel *and* the per-shard leader cache.
//!
//! So load put every client back into cold start: redial, re-search the
//! candidate list, arrive at the same saturated node, repeat. Offered load
//! rose exactly when the system was least able to absorb it. Measured on
//! `kv-node`'s cluster benchmark at 64 shards: 138 write op/s at 32 clients,
//! 140 at 128, **93 at 256** with p99 going 394 ms → 1.50 s → 28.3 s, and the
//! client-side probe showing 572 extra dials against 572 errors. Past 512
//! clients the whole arm failed outright.
//!
//! These tests hold the distinction that fixes it: **reachable-but-refusing
//! keeps its channel, unreachable does not.**

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::tests::fake_node::{Answer, cluster};

/// One connection per node, which is what a client that never redials looks
/// like. `Client` dials a node at most once unless something drops it.
const ONE_DIAL: usize = 1;

/// A node slower than the deadline is still the leader and still connected.
///
/// The client has to give up on the *attempt* — a deadline it never enforced
/// would be a hung client — but giving up on the attempt must not cost it the
/// connection. Redialling is the most expensive thing it could do to a node
/// that is already saturated.
#[tokio::test]
async fn a_slow_node_is_not_redialled() {
    // Two calls slower than the deadline, then normal service.
    let (endpoints, nodes) =
        cluster(&[Answer::Slow { calls: 2, delay: Duration::from_millis(500) }]).await;
    let mut client = crate::Client::new(endpoints).with_request_timeout(Duration::from_millis(100));

    let got = client.get(b"k").await.expect("the node answers once it is no longer slow");

    assert_eq!(got, Some(b"v".to_vec()));
    assert_eq!(
        nodes[0].accepts.load(Ordering::Relaxed),
        ONE_DIAL,
        "a node that was merely slow got redialled: the deadline was treated as a dead connection",
    );
}

/// The same for explicit backpressure, which is the answer the server
/// actually sends under load.
#[tokio::test]
async fn a_shed_request_does_not_cost_the_connection() {
    let (endpoints, nodes) = cluster(&[Answer::Busy { calls: 3 }]).await;
    let mut client = crate::Client::new(endpoints);

    let got = client.get(b"k").await.expect("the node answers once its queue drains");

    assert_eq!(got, Some(b"v".to_vec()));
    assert_eq!(
        nodes[0].accepts.load(Ordering::Relaxed),
        ONE_DIAL,
        "a shed request was treated as a transport failure and the channel was redialled",
    );
}

/// The write path, because that is where the collapse was measured. A `Put`
/// carries a session context and is the expensive half; it must classify a
/// refusal exactly as a `Get` does.
#[tokio::test]
async fn a_write_to_a_busy_node_does_not_cost_the_connection() {
    let (endpoints, nodes) = cluster(&[Answer::Busy { calls: 3 }]).await;
    let mut client = crate::Client::new(endpoints);

    client.put(b"k", b"v").await.expect("the node accepts once its queue drains");

    assert_eq!(
        nodes[0].accepts.load(Ordering::Relaxed),
        ONE_DIAL,
        "a shed write was treated as a transport failure and the channel was redialled",
    );
}

/// The other half, and the reason `forget` exists at all: a node that is
/// actually gone **must** lose its channel, or the client reuses a socket to
/// a process that is not there.
///
/// Node 1 is `Unavailable` forever and node 2 answers, so the request
/// succeeds either way — what is under test is that node 1 was not kept.
#[tokio::test]
async fn an_unreachable_node_is_still_forgotten() {
    let (endpoints, _) = cluster(&[Answer::Dead, Answer::Value]).await;
    let mut client = crate::Client::new(endpoints);

    let got = client.get(b"k").await.expect("the reachable node answers");

    assert_eq!(got, Some(b"v".to_vec()));
    assert!(
        !client.holds_channel(1),
        "an `Unavailable` node kept its channel: the client will reuse a socket to a process \
         that is gone",
    );
}

/// Keeping the connection must not turn the retry loop into a hot loop
/// against the node that is already overloaded.
///
/// A node that sheds forever is the shape of a cluster under sustained
/// overload. The client's whole retry budget has to go on waiting rather than
/// on round trips, and the wait has to *grow* — a fixed pause against a
/// saturated server is still `RETRY_ROUNDS` requests it did not need.
#[tokio::test]
async fn sustained_backpressure_backs_off_rather_than_spinning() {
    let (endpoints, nodes) = cluster(&[Answer::Busy { calls: usize::MAX }]).await;
    let mut client = crate::Client::new(endpoints);

    let _ = tokio::time::timeout(Duration::from_millis(600), client.get(b"k")).await;
    let requests = nodes[0].requests.load(Ordering::Relaxed);

    // Unbacked-off at `RETRY_PAUSE` this is ~12 requests in 600 ms and never
    // fewer; with growth it is a handful. The bound is deliberately loose:
    // what is being pinned is that the interval grows, not its exact schedule.
    assert!(requests <= 8, "sustained backpressure did not back off: {requests} requests in 600ms");
    assert_eq!(
        nodes[0].accepts.load(Ordering::Relaxed),
        ONE_DIAL,
        "sustained backpressure redialled",
    );
}
