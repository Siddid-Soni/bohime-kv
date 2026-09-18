//! What a redirect costs (2026-09-18).
//!
//! The client's retry loop has one pause in it, and it exists for one case:
//! nobody could serve this request, so hammering the cluster helps nobody.
//! A `NotLeader` that carries a *hint* is the opposite case — a node just
//! answered and said where to go — and sleeping before taking it up turns
//! every leader-cache miss into a `RETRY_PAUSE`.
//!
//! That is not a corner: a fresh client has to learn one leader per shard it
//! touches, so the cost is `shards * RETRY_PAUSE` spread over the client's
//! first pass. It is what made the cluster benchmark's read arm appear to
//! fall 6.5x between 1 and 64 shards while the servers were idle.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::tests::fake_node::{Answer, cluster};

/// The regression test. Node 1 refuses and names node 2; node 2 answers.
///
/// Two round trips on loopback, and nothing else has to happen — so this must
/// finish inside `RETRY_PAUSE`, with an order of magnitude to spare. Against
/// the unconditional pause it takes `RETRY_PAUSE` plus those two round trips,
/// every time.
#[tokio::test]
async fn a_leader_hint_is_followed_without_the_retry_pause() {
    let (endpoints, _) = cluster(&[Answer::Hint(2), Answer::Value]).await;
    let mut client = crate::Client::new(endpoints);

    let start = Instant::now();
    let got = client.get(b"k").await.expect("the hinted node answers");
    let elapsed = start.elapsed();

    assert_eq!(got, Some(b"v".to_vec()));
    assert!(
        elapsed < crate::RETRY_PAUSE,
        "following a hint took {elapsed:?}, which is at least one RETRY_PAUSE ({:?}): \
         the redirect slept before taking up an answer it already had",
        crate::RETRY_PAUSE,
    );
}

/// The other half: skipping the pause must not turn the loop into a spin.
///
/// Two nodes that point at each other is what a client sees for a moment
/// after a failover, and it is a hint every round forever. The budget has to
/// go on pacing itself rather than on round trips.
#[tokio::test]
async fn nodes_pointing_at_each_other_still_pace_themselves() {
    let (endpoints, nodes) = cluster(&[Answer::Hint(2), Answer::Hint(1)]).await;
    let mut client = crate::Client::new(endpoints);

    // Long enough for several pauses, far short of the whole retry budget.
    let _ = tokio::time::timeout(Duration::from_millis(400), client.get(b"k")).await;
    let gets: usize = nodes.iter().map(|n| n.requests.load(Ordering::Relaxed)).sum();

    // Unpaced, two loopback round trips a round would be thousands. Paced,
    // it is a handful of pause-free hops and then one round per pause.
    assert!(gets < 100, "the retry loop spun: {gets} gets in 400ms");
}

/// And a node that refuses with no hint at all still pays the pause, which is
/// the case the pause was always for.
#[tokio::test]
async fn a_hintless_refusal_still_pauses() {
    let (endpoints, nodes) = cluster(&[Answer::Blind, Answer::Blind]).await;
    let mut client = crate::Client::new(endpoints);

    let _ = tokio::time::timeout(Duration::from_millis(400), client.get(b"k")).await;
    let gets: usize = nodes.iter().map(|n| n.requests.load(Ordering::Relaxed)).sum();

    assert!(gets < 100, "a hintless refusal stopped pausing: {gets} gets in 400ms");
}
