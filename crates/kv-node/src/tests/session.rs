//! Exactly-once retries (M7, §1.8). The second of the milestone's two gates.

use std::time::Duration;

use crate::command::Mutation;
use crate::driver::{ClientOp, ClientReply};
use crate::session::RequestCtx;
use crate::tests::cluster::{ALL, Cluster};

fn cas(ctx: Option<RequestCtx>, key: &[u8], expected: Option<&[u8]>, new: &[u8]) -> ClientOp {
    ClientOp::Mutate {
        ctx,
        op: Mutation::Cas {
            key: key.to_vec(),
            expected: expected.map(|e| e.to_vec()),
            new_value: new.to_vec(),
        },
    }
}

/// Retries `op` through `NotLeader` until a leader answers.
async fn until_answered(cluster: &Cluster, op: impl Fn() -> ClientOp) -> ClientReply {
    for _ in 0..200 {
        for &id in &ALL {
            match cluster.call(id, op()).await {
                ClientReply::NotLeader { .. } => {}
                answered => return answered,
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no leader answered in 4s");
}

/// **The gate.** A client sends a `Cas`, it commits, and the reply is lost —
/// from the client's side that is indistinguishable from the leader having
/// died before answering. The client retries with the same context.
///
/// Without the session table the retry is evaluated afresh, sees the value the
/// first attempt swapped in, and answers `swapped: false`. That is not a
/// wasted write, it is a **wrong answer**: the operation did take effect and
/// the client is told it did not.
#[tokio::test]
async fn a_retried_cas_applies_exactly_once() {
    let cluster = Cluster::of_three();
    let ctx = Some(RequestCtx { client_id: 42, sequence: 1 });

    until_answered(&cluster, || ClientOp::put(b"k", b"v1")).await;

    let first = until_answered(&cluster, || cas(ctx, b"k", Some(b"v1"), b"v2")).await;
    assert!(matches!(first, ClientReply::Swapped(true)), "got {first:?}");

    // The same request again, as a retry after a lost reply.
    let retry = until_answered(&cluster, || cas(ctx, b"k", Some(b"v1"), b"v2")).await;
    assert!(
        matches!(retry, ClientReply::Swapped(true)),
        "a retry must repeat the original answer, got {retry:?}"
    );

    assert_eq!(cluster.read(b"k").await, Some(b"v2".to_vec()));
}

/// The same retry *without* a context is a different request, and gets the
/// second evaluation's answer. This is what the session table buys, stated as
/// a test so the cost of omitting the context is visible rather than implied.
#[tokio::test]
async fn a_retried_cas_without_a_context_reports_the_wrong_answer() {
    let cluster = Cluster::of_three();
    until_answered(&cluster, || ClientOp::put(b"k", b"v1")).await;

    let first = until_answered(&cluster, || cas(None, b"k", Some(b"v1"), b"v2")).await;
    assert!(matches!(first, ClientReply::Swapped(true)), "got {first:?}");

    let retry = until_answered(&cluster, || cas(None, b"k", Some(b"v1"), b"v2")).await;
    assert!(
        matches!(retry, ClientReply::Swapped(false)),
        "without a context the retry is re-evaluated, got {retry:?}"
    );
}

#[tokio::test]
async fn cas_against_an_absent_key_creates_it() {
    let cluster = Cluster::of_three();
    let reply = until_answered(&cluster, || cas(None, b"fresh", None, b"v")).await;
    assert!(matches!(reply, ClientReply::Swapped(true)), "got {reply:?}");
    assert_eq!(cluster.read(b"fresh").await, Some(b"v".to_vec()));
}

#[tokio::test]
async fn cas_declines_when_the_value_does_not_match() {
    let cluster = Cluster::of_three();
    until_answered(&cluster, || ClientOp::put(b"k", b"actual")).await;

    let reply = until_answered(&cluster, || cas(None, b"k", Some(b"guessed"), b"new")).await;
    assert!(matches!(reply, ClientReply::Swapped(false)), "got {reply:?}");
    assert_eq!(cluster.read(b"k").await, Some(b"actual".to_vec()), "it must not have written");
}

/// The table is bounded (M8): at most `cap` clients, LRU by applied index.
/// Deterministic across replicas — recency is log order, never wall-clock —
/// so every replica evicts the same client.
#[test]
fn client_count_is_bounded_by_lru_eviction() {
    use crate::session::{CommandResponse, RequestCtx, cached, record_bounded};
    use kv_storage::Engine;

    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    let cap = 4;
    for client in 0..6 {
        record_bounded(
            &mut engine,
            &RequestCtx { client_id: client, sequence: 1 },
            &CommandResponse::Applied,
            client + 1,
            cap,
        )
        .unwrap();
    }

    // The two least recently touched are gone; the rest answer from cache.
    for client in 0..2 {
        assert!(
            cached(&mut engine, &RequestCtx { client_id: client, sequence: 1 }).unwrap().is_none(),
            "client {client} must have been evicted"
        );
    }
    for client in 2..6 {
        assert!(
            cached(&mut engine, &RequestCtx { client_id: client, sequence: 1 }).unwrap().is_some(),
            "client {client} must be retained"
        );
    }
}

/// Touching a client refreshes its recency: the eviction takes the idlest,
/// not the oldest id.
#[test]
fn a_touch_refreshes_recency_against_eviction() {
    use crate::session::{CommandResponse, RequestCtx, cached, record_bounded};
    use kv_storage::Engine;

    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    let cap = 3;
    for client in 0..3 {
        record_bounded(
            &mut engine,
            &RequestCtx { client_id: client, sequence: 1 },
            &CommandResponse::Applied,
            client + 1,
            cap,
        )
        .unwrap();
    }
    // Re-touch client 0 at a newer applied index, then overflow with client 3.
    record_bounded(
        &mut engine,
        &RequestCtx { client_id: 0, sequence: 2 },
        &CommandResponse::Applied,
        10,
        cap,
    )
    .unwrap();
    record_bounded(
        &mut engine,
        &RequestCtx { client_id: 3, sequence: 1 },
        &CommandResponse::Applied,
        11,
        cap,
    )
    .unwrap();

    assert!(
        cached(&mut engine, &RequestCtx { client_id: 0, sequence: 2 }).unwrap().is_some(),
        "touched client 0 must survive"
    );
    assert!(
        cached(&mut engine, &RequestCtx { client_id: 1, sequence: 1 }).unwrap().is_none(),
        "idle client 1 must go first"
    );
}

/// Entries written before M8's recency field existed still decode — with
/// unknown age, so they are eviction-first rather than lost.
#[test]
fn entries_from_before_recency_tracking_still_decode() {
    use crate::session::{CommandResponse, RequestCtx, cached};
    use kv_storage::Engine;

    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    let mut key = vec![0x00];
    key.extend_from_slice(b"session/");
    key.extend_from_slice(&7u64.to_be_bytes());
    let old = bincode::serialize(&OldSessionEntry {
        last_sequence: 1,
        last_response: CommandResponse::Applied,
    })
    .unwrap();
    engine.put(&key, &old).unwrap();

    assert_eq!(
        cached(&mut engine, &RequestCtx { client_id: 7, sequence: 1 }).unwrap(),
        Some(CommandResponse::Applied)
    );
}

#[derive(serde::Serialize)]
struct OldSessionEntry {
    last_sequence: u64,
    last_response: crate::session::CommandResponse,
}

/// The table is replicated state, so it survives the only event it exists for.
/// A session kept beside the state machine would die with exactly the leader
/// whose death made it necessary.
#[tokio::test]
async fn the_session_table_survives_a_leader_change() {
    let cluster = Cluster::of_three();
    let ctx = Some(RequestCtx { client_id: 7, sequence: 1 });

    until_answered(&cluster, || ClientOp::put(b"k", b"v1")).await;
    let first = until_answered(&cluster, || cas(ctx, b"k", Some(b"v1"), b"v2")).await;
    assert!(matches!(first, ClientReply::Swapped(true)));

    // Take the leader away; the survivors elect a new one.
    let leader = cluster.leader_of(&ALL).await.expect("someone leads");
    let survivors: Vec<u64> = ALL.into_iter().filter(|n| *n != leader).collect();
    cluster.switchboard().isolate(&[leader], &ALL);
    cluster.settle(Duration::from_secs(1)).await;

    // The retry now lands on a node that never answered the original, and
    // must still recognise it.
    for _ in 0..200 {
        let mut answered = None;
        for &id in &survivors {
            match cluster.call(id, cas(ctx, b"k", Some(b"v1"), b"v2")).await {
                ClientReply::NotLeader { .. } => {}
                reply => answered = Some(reply),
            }
        }
        if let Some(reply) = answered {
            assert!(
                matches!(reply, ClientReply::Swapped(true)),
                "the new leader must have the session entry, got {reply:?}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the survivors never elected a leader");
}
