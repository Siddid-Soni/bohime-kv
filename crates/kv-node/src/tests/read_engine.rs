//! The read path's I/O engine (see `crate::read_engine`).
//!
//! Both backends are exercised explicitly. The `io_uring` one is skipped where
//! the kernel will not give us a ring — that is a supported deployment, not a
//! broken machine — but the fallback is tested unconditionally, because it is
//! what runs wherever seccomp or `io_uring_disabled` takes the ring away.

use kv_storage::Engine;
use tokio::sync::oneshot;

use crate::driver::ClientReply;
use crate::read_engine::ReadEngine;

/// An engine holding `count` keys, `k{i}` -> `v{i}`.
fn engine_with_keys(dir: &std::path::Path, count: u32) -> Engine {
    let mut engine = Engine::open(dir).unwrap();
    for i in 0..count {
        engine.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    engine
}

/// Submits every key at once and waits for all of them, so the reads really do
/// overlap rather than proceeding one at a time. With `io_uring` this is a
/// batch of submissions against one ring on one thread — the whole point of
/// the engine.
async fn answers_every_key(reads: &ReadEngine, engine: &Engine, count: u32) {
    let mut waits = Vec::new();
    for i in 0..count {
        let located = engine.locate(format!("k{i}").as_bytes()).expect("key is live");
        let (reply, wait) = oneshot::channel();
        reads.submit(located, reply);
        waits.push((i, wait));
    }

    for (i, wait) in waits {
        match wait.await.expect("the engine must answer every read") {
            ClientReply::Value(value) => {
                assert_eq!(value, Some(format!("v{i}").into_bytes()), "key k{i}");
            }
            other => panic!("expected a value for k{i}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_fallback_answers_many_overlapping_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_keys(dir.path(), 200);

    let reads = ReadEngine::blocking();
    answers_every_key(&reads, &engine, 200).await;
}

#[tokio::test]
async fn io_uring_answers_many_overlapping_reads() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_keys(dir.path(), 200);

    let Some(reads) = ReadEngine::io_uring() else {
        // No ring here: seccomp, `io_uring_disabled`, or an old kernel. A
        // supported deployment, so this is not a failure.
        eprintln!("io_uring unavailable; fallback is covered by the test above");
        return;
    };
    answers_every_key(&reads, &engine, 200).await;
}

/// A missing key never reaches the I/O engine — `locate` returns `None` and
/// the driver answers directly — so the engine only ever sees live values.
/// This pins the other half: a value written and immediately located must read
/// back through the engine, with no flush in between.
#[tokio::test]
async fn a_value_is_readable_through_the_engine_before_any_sync() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    engine.put(b"k", b"unsynced").unwrap();

    let reads = ReadEngine::new();
    let located = engine.locate(b"k").expect("k is live");
    let (reply, wait) = oneshot::channel();
    reads.submit(located, reply);

    match wait.await.unwrap() {
        ClientReply::Value(value) => assert_eq!(value, Some(b"unsynced".to_vec())),
        other => panic!("expected a value, got {other:?}"),
    }
}

/// Operators must be able to turn `io_uring` off without shipping a different
/// binary. It has a CVE history, and the usual response to one is to disable
/// it fleet-wide that day; a database that can only be reconfigured by
/// redeploying is not much use in that hour.
///
/// Safe to set the environment here because nextest runs each test in its own
/// process.
#[test]
fn the_io_uring_path_can_be_disabled_by_configuration() {
    unsafe { std::env::set_var("BOHIME_READ_ENGINE", "blocking") };
    assert!(
        matches!(ReadEngine::new(), ReadEngine::Blocking(_)),
        "an explicit request for the fallback must be honoured even where a ring is available"
    );
}

/// An unrecognised value must not silently pick something. Getting this wrong
/// means a typo in a deployment config quietly selects a different I/O path.
#[test]
fn an_unknown_read_engine_setting_is_rejected() {
    assert!(ReadEngine::from_setting(Some("uring")).is_err());
    assert!(ReadEngine::from_setting(Some("blocking")).is_ok());
    assert!(ReadEngine::from_setting(Some("io_uring")).is_ok());
    assert!(ReadEngine::from_setting(Some("auto")).is_ok());
    assert!(ReadEngine::from_setting(None).is_ok());
}
