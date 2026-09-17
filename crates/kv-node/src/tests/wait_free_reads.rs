//! M11.5's gate: a read may be served only from state a `publish` has made
//! visible.
//!
//! With the keydir behind `left-right`, applying an entry and making it
//! readable are two different events. `applied_index` moves at the first;
//! readers are on the other copy until the second. §1.15 calls waiting on the
//! wrong one "the correctness trap", and it is: a `Get` served on
//! `applied_index` can miss a `Put` that has already been acknowledged.
//!
//! **Why these tests are deterministic and not a race.** `Engine::publish`
//! will not stall the driver waiting for a reader to leave the copy it would
//! overwrite — it declines and retries next drain. So parking a reader inside
//! that copy holds the window open for as long as the test likes. What the
//! test then exercises is the ordinary path, not a special one: the driver
//! applies, acknowledges, fails to publish, and answers reads.

use std::time::Duration;

use crate::tests::cluster::{ALL, Cluster};

/// The named regression test. A `Get` issued after a `Put` has returned must
/// never miss it — however long visibility lags application.
///
/// Against the `applied_index` wait this fails with `None`: the read is
/// confirmed at an index the driver has applied, so it is dispatched at once,
/// and the reader resolving it is on a keydir copy that predates the write.
/// Against the `published_index` wait the read waits for the publish it needs
/// and then returns the value.
#[tokio::test(flavor = "multi_thread")]
async fn a_get_after_a_put_returns_never_misses_it() {
    let cluster = Cluster::of_three();

    // Park a reader on every node before the first publish that can record
    // its epoch. left-right waits only for readers that entered before the
    // last swap, so a hold taken after one would not defer the next.
    let holds: Vec<_> = ALL.iter().map(|&id| cluster.hold_read_copy(id)).collect();

    // Elects a leader and takes the one publish the holds are recorded by.
    cluster.put(b"warm", b"v").await;

    // Acknowledged, and now invisible: the publish covering it is deferred.
    cluster.put(b"k", b"v1").await;
    // The premise, asserted directly against what a reader can see rather
    // than through a `Get` — a `Get` is the thing under test, and with the
    // correct wait it would block here rather than answer.
    for &id in &ALL {
        assert_eq!(
            cluster.read_view(id).get(b"k").unwrap(),
            None,
            "node {id}: the write is applied and acknowledged but must not yet be visible"
        );
    }

    // The read the gate is about. In flight while the window is open.
    let client = cluster.client();
    let read = tokio::spawn(async move { client.read_among(&ALL, b"k").await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The readers depart, so the next drain publishes and the read can be
    // answered. Dropping them here rather than before the read is what keeps
    // the failing case failing.
    drop(holds);

    let value = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("the read is answered once the publish goes through")
        .expect("the read task does not panic");
    assert_eq!(value, Some(b"v1".to_vec()), "a Get after a Put returned must not miss it");
}

/// The same trap on the zero-round-trip path. A lease read skips the quorum
/// confirmation, not the visibility rule — it may only answer from what is
/// published, and must fall back to a full ReadIndex round if it cannot.
#[tokio::test(flavor = "multi_thread")]
async fn a_lease_read_waits_for_visibility_too() {
    let cluster = Cluster::of_three_with(true);
    let holds: Vec<_> = ALL.iter().map(|&id| cluster.hold_read_copy(id)).collect();

    cluster.put(b"warm", b"v").await;
    cluster.put(b"k", b"leased").await;

    let client = cluster.client();
    let read = tokio::spawn(async move { client.read_among(&ALL, b"k").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(holds);

    let value = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .expect("the read is answered once the publish goes through")
        .expect("the read task does not panic");
    assert_eq!(value, Some(b"leased".to_vec()));
}

/// A deferred publish must not lose a read: once the reader leaves, the next
/// drain publishes and everything waiting is answered. The driver retries on
/// its own tick, with nothing having to prompt it — no write arrives here
/// after the holds are dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_deferred_publish_is_retried_without_another_write() {
    let cluster = Cluster::of_three();
    let holds: Vec<_> = ALL.iter().map(|&id| cluster.hold_read_copy(id)).collect();

    cluster.put(b"warm", b"v").await;
    for i in 0..5u32 {
        cluster.put(format!("k{i}").as_bytes(), b"v").await;
    }

    let client = cluster.client();
    let reads = tokio::spawn(async move {
        let mut values = Vec::new();
        for i in 0..5u32 {
            values.push(client.read_among(&ALL, format!("k{i}").as_bytes()).await);
        }
        values
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(holds);

    let values = tokio::time::timeout(Duration::from_secs(5), reads)
        .await
        .expect("every read is answered")
        .expect("the read task does not panic");
    assert_eq!(values, vec![Some(b"v".to_vec()); 5]);
}

/// Reads are resolved off the driver now, so many of them proceed at once
/// rather than one per pass of the `select!` loop. This asserts the contract
/// that makes that safe: whatever the concurrency, every read sees at least
/// the write that preceded it.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_all_see_the_write_that_preceded_them() {
    let cluster = Cluster::of_three();
    cluster.put(b"k", b"v").await;

    let mut readers = Vec::new();
    for _ in 0..16 {
        let client = cluster.client();
        readers.push(tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..8 {
                seen.push(client.read_among(&ALL, b"k").await);
            }
            seen
        }));
    }

    for reader in readers {
        let seen = tokio::time::timeout(Duration::from_secs(10), reader)
            .await
            .expect("every reader finishes")
            .expect("no reader panics");
        assert_eq!(seen, vec![Some(b"v".to_vec()); 8]);
    }
}

/// `--keydir locked` is a supported configuration, not a benchmark fixture:
/// the whole node has to work on it. Its writes are visible as soon as they
/// return, so `visible` tracks `applied` exactly and no read ever waits — the
/// same linearizability contract reached a different way.
#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_on_the_locked_keydir_reads_and_writes() {
    let cluster = Cluster::of_three_on(kv_storage::IndexKind::Locked);

    cluster.put(b"k", b"v1").await;
    assert_eq!(cluster.read(b"k").await, Some(b"v1".to_vec()));
    cluster.put(b"k", b"v2").await;
    assert_eq!(cluster.read(b"k").await, Some(b"v2".to_vec()));
    assert_eq!(cluster.read(b"absent").await, None);

    // No publish step to defer, so a parked reader changes nothing: the hold
    // is a no-op on this arm and reads keep being answered.
    let holds: Vec<_> = ALL.iter().map(|&id| cluster.hold_read_copy(id)).collect();
    cluster.put(b"k", b"v3").await;
    assert_eq!(cluster.read(b"k").await, Some(b"v3".to_vec()));
    drop(holds);
}
