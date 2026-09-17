//! The portable snapshot image (M8): a sorted scan of live pairs plus the
//! log boundary, checksummed so a truncated stream never restores silently.

use crate::snapshot;

#[test]
fn snapshot_round_trips_through_encode_decode() {
    let pairs = vec![
        (b"k2".to_vec(), b"v2".to_vec()),
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"\x00session/".to_vec(), b"s".to_vec()),
    ];
    let encoded = snapshot::encode(17, 4, &pairs);
    let decoded = snapshot::decode(&encoded).unwrap();
    assert_eq!(decoded.last_included_index, 17);
    assert_eq!(decoded.last_included_term, 4);
    assert_eq!(
        decoded.pairs,
        vec![
            (b"\x00session/".to_vec(), b"s".to_vec()),
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ],
        "encode sorts by key: any two replicas snapshotting the same state produce the same bytes"
    );
}

#[test]
fn decode_rejects_a_truncated_or_tampered_payload() {
    let pairs = vec![(b"k".to_vec(), b"v".to_vec())];
    let mut encoded = snapshot::encode(3, 1, &pairs);
    encoded.pop();
    assert!(snapshot::decode(&encoded).is_err(), "truncation must not decode");

    let mut encoded = snapshot::encode(3, 1, &pairs);
    let last = encoded.len() - 1;
    encoded[last] ^= 0xff;
    assert!(snapshot::decode(&encoded).is_err(), "tampering must fail the checksum");

    assert!(snapshot::decode(b"garbage").is_err());
}

/// **The M8 gate.** A node partitioned away while the group commits past its
/// `next_index` — far enough that the leader compacts the prefix out from
/// under it — catches up via snapshot after the heal and rejoins serving the
/// same state.
///
/// Reads alone cannot prove this: the leader serves them whether or not the
/// victim ever caught up. So the test also reads the traffic log — a snapshot
/// must have gone to the victim, the victim must have answered it
/// successfully, and tail appends must have flowed after it.
#[tokio::test]
async fn a_partitioned_follower_catches_up_via_snapshot_and_rejoins() {
    use std::time::Duration;

    use crate::tests::cluster::{ALL, Cluster, Traffic};

    let cluster = Cluster::with_snapshots(5);
    cluster.put(b"seed", b"0").await;
    let leader = cluster.leader_of(&ALL).await.expect("someone leads");
    let victim = *ALL.iter().find(|id| **id != leader).expect("a follower");
    let survivors: Vec<u64> = ALL.into_iter().filter(|n| *n != victim).collect();

    cluster.switchboard().isolate(&[victim], &ALL);
    for i in 0..12u32 {
        cluster.put_among(&survivors, format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).await;
    }

    cluster.switchboard().heal();
    cluster.settle(Duration::from_secs(3)).await;

    for i in 0..12u32 {
        let got = cluster.read(format!("k{i}").as_bytes()).await;
        assert_eq!(got, Some(format!("v{i}").as_bytes().to_vec()), "k{i} lost");
    }

    let traffic = cluster.switchboard().traffic();
    assert!(
        traffic.iter().any(|(_, to, k)| *to == victim && *k == Traffic::InstallSnapshot),
        "no snapshot ever went to the victim"
    );
    assert!(
        traffic.iter().any(|(from, _, k)| *from == victim
            && matches!(k, Traffic::InstallSnapshotResp { success: true })),
        "the victim never confirmed an install"
    );
    assert!(
        traffic.iter().any(|(_, to, k)| *to == victim
            && matches!(k, Traffic::AppendEntries { entries: n } if *n > 0)),
        "no log tail flowed to the victim after its snapshot"
    );
}

#[test]
fn an_empty_state_is_a_valid_snapshot() {
    let decoded = snapshot::decode(&snapshot::encode(9, 2, &[])).unwrap();
    assert_eq!(decoded.last_included_index, 9);
    assert!(decoded.pairs.is_empty());
}
