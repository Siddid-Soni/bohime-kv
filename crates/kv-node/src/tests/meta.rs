//! The meta group: a second Raft group inside the same process (M10.5).
//!
//! This is the milestone where `kv-raft` gets used as a library *twice*, for
//! two different state machines, in one node. Everything here is about the two
//! groups staying genuinely separate — separate logs, separate state machines,
//! separate directories — while sharing a process, a listener and every peer
//! address.

use std::time::Duration;

use crate::driver::ClientOp;
use crate::tests::cluster::{ALL, Cluster};
use crate::tests::support::config_in;

/// Each Bitcask instance owns a key space, and one keydir cannot hold two.
/// The data group's log already had to be kept apart from its state machine
/// for exactly this reason; a second group doubles the number of ways to get
/// it wrong.
#[test]
fn the_two_groups_never_share_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());

    let dirs = [
        config.shard_raft_dir(0),
        config.shard_state_dir(0),
        config.meta_raft_dir(),
        config.meta_state_dir(),
    ];
    for (i, a) in dirs.iter().enumerate() {
        for b in dirs.iter().skip(i + 1) {
            assert_ne!(a, b, "two Bitcask instances would share {}", a.display());
        }
    }
}

/// Both groups must reach a leader on their own. They share a node id and a
/// peer set, so a bug that routed one group's votes into the other would show
/// up here as one group never electing at all.
#[tokio::test]
async fn both_groups_elect_a_leader() {
    let cluster = Cluster::of_three();

    let data = cluster.leader_of(&ALL).await.expect("the data group elects");
    let meta = cluster.meta_leader_of(&ALL).await.expect("the meta group elects");

    // They need not be the same node — two independent elections — but both
    // must be real members.
    assert!(ALL.contains(&data), "data leader {data} is not a member");
    assert!(ALL.contains(&meta), "meta leader {meta} is not a member");
}

/// The two groups replicate different things. A write to one must leave the
/// other's log and state machine untouched; if they shared either, this is
/// where it would show.
#[tokio::test]
async fn the_groups_keep_independent_logs() {
    let cluster = Cluster::of_three();
    // Past the reconciler's bootstrap entry, so the baseline is a settled meta
    // log rather than one that is still being written.
    cluster.await_shard_map(Duration::from_secs(10)).await.expect("a map");

    let meta_before = cluster.meta_status_of(1).await.log_last_index;

    for i in 0..20u32 {
        cluster.put(format!("k{i}").as_bytes(), b"v").await;
    }

    let data = cluster.status_of(1).await;
    let meta_after = cluster.meta_status_of(1).await;

    assert!(
        data.log_last_index >= 20,
        "the data group should have grown by the writes, got {}",
        data.log_last_index
    );
    assert_eq!(
        meta_after.log_last_index, meta_before,
        "20 data writes reached the meta group's log"
    );
}

/// ...and the state machines are separate too, which is the half a shared
/// *log* check would miss.
#[tokio::test]
async fn a_data_write_is_invisible_to_the_meta_state_machine() {
    let cluster = Cluster::of_three();
    cluster.meta_leader_of(&ALL).await.expect("the meta group elects");
    cluster.put(b"only-in-data", b"value").await;

    assert_eq!(cluster.read(b"only-in-data").await, Some(b"value".to_vec()));

    let meta = cluster.meta_leader_of(&ALL).await.unwrap();
    let reply = cluster.meta_call(meta, ClientOp::get(b"only-in-data")).await;
    assert!(
        matches!(reply, crate::driver::ClientReply::Value(None)),
        "the meta state machine can see the data group's keys: {reply:?}"
    );
}

/// The meta group writes when placement changes and not otherwise, so on a
/// settled cluster its log stops growing — the property that makes hosting a
/// second Raft group on every node cheap.
///
/// It is not *silent*: bootstrapping version 1 of the map is one entry, and a
/// leader's no-op is another. What matters is that it goes quiet afterwards,
/// rather than the reconciler re-proposing an identical map every interval and
/// snapshotting it forever.
#[tokio::test]
async fn the_meta_group_goes_quiet_once_the_map_is_settled() {
    let cluster = Cluster::of_three();
    cluster.await_shard_map(Duration::from_secs(10)).await.expect("a map");

    let first = cluster.meta_status_of(1).await.log_last_index;
    // Many reconcile intervals: the harness runs them every 20ms.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let second = cluster.meta_status_of(1).await.log_last_index;

    assert_eq!(first, second, "the settled meta group is still appending entries");
}
