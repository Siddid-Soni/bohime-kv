//! Reading the shard map (M10.8), in the two ways it can be read.
//!
//! The distinction is the whole point of putting the map in a Raft group
//! rather than in a config file:
//!
//! - the **published** copy is what every request handler reads through the
//!   `ArcSwap`, on every request, from any replica. It is stale by at most
//!   this node's apply lag, and that is fine for routing: a node that routes
//!   by a map one version old sends the request to a node that will redirect
//!   it.
//! - the **linearizable** copy goes through the meta group's ReadIndex, costs
//!   a quorum round trip, and is what M10's third ✅ criterion is about. A
//!   deposed leader must not be able to answer one.

use std::time::Duration;

use kv_raft::NodeId;

use crate::driver::{ClientOp, ClientReply};
use crate::shard_map::SHARD_MAP_KEY;
use crate::tests::cluster::{ALL, Cluster};

const PATIENCE: Duration = Duration::from_secs(10);

/// A linearizable read of the map through the meta group, as `AdminService`
/// issues it.
async fn read_map_from(cluster: &Cluster, id: NodeId, within: Duration) -> Option<ClientReply> {
    cluster.try_meta_call(id, ClientOp::Get { key: SHARD_MAP_KEY.to_vec() }, within).await
}

/// Every replica serves the published copy, leader or not — that is what makes
/// it usable on the request path.
#[tokio::test]
async fn every_replica_serves_the_published_map() {
    let cluster = Cluster::of_three();
    cluster.await_shard_map(PATIENCE).await.expect("a map");

    for &id in &ALL {
        let map = cluster
            .await_shard_map_on(id, PATIENCE)
            .await
            .unwrap_or_else(|| panic!("node {id} published nothing"));
        assert_eq!(map.nodes, ALL.into_iter().collect(), "node {id}");
    }
}

/// A linearizable read is a leader's answer or no answer at all. A follower
/// redirects rather than serving its own applied copy — which it has, and
/// which is very probably correct, and which it still may not present as
/// linearizable.
#[tokio::test]
async fn a_linearizable_read_redirects_on_a_follower() {
    let cluster = Cluster::of_three();
    cluster.await_shard_map(PATIENCE).await.expect("a map");
    let leader = cluster.meta_leader_of(&ALL).await.expect("the meta group elects");

    for &id in ALL.iter().filter(|&&n| n != leader) {
        match read_map_from(&cluster, id, Duration::from_secs(3)).await {
            Some(ClientReply::NotLeader { .. }) | None => {}
            other => panic!("follower {id} answered a linearizable map read with {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_meta_leader_answers_a_linearizable_read() {
    let cluster = Cluster::of_three();
    let published = cluster.await_shard_map(PATIENCE).await.expect("a map");
    let leader = cluster.meta_leader_of(&ALL).await.expect("the meta group elects");

    match read_map_from(&cluster, leader, PATIENCE).await {
        Some(ClientReply::Value(Some(bytes))) => {
            assert_eq!(
                bytes,
                published.encode(),
                "the linearizable map differs from the published one"
            );
        }
        other => panic!("the meta leader answered with {other:?}"),
    }
}

/// M10's third ✅ criterion: **the shard map is itself linearizable**.
///
/// The same shape as M7's `a_deposed_leader_must_not_serve_a_stale_read`, and
/// for the same reason — a leader that has been partitioned away still
/// believes it leads, and the only thing stopping it answering from its own
/// state is the quorum confirmation ReadIndex forces. Here the stale value
/// would be a *placement*, so serving it would send a client's writes to a
/// group that no longer owns the shard.
#[tokio::test]
async fn a_deposed_meta_leader_must_not_serve_a_stale_map() {
    let mut cluster = Cluster::of_three();
    let before = cluster.await_shard_map(PATIENCE).await.expect("a map");
    let old_leader = cluster.meta_leader_of(&ALL).await.expect("the meta group elects");
    let survivors: Vec<NodeId> = ALL.into_iter().filter(|&n| n != old_leader).collect();

    // Cut the meta leader off. The majority side keeps working and will
    // republish the map when the data cluster grows.
    cluster.switchboard().isolate(&[old_leader], &ALL);
    cluster.settle(Duration::from_secs(1)).await;

    // Grow the cluster on the majority side, so the map genuinely moves on.
    admit_on(&mut cluster, &survivors, 4).await;
    let after = await_version_past(&cluster, &survivors, before.version).await;
    assert!(after > before.version, "the surviving side never republished the map");

    // The deposed leader still thinks it leads and has heard none of it.
    match read_map_from(&cluster, old_leader, Duration::from_secs(3)).await {
        // Refusing, redirecting, or declining to answer are all honest.
        None | Some(ClientReply::NotLeader { .. }) => {}
        Some(ClientReply::Value(Some(bytes))) => {
            let served = kv_ring::ShardMap::decode(&bytes).expect("a map");
            assert!(
                served.version >= after,
                "stale read: deposed meta leader {old_leader} served version {} \
                 after version {after} was committed",
                served.version
            );
        }
        other => panic!("unexpected reply {other:?}"),
    }
}

async fn admit_on(cluster: &mut Cluster, among: &[NodeId], id: NodeId) {
    use crate::driver::{AdminOp, AdminReply};
    cluster.start_joining_node(id, &ALL);
    let reply = cluster
        .administer_meta(among, || AdminOp::AddNode { id, address: format!("in-process://{id}") })
        .await;
    assert!(matches!(reply, AdminReply::Accepted { .. }), "admitting {id}: {reply:?}");
}

/// The highest map version any of `among` has published, once it exceeds
/// `floor`.
async fn await_version_past(cluster: &Cluster, among: &[NodeId], floor: u64) -> u64 {
    let deadline = std::time::Instant::now() + PATIENCE;
    while std::time::Instant::now() < deadline {
        for &id in among {
            if let Some(map) = cluster.await_shard_map_on(id, Duration::from_millis(50)).await
                && map.version > floor
            {
                return map.version;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    floor
}
