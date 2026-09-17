//! The map following the cluster (M10.7).
//!
//! The ring is a pure function and `kv-ring`'s proptests already pin what it
//! computes. What these tests are about is the *loop*: a node joins the data
//! group, and some time later every node is serving a new version of the map
//! that includes it — without an operator asking for a rebalance, and without
//! reshuffling shards between the nodes that were already there.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use kv_raft::NodeId;
use kv_ring::ShardMap;

use crate::driver::{AdminOp, AdminReply};
use crate::tests::cluster::{ALL, Cluster};

const PATIENCE: Duration = Duration::from_secs(10);

async fn map_of(cluster: &Cluster) -> Arc<ShardMap> {
    cluster.await_shard_map(PATIENCE).await.expect("a map is published")
}

/// Waits until node 1 publishes a map whose node set is exactly `want`.
async fn await_map_over(cluster: &Cluster, want: &[NodeId]) -> Option<Arc<ShardMap>> {
    let want: BTreeSet<NodeId> = want.iter().copied().collect();
    let deadline = std::time::Instant::now() + PATIENCE;
    while std::time::Instant::now() < deadline {
        if let Some(map) = cluster.await_shard_map(PATIENCE).await
            && map.nodes == want
        {
            return Some(map);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

/// Admits `id` to the cluster — which since M11 means to the **meta** group,
/// as a permanent learner. There is no promotion to wait for: growing the meta
/// quorum with the data cluster is exactly what keeping placement in a small
/// fixed group was for, so `expected` is what the map must come to hold, not
/// what the voter set must become.
async fn admit(cluster: &mut Cluster, id: NodeId, expected: &[NodeId]) {
    cluster.start_joining_node(id, &ALL);
    let reply = cluster
        .administer_meta(&ALL, || AdminOp::AddNode { id, address: format!("in-process://{id}") })
        .await;
    assert!(matches!(reply, AdminReply::Accepted { .. }), "admitting {id}: {reply:?}");
    assert!(
        await_map_over(cluster, expected).await.is_some(),
        "the map never grew to include node {id}"
    );
}

#[tokio::test]
async fn admitting_a_node_publishes_a_new_map_that_includes_it() {
    let mut cluster = Cluster::of_three();
    let before = map_of(&cluster).await;
    assert_eq!(before.nodes, ALL.into_iter().collect());

    admit(&mut cluster, 4, &[1, 2, 3, 4]).await;

    let after = await_map_over(&cluster, &[1, 2, 3, 4]).await.expect("the map grows to four nodes");
    assert!(after.version > before.version, "the map was republished without a version bump");
    assert!(after.nodes.contains(&4));
    assert!((0..after.num_shards).any(|s| after.holds(4, s)), "node 4 was given no shards at all");
}

/// M10's second ✅ criterion, end to end rather than in `kv-ring`'s unit
/// tests: adding a node moves shards **onto** it and nowhere else. A shard
/// that changed hands between two nodes that both stayed would be a Raft
/// membership change and a full data move that nothing asked for.
#[tokio::test]
async fn admitting_a_node_moves_shards_only_onto_it() {
    let mut cluster = Cluster::of_three();
    let before = map_of(&cluster).await;

    admit(&mut cluster, 4, &[1, 2, 3, 4]).await;
    let after = await_map_over(&cluster, &[1, 2, 3, 4]).await.expect("the map grows");

    let rf = before.replication_factor as usize;
    for shard in 0..before.num_shards {
        let old = before.replicas(shard);
        let new = after.replicas(shard);
        if new.contains(&4) {
            let without: Vec<NodeId> = new.iter().copied().filter(|&n| n != 4).collect();
            assert_eq!(
                without,
                old[..rf - 1].to_vec(),
                "shard {shard}: admitting node 4 disturbed the surviving replicas"
            );
        } else {
            assert_eq!(new, old, "shard {shard} was reshuffled without gaining node 4");
        }
    }
}

/// ...and it takes roughly its fair share, which is the quantitative half of
/// "≈1/N of shards and no more".
#[tokio::test]
async fn a_new_node_takes_about_a_quarter_of_a_four_node_cluster() {
    let mut cluster = Cluster::of_three();
    map_of(&cluster).await;
    admit(&mut cluster, 4, &[1, 2, 3, 4]).await;
    let after = await_map_over(&cluster, &[1, 2, 3, 4]).await.expect("the map grows");

    let total = after.num_shards as usize * after.replication_factor as usize;
    let mine = after.shards_of(4).count();
    let fair = total as f64 / 4.0;
    assert!(
        (mine as f64) > fair * 0.5 && (mine as f64) < fair * 1.6,
        "node 4 holds {mine} of {total} replica slots, fair share is {fair:.1}"
    );
}

#[tokio::test]
async fn removing_a_node_drops_it_from_the_map() {
    let mut cluster = Cluster::of_three();
    map_of(&cluster).await;
    admit(&mut cluster, 4, &[1, 2, 3, 4]).await;
    await_map_over(&cluster, &[1, 2, 3, 4]).await.expect("the map grows");

    let reply = cluster.administer_meta(&[1, 2, 3], || AdminOp::RemoveNode { id: 4 }).await;
    assert!(matches!(reply, AdminReply::Accepted { .. }), "removing node 4: {reply:?}");

    let after = await_map_over(&cluster, &ALL).await.expect("the map shrinks back to three");
    assert!(!after.nodes.contains(&4));
    for shard in 0..after.num_shards {
        assert!(!after.holds(4, shard), "shard {shard} still lists the removed node");
    }
}

/// A node admitted after bootstrap is placed, and never votes.
///
/// This inverts M10's rule, deliberately. There, placement came from the data
/// group's voters and a learner was excluded because it was still catching up
/// on user data. Since M11 there is no data group: the cluster's membership
/// *is* the meta group's, and a node admitted to it is a permanent learner
/// that replicates the map and nothing else. Excluding learners now would mean
/// a node admitted after bootstrap never appeared in the map at all — it could
/// not route, could not be routed to, and could never receive a shard.
///
/// It never becomes a voter, which is the other half: the meta quorum must not
/// grow with the data cluster, or the small fixed group that records placement
/// stops being small or fixed.
#[tokio::test]
async fn a_node_admitted_after_bootstrap_is_placed_and_never_votes() {
    let mut cluster = Cluster::of_three();
    map_of(&cluster).await;

    admit(&mut cluster, 4, &[1, 2, 3, 4]).await;

    // Several reconcile intervals past the point a promotion would have
    // happened, since a caught-up learner in any other group would be
    // promoted by now.
    cluster.settle(Duration::from_millis(400)).await;
    for &id in &ALL {
        assert_eq!(
            cluster.meta_status_of(id).await.voters,
            ALL.to_vec(),
            "node {id}'s meta group grew its quorum with the data cluster"
        );
    }
    assert!(cluster.meta_status_of(1).await.learners.contains(&4), "node 4 is not a meta learner");

    let map = map_of(&cluster).await;
    assert!(map.nodes.contains(&4), "an admitted node was left out of the shard map");
}

/// The map is republished only when the cluster actually moves. A reconciler
/// that proposed on every pass would fill the meta log with versions that say
/// the same thing, and snapshot it forever.
#[tokio::test]
async fn a_settled_cluster_stops_republishing() {
    let cluster = Cluster::of_three();
    let first = map_of(&cluster).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let later = map_of(&cluster).await;
    assert_eq!(later.version, first.version, "the map keeps being rebuilt with nothing to change");
}
