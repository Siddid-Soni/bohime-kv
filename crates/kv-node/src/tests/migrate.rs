//! The migration decision (M12.1).
//!
//! `plan_step` is the whole of what M12.1 decides; everything around it is
//! plumbing that has been exercised since M9. These tests are about the two
//! rules that make a rebalance safe rather than merely eventual: a replica is
//! never removed before its replacement votes, and no more than
//! `--max-migrations` shards move at once.

use std::time::Duration;

use kv_ring::ShardMap;

use crate::driver::ShardStatus;
use crate::migrate::{Step, plan_step};

const PATIENCE: Duration = Duration::from_secs(10);

fn status(shard: u16, leading: bool, voters: &[u64], learners: &[u64]) -> ShardStatus {
    ShardStatus {
        shard,
        leader: if leading { Some(1) } else { Some(2) },
        term: 1,
        replicas: voters.to_vec(),
        learners: learners.to_vec(),
        leading,
        applied_index: 1,
    }
}

/// `shards[i]` set by hand: the ring's own placement is `kv-ring`'s to test,
/// and a test that used it would be asserting on whatever the ring happened
/// to produce rather than on the rule under test.
fn map_placing(shards: Vec<Vec<u64>>) -> ShardMap {
    let mut map = ShardMap::build(1, [1, 2, 3, 4], shards.len() as u16, 3, kv_ring::DEFAULT_VNODES)
        .expect("placeable");
    map.shards = shards;
    map
}

/// A node the map places on a shard we lead, which the group does not hold,
/// is added.
#[test]
fn a_node_the_map_adds_is_proposed_as_a_learner() {
    let map = map_placing(vec![vec![1, 2, 4]]);
    let statuses = [status(0, true, &[1, 2, 3], &[])];
    assert_eq!(plan_step(&map, &statuses, 1, 4), vec![Step::Add { shard: 0, node: 4 }]);
}

/// **The ordering rule.** Node 3 is leaving and node 4 arriving. While 4 is
/// only a learner it counts toward no quorum, so removing 3 now would leave
/// two voters of three — one further failure and the shard is down, which is
/// the unavailability a rebalance exists to avoid.
#[test]
fn a_replica_is_never_removed_before_its_replacement_votes() {
    let map = map_placing(vec![vec![1, 2, 4]]);

    let learning = [status(0, true, &[1, 2, 3], &[4])];
    assert_eq!(plan_step(&map, &learning, 1, 4), vec![], "removed a voter while 4 still learned");

    let promoted = [status(0, true, &[1, 2, 3, 4], &[])];
    assert_eq!(
        plan_step(&map, &promoted, 1, 4),
        vec![Step::Remove { shard: 0, node: 3 }],
        "did not remove the outgoing replica once its replacement voted"
    );
}

/// A shard this node does not lead is somebody else's to move. Every node
/// runs a reconciler, so acting on a shard we only replicate would be two
/// nodes proposing the same conf change.
#[test]
fn only_the_leader_of_a_shard_moves_it() {
    let map = map_placing(vec![vec![1, 2, 4]]);
    let statuses = [status(0, false, &[1, 2, 3], &[])];
    assert!(plan_step(&map, &statuses, 1, 4).is_empty());
}

/// The rate limit, and it is a limit on *shards*, not on steps.
#[test]
fn at_most_max_migrations_move_at_once() {
    let map = map_placing(vec![vec![4]; 6]);
    let statuses: Vec<ShardStatus> = (0..6).map(|s| status(s, true, &[1], &[])).collect();

    let steps = plan_step(&map, &statuses, 1, 2);
    assert_eq!(steps.len(), 2, "the limit was not applied: {steps:?}");
    assert_eq!(
        steps,
        vec![Step::Add { shard: 0, node: 4 }, Step::Add { shard: 1, node: 4 }],
        "the limit must take shards in id order, or which ones move changes every pass"
    );
}

/// A shard the map does not describe — a status for a shard beyond
/// `num_shards`, which a node briefly reports while its map is a version
/// behind — is left alone rather than indexed into the map.
#[test]
fn a_shard_outside_the_map_is_ignored() {
    let map = map_placing(vec![vec![1, 2, 3]]);
    let statuses = [status(9, true, &[1], &[])];
    assert!(plan_step(&map, &statuses, 1, 4).is_empty());
}

/// A shard already where the map wants it produces nothing at all. The
/// reconciler runs on every node twice a second forever, and a pass that
/// proposed something for a settled shard would fill every group's log.
#[test]
fn a_settled_shard_needs_no_step() {
    let map = map_placing(vec![vec![1, 2, 3]]);
    let statuses = [status(0, true, &[1, 2, 3], &[])];
    assert!(plan_step(&map, &statuses, 1, 4).is_empty());
}

/// A supervised cluster founds its own shard groups, through the same
/// supervisor `main` spawns.
///
/// The precondition for everything below it: every harness before this one
/// founds groups in the test, which is the code path M12.1 replaces.
#[tokio::test]
async fn a_supervised_cluster_founds_its_shards() {
    let cluster = crate::tests::cluster::Cluster::supervised(&[1, 2, 3], 4, 3).await;
    for id in [1, 2, 3] {
        assert!(
            cluster.await_hosted(id, &[0, 1, 2, 3], PATIENCE).await,
            "node {id} never founded all four shards"
        );
    }
}

/// ✅ **M12.1's gate.** Add a node to a live cluster under load: data
/// rebalances, no request fails, and every shard's group ends up exactly
/// where the map says.
///
/// Four shards and RF 3 on three nodes means every node holds every shard;
/// admitting a fourth makes the ring move one replica slot per shard it
/// joins, so the cluster does add → catch up → promote → remove on real data
/// while a writer is going. The writer is what makes this a migration test
/// rather than a placement test: a rebalance that stopped the cluster would
/// still satisfy an assertion about the final map.
#[tokio::test]
async fn the_m12_1_gate() {
    use crate::driver::{AdminOp, AdminReply, ClientOp, ClientReply};
    use crate::tests::cluster::Cluster;

    let mut cluster = Cluster::supervised(&[1, 2, 3], 4, 3).await;
    for id in [1, 2, 3] {
        assert!(cluster.await_hosted(id, &[0, 1, 2, 3], PATIENCE).await, "node {id} founded late");
    }
    let before = cluster.await_shard_map_on(1, PATIENCE).await.expect("a map");

    // Data to move. Spread across shards rather than sequential, so a
    // migration that dropped one shard's state is not hidden by another's.
    let keys: Vec<Vec<u8>> = (0..32u32).map(|i| format!("user:{}", i * 37).into_bytes()).collect();
    for key in &keys {
        let shard = before.shard_for_key(key);
        let reply = cluster
            .client()
            .on_shard(shard, || ClientOp::put(key, b"before"), &[1, 2, 3], PATIENCE)
            .await;
        assert!(matches!(reply, Some(ClientReply::Applied)), "seeding {key:?}: {reply:?}");
    }

    // A writer that runs across the whole migration and records every
    // failure. Nothing here tolerates a dropped request: "no request fails"
    // is half the gate.
    let client = cluster.client();
    let writing = tokio::spawn({
        let keys = keys.clone();
        let map = (*before).clone();
        async move {
            let mut failures = Vec::new();
            for round in 0..120u32 {
                let key = &keys[(round as usize) % keys.len()];
                let shard = map.shard_for_key(key);
                let reply = client
                    .on_shard(shard, || ClientOp::put(key, b"during"), &[1, 2, 3, 4], PATIENCE)
                    .await;
                if !matches!(reply, Some(ClientReply::Applied)) {
                    failures.push((round, format!("{reply:?}")));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            failures
        }
    });

    cluster.start_joining_supervised(4, &[1, 2, 3]);
    let reply = cluster
        .administer_meta(&[1, 2, 3], || AdminOp::AddNode {
            id: 4,
            address: "in-process://4".into(),
        })
        .await;
    assert!(matches!(reply, AdminReply::Accepted { .. }), "admitting node 4: {reply:?}");

    let failures = writing.await.expect("the writer does not panic");
    assert!(failures.is_empty(), "requests failed during the migration: {failures:?}");

    // The map has moved; every group must follow it.
    let map = cluster.await_shard_map_on(1, PATIENCE).await.expect("a map");
    assert!(map.nodes.contains(&4), "node 4 never entered the map: {:?}", map.nodes);
    let mine: Vec<u16> = map.shards_of(4).collect();
    assert!(!mine.is_empty(), "the map placed no shard on node 4");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut disagreeing = Vec::new();
        for shard in 0..4u16 {
            let target: std::collections::BTreeSet<u64> =
                map.replicas(shard).iter().copied().collect();
            for &id in &target {
                let held: Option<std::collections::BTreeSet<u64>> = cluster
                    .shard_statuses_of(id)
                    .await
                    .into_iter()
                    .find(|s| s.shard == shard)
                    .map(|s| s.replicas.into_iter().collect());
                if held.as_ref() != Some(&target) {
                    disagreeing.push((shard, id, held));
                }
            }
        }
        if disagreeing.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "shard groups never converged on the map: {disagreeing:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Node 4 holds what the map gave it, with the data written before it
    // existed.
    assert!(
        cluster.await_hosted(4, &mine, Duration::from_secs(30)).await,
        "node 4 did not adopt exactly the shards the map placed on it"
    );
    // Two checks, because one alone would not say the data moved.
    //
    // A `Get` is linearizable, so only a leader answers one and node 4 is a
    // follower of most of what it holds — asking node 4 directly would test
    // the read path, not the migration. So: every key is still readable
    // through whichever node leads its shard (the rebalance lost nothing),
    // **and** node 4 has applied everything that shard's leader has (node 4
    // is the replica holding it, not merely named as one).
    for key in &keys {
        let shard = map.shard_for_key(key);
        let reply =
            cluster.client().on_shard(shard, || ClientOp::get(key), &[1, 2, 3, 4], PATIENCE).await;
        assert!(
            matches!(reply, Some(ClientReply::Value(Some(_)))),
            "key {key:?} did not survive the rebalance: {reply:?}"
        );
    }

    for &shard in &mine {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let applied = |statuses: Vec<crate::driver::ShardStatus>| {
                statuses.into_iter().find(|s| s.shard == shard).map(|s| s.applied_index)
            };
            let ahead = {
                let mut best = 0;
                for &id in map.replicas(shard) {
                    best = best.max(applied(cluster.shard_statuses_of(id).await).unwrap_or(0));
                }
                best
            };
            let ours = applied(cluster.shard_statuses_of(4).await).unwrap_or(0);
            if ours >= ahead && ours > 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "node 4 never caught up on shard {shard}: applied {ours} of {ahead}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // **What a departing replica does *not* do yet, and why.** Nodes 1-3 each
    // lost a replica slot here, and each still hosts that shard's data.
    //
    // The release path itself works and is pinned by
    // `tests::shards::a_shard_is_deleted_only_when_both_witnesses_agree` and
    // `tests::driver::a_group_that_still_names_this_node_is_not_released`.
    // What cannot happen yet is the *first witness arriving*: a removed
    // replica has no way to learn it was removed. `RemoveVoter` takes effect
    // when the entry is **appended**, and `replication_targets` is derived
    // from the live config, so the leader stops replicating to the node
    // before the entry that removes it goes out. The departing node is left
    // believing it is still a voter, with no leader and nobody to ask —
    // observed here as `replicas [1, 2, 3, 4], leader None`.
    //
    // Fixing it needs an explicit "you are gone" message from the leader (a
    // `kv-raft` change) or a node-to-node control path for a replica to ask
    // the group about itself. Both are larger than M12.1; recorded in
    // `docs/KNOWN-ISSUES.md`. Asserted here so the day it is fixed, this
    // assertion fails and says so.
    let orphaned: Vec<(u64, u16)> = {
        let mut found = Vec::new();
        for &id in &[1u64, 2, 3] {
            let kept: Vec<u16> = map.shards_of(id).collect();
            for status in cluster.shard_statuses_of(id).await {
                if !kept.contains(&status.shard) {
                    found.push((id, status.shard));
                }
            }
        }
        found
    };
    assert!(
        !orphaned.is_empty(),
        "a departed replica released itself: the tombstone gap is fixed, and this gate and \
         docs/KNOWN-ISSUES.md should now say so"
    );
}

/// `Rebalance` is a nudge, not a command: the reconciler runs on its own loop
/// regardless, and an operator who has just added a node wants the pass now
/// rather than within half a second. It answers with what is still moving on
/// the node that was asked.
#[tokio::test]
async fn rebalance_runs_a_pass_and_reports_what_is_still_moving() {
    let cluster = crate::tests::cluster::Cluster::supervised(&[1, 2, 3], 4, 3).await;
    for id in [1, 2, 3] {
        assert!(cluster.await_hosted(id, &[0, 1, 2, 3], PATIENCE).await);
    }
    assert_eq!(cluster.rebalance(1).await, 0, "a settled cluster reported a migration");
}
