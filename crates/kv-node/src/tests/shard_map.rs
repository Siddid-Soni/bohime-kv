//! The shard map inside the meta group's state machine (M10.6).

use std::time::Duration;

use kv_ring::ShardMap;
use kv_storage::Engine;

use crate::shard_map::{self, ParamMismatch};
use crate::tests::cluster::{ALL, Cluster};
use crate::tests::support::config_in;

fn a_map(nodes: impl IntoIterator<Item = u64>) -> ShardMap {
    ShardMap::build(1, nodes, 256, 3, kv_ring::DEFAULT_VNODES).unwrap()
}

/// The map shares the reserved key space with the session table and the peer
/// address book, so the service-boundary check that stops a client forging a
/// session entry also stops it rewriting cluster placement.
#[test]
fn the_map_key_is_in_the_reserved_space() {
    assert_eq!(shard_map::SHARD_MAP_KEY[0], crate::session::RESERVED_PREFIX);
}

#[test]
fn a_stored_map_round_trips_through_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(dir.path()).unwrap();
    assert_eq!(shard_map::encoded(&engine).unwrap(), None);

    let map = a_map([1, 2, 3]);
    engine.put(shard_map::SHARD_MAP_KEY, &map.encode()).unwrap();

    let stored = shard_map::encoded(&engine).unwrap().expect("a map is stored");
    assert_eq!(ShardMap::decode(&stored).unwrap(), map);
}

/// The map lives in the state machine, so a snapshot carries it and a restart
/// replays it — the same argument M7 made for the session table and M9 for the
/// address book.
#[test]
fn a_stored_map_survives_reopening_the_engine() {
    let dir = tempfile::tempdir().unwrap();
    let map = a_map([1, 2, 3]);
    {
        let mut engine = Engine::open(dir.path()).unwrap();
        engine.put(shard_map::SHARD_MAP_KEY, &map.encode()).unwrap();
    }
    let engine = Engine::open(dir.path()).unwrap();
    let stored = shard_map::encoded(&engine).unwrap().expect("a map survives the reopen");
    assert_eq!(ShardMap::decode(&stored).unwrap(), map);
}

/// `--shards` and `--replication-factor` seed version 1 and are owned by the
/// map thereafter. A node whose flags disagree is not stale, it is
/// *misconfigured*: `hash % 256` and `hash % 512` agree about almost no key,
/// so letting it serve would misroute the whole keyspace silently.
#[test]
fn flags_matching_the_stored_map_are_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path());
    config.num_shards = 256;
    config.replication_factor = 3;
    config.vnodes_per_node = kv_ring::DEFAULT_VNODES;

    assert_eq!(shard_map::check_params(&config, &a_map([1, 2, 3])), Ok(()));
}

#[test]
fn a_different_shard_count_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path());
    config.num_shards = 512;
    config.replication_factor = 3;

    assert_eq!(
        shard_map::check_params(&config, &a_map([1, 2, 3])),
        Err(ParamMismatch { parameter: "shards", ours: 512, stored: 256 })
    );
}

#[test]
fn a_different_replication_factor_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path());
    config.num_shards = 256;
    config.replication_factor = 5;

    assert_eq!(
        shard_map::check_params(&config, &a_map([1, 2, 3])),
        Err(ParamMismatch { parameter: "replication-factor", ours: 5, stored: 3 })
    );
}

#[test]
fn a_different_vnode_count_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path());
    config.num_shards = 256;
    config.replication_factor = 3;
    config.vnodes_per_node = 64;

    assert_eq!(
        shard_map::check_params(&config, &a_map([1, 2, 3])),
        Err(ParamMismatch { parameter: "vnodes", ours: 64, stored: 128 })
    );
}

/// The whole of M10.6: a fresh cluster ends up with exactly one version-1 map,
/// covering the data cluster's voters, without an operator asking for it.
#[tokio::test]
async fn the_meta_leader_bootstraps_version_one() {
    let cluster = Cluster::of_three();
    let map = cluster.await_shard_map(Duration::from_secs(5)).await.expect("a map is published");

    assert_eq!(map.version, 1);
    assert_eq!(map.num_shards, 256);
    assert_eq!(map.replication_factor, 3);
    assert_eq!(map.nodes, ALL.into_iter().collect());
    assert_eq!(map.shards.len(), 256);
}

/// Every node publishes the same map, not merely *a* map. A replicated value
/// read locally is only useful if every replica agrees on it.
#[tokio::test]
async fn every_node_publishes_the_same_map() {
    let cluster = Cluster::of_three();
    cluster.await_shard_map(Duration::from_secs(5)).await.expect("a map is published");

    let mut seen = Vec::new();
    for &id in &ALL {
        let map = cluster
            .await_shard_map_on(id, Duration::from_secs(5))
            .await
            .unwrap_or_else(|| panic!("node {id} never published a map"));
        seen.push(map.encode());
    }
    assert!(seen.windows(2).all(|w| w[0] == w[1]), "nodes published different maps");
}

/// Bootstrap must land exactly once. Every node runs a reconciler and they all
/// see the same empty state at the same time, so the only thing stopping three
/// version-1 proposals from becoming three *versions* is the compare-and-swap
/// against "absent".
#[tokio::test]
async fn bootstrap_does_not_run_twice() {
    let cluster = Cluster::of_three();
    let first = cluster.await_shard_map(Duration::from_secs(5)).await.expect("a map");

    // Well past several reconcile intervals.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let later = cluster.await_shard_map(Duration::from_secs(5)).await.expect("still a map");
    assert_eq!(later.version, first.version, "the map was rebuilt with nothing to change");
}
