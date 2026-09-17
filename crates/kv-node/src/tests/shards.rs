//! Founding this node's shard groups (M11.5).
//!
//! The supervisor is what turns a replicated placement into local Raft
//! groups, and the rule it enforces is the one that keeps that safe: found
//! once, from version 1, and never from a map this node might not share with
//! its co-replicas. Everything here is about that rule, because getting it
//! wrong produces two configurations for one shard — split brain arriving
//! through the front door rather than through a partition.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use kv_ring::{ShardId, ShardMap};
use tokio::sync::mpsc;

use crate::config::NodeConfig;
use crate::driver::Group;
use crate::meta::PublishedMap;
use crate::placement;
use crate::shards::ShardSupervisor;
use crate::tests::support::config_in;
use crate::transport::group;
use crate::transport::server::GroupRegistry;

/// Runs a supervisor over `config` against a published `map`, and collects
/// whatever it founds.
///
/// The groups are drained off the channel rather than handed to a real
/// driver: what is under test is which shards get founded and what is
/// recorded, not what the driver then does with them.
async fn found_with(config: &NodeConfig, map: Option<&ShardMap>) -> BTreeSet<ShardId> {
    let published = PublishedMap::default();
    if let Some(map) = map {
        published.store(Some(Arc::new(map.clone())));
    }
    let registry = GroupRegistry::default();
    let (inbox_tx, _inbox) = mpsc::channel(16);
    let (groups_tx, mut groups) = mpsc::channel::<Group>(512);

    let supervisor = ShardSupervisor::new(
        config.clone(),
        published,
        registry.clone(),
        inbox_tx,
        groups_tx,
        Duration::from_millis(10),
    );
    let task = tokio::spawn(supervisor.run());

    // Long enough for the first pass plus a couple more, so a supervisor that
    // founded twice would be caught rather than merely not observed.
    tokio::time::sleep(Duration::from_millis(120)).await;
    task.abort();

    let mut founded = BTreeSet::new();
    while let Ok(group) = groups.try_recv() {
        let shard = group::shard_of(group.id()).expect("a shard group");
        assert!(founded.insert(shard), "shard {shard} was founded twice");
    }
    // Whatever it founded, it made routable — a peer that dials during the
    // group's first tick must find an inbox, not a gap.
    let routable: BTreeSet<ShardId> =
        registry.groups().into_iter().filter_map(group::shard_of).collect();
    assert_eq!(routable, founded, "the routing table and the driver disagree");
    founded
}

fn map_of(nodes: &[u64], shards: u16, rf: u8, version: u64) -> ShardMap {
    ShardMap::build(version, nodes.iter().copied(), shards, rf, kv_ring::DEFAULT_VNODES)
        .expect("placeable")
}

fn founder(dir: &std::path::Path, shards: u16, rf: u8) -> NodeConfig {
    let mut config = config_in(dir);
    config.num_shards = shards;
    config.replication_factor = rf;
    config
}

#[tokio::test]
async fn a_founder_creates_a_group_for_every_shard_the_map_gives_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = founder(dir.path(), 8, 1);
    let map = map_of(&[1], 8, 1, 1);

    let founded = found_with(&config, Some(&map)).await;

    assert_eq!(founded, map.shards_of(1).collect(), "the map's shards were not all founded");
    assert_eq!(
        placement::founded_at(&config).unwrap(),
        Some(1),
        "the founding version was not recorded"
    );
}

/// Without a map there is nothing to found *and nothing to record*: a marker
/// written now would make the next pass skip founding entirely, and the node
/// would host nothing forever.
#[tokio::test]
async fn a_node_with_no_map_founds_nothing_and_records_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let config = founder(dir.path(), 8, 1);

    assert!(found_with(&config, None).await.is_empty());
    assert_eq!(placement::founded_at(&config).unwrap(), None);
}

/// A joining node founds nothing, however inviting the map looks.
///
/// Its shards arrive by migration (M12), as learners added to groups that
/// already exist. Founding from the map it happens to see would create a
/// second configuration for a shard the incumbents are already running — the
/// exact split brain the marker exists to prevent.
#[tokio::test]
async fn a_joining_node_founds_nothing_and_still_records_the_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = founder(dir.path(), 8, 1);
    config.initial_learner = true;
    let map = map_of(&[1], 8, 1, 1);

    assert!(
        found_with(&config, Some(&map)).await.is_empty(),
        "a joining node founded shard groups"
    );
    // Recorded, so a later pass does not reconsider: the node is a member
    // that holds nothing, which is a settled state rather than a pending one.
    assert_eq!(placement::founded_at(&config).unwrap(), Some(1));
}

/// A founder that missed bootstrap founds nothing either.
///
/// Its shards were placed by a map it never saw. Version 2 may have moved a
/// replica slot, so founding from it would put a different voter set on a
/// shard its co-replicas are already running with the version-1 set.
#[tokio::test]
async fn a_founder_that_missed_bootstrap_founds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let config = founder(dir.path(), 8, 1);
    let map = map_of(&[1], 8, 1, 4);

    assert!(
        found_with(&config, Some(&map)).await.is_empty(),
        "a node with no marker founded from a map past version 1"
    );
    assert_eq!(placement::founded_at(&config).unwrap(), Some(4));
}

/// After a restart the shards on disk are the truth, not the map.
///
/// A group whose log is on this disk is one this node is a member of as far as
/// Raft is concerned, whatever placement has since decided — and a shard the
/// map has since given us is *not* ours until the data moves.
#[tokio::test]
async fn a_restart_hosts_what_is_on_disk_and_founds_nothing_new() {
    let dir = tempfile::tempdir().unwrap();
    let config = founder(dir.path(), 8, 1);

    // A previous life: two shards founded, the marker written.
    for shard in [2u16, 5] {
        std::fs::create_dir_all(config.shard_raft_dir(shard)).unwrap();
        std::fs::create_dir_all(config.shard_state_dir(shard)).unwrap();
    }
    placement::record_founding(&config, 1).unwrap();

    // A map that would give this node every shard, if it were allowed to
    // found from it.
    let map = map_of(&[1], 8, 1, 3);
    let founded = found_with(&config, Some(&map)).await;

    assert_eq!(founded, BTreeSet::from([2, 5]), "a restart founded shards it never held");
    assert_eq!(placement::founded_at(&config).unwrap(), Some(1), "the marker was rewritten");
}
