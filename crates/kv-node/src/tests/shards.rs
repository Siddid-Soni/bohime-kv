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

use kv_raft::NodeId;
use kv_ring::{ShardId, ShardMap};
use tokio::sync::mpsc;

use crate::config::NodeConfig;
use crate::driver::{AdminReply, AdminRequest, Group};
use crate::meta::PublishedMap;
use crate::placement;
use crate::shards::{Origin, ShardSupervisor};
use crate::tests::support::config_in;
use crate::transport::group;
use crate::transport::server::GroupRegistry;

/// What one supervisor run left behind.
///
/// `founded` and `hosted` are genuinely different since M12.1, and conflating
/// them would hide the milestone: a node that founds nothing may still
/// *adopt* every shard the map places on it, and the two differ in the one
/// way that matters — a founded group starts from a voter set this node chose
/// out of the map, an adopted one starts empty and takes its membership from
/// the leader that admits it.
struct Supervised {
    /// Every shard handed to the driver, founded or adopted.
    hosted: BTreeSet<ShardId>,
    /// Those with a `.voters` record, which only founding writes.
    founded: BTreeSet<ShardId>,
}

/// Runs a supervisor over `config` against a published `map`, and collects
/// what it hosted.
///
/// The groups are drained off the channel rather than handed to a real
/// driver: what is under test is which shards get hosted and what is
/// recorded, not what the driver then does with them.
async fn found_with(config: &NodeConfig, map: Option<&ShardMap>) -> Supervised {
    let published = PublishedMap::default();
    if let Some(map) = map {
        published.store(Some(Arc::new(map.clone())));
    }
    let registry = GroupRegistry::default();
    let (inbox_tx, _inbox) = mpsc::channel(16);
    let (groups_tx, mut groups) = mpsc::channel::<Group>(512);
    // Nothing here is under test for release, but a supervisor that asked
    // must not be left awaiting a reply forever.
    let admin_tx = stand_in_driver(Released::Refused);

    let supervisor = ShardSupervisor::new(
        config.clone(),
        published,
        registry.clone(),
        inbox_tx,
        admin_tx,
        groups_tx,
        Duration::from_millis(10),
    );
    let task = tokio::spawn(supervisor.run());

    // Long enough for the first pass plus a couple more, so a supervisor that
    // founded twice would be caught rather than merely not observed.
    tokio::time::sleep(Duration::from_millis(120)).await;
    task.abort();

    let mut hosted = BTreeSet::new();
    while let Ok(group) = groups.try_recv() {
        let shard = group::shard_of(group.id()).expect("a shard group");
        assert!(hosted.insert(shard), "shard {shard} was hosted twice");
    }
    // Whatever it hosted, it made routable — a peer that dials during the
    // group's first tick must find an inbox, not a gap.
    let routable: BTreeSet<ShardId> =
        registry.groups().into_iter().filter_map(group::shard_of).collect();
    assert_eq!(routable, hosted, "the routing table and the driver disagree");

    // Only founding records a voter set: an adopted group has none to record,
    // which is the durable difference between the two.
    let founded = hosted
        .iter()
        .copied()
        .filter(|&shard| placement::founding_voters(config, shard).unwrap().is_some())
        .collect();
    Supervised { hosted, founded }
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

    let supervised = found_with(&config, Some(&map)).await;

    let mine: BTreeSet<ShardId> = map.shards_of(1).collect();
    assert_eq!(supervised.founded, mine, "the map's shards were not all founded");
    assert_eq!(supervised.hosted, mine, "a founder hosts exactly what it founded");
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

    assert!(found_with(&config, None).await.hosted.is_empty());
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

    let supervised = found_with(&config, Some(&map)).await;
    assert!(supervised.founded.is_empty(), "a joining node founded shard groups");
    // It *adopts* them instead (M12.1): the map places them here, so their
    // leaders are about to add it as a learner and the group has to exist for
    // that message to land. Founded and adopted differ in exactly the way
    // that matters — an adopted group starts with no voters at all and takes
    // its membership from the leader, rather than inventing one from a map
    // its co-replicas may not share.
    assert_eq!(
        supervised.hosted,
        map.shards_of(1).collect::<BTreeSet<_>>(),
        "a joining node did not adopt the shards the map placed on it"
    );
    // Recorded, so a later pass does not reconsider founding.
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

    let supervised = found_with(&config, Some(&map)).await;
    assert!(
        supervised.founded.is_empty(),
        "a node with no marker founded from a map past version 1"
    );
    // Adopted, not founded: the distinction is the whole safety argument.
    // Founding from version 4 would put a voter set on shard 7 that its
    // co-replicas — running the version-1 set — never agreed to. Adopting
    // puts no voter set on it at all and waits to be told.
    assert_eq!(supervised.hosted, map.shards_of(1).collect::<BTreeSet<_>>());
    assert_eq!(placement::founded_at(&config).unwrap(), Some(4));
}

/// After a restart the shards on disk are reopened, nothing is founded again,
/// and whatever the map has since placed here is adopted.
///
/// Three claims in one test because they are one rule: a group whose log is on
/// this disk is one this node is a member of as far as Raft is concerned,
/// whatever placement has since decided — so it is *reopened*, with the voter
/// set founding recorded. A shard the map has since given us is not ours by
/// the map's word either: it is adopted with no voters at all, and becomes
/// ours when its leader admits us. What must never happen between those two is
/// *founding*, which would invent a membership from a map this node's
/// co-replicas may not share.
#[tokio::test]
async fn a_restart_reopens_disk_adopts_the_map_and_founds_nothing() {
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
    let supervised = found_with(&config, Some(&map)).await;

    assert!(supervised.founded.is_empty(), "a restart founded shards it never held");
    assert_eq!(
        supervised.hosted,
        map.shards_of(1).collect::<BTreeSet<_>>(),
        "a restart did not host the shards on disk plus the ones the map added"
    );
    assert_eq!(placement::founded_at(&config).unwrap(), Some(1), "the marker was rewritten");
}

/// A founded shard group's voter set is *its replica set*, not the cluster.
///
/// Nothing writes it to the log — founding happens with no conf change, which
/// is the whole reason `.placement` exists — so a reopen has nothing to replay
/// and falls back to `raft_config_for`, whose `peers` is every `--peer` this
/// node was ever given. With RF equal to the node count the two agree and the
/// bug is invisible; with RF 3 on 5 nodes the reopened group has a quorum of 3
/// where it should have 2, and two members that never answer.
#[tokio::test]
async fn a_founded_groups_voters_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path());
    config.id = 1;
    // The whole cluster, as `--peer` would name it.
    config.peers = (2..=5).map(|p| (p, format!("in-process://{p}"))).collect();

    let replicas = [1u64, 2, 3];
    let group = group::shard(7);

    let founded =
        crate::shards::open(&config, 7, group, Origin::Found { voters: replicas.to_vec() })
            .expect("founding shard 7");
    assert_eq!(
        founded.voters(),
        BTreeSet::from([1, 2, 3]),
        "founding did not take the shard's replica set"
    );
    drop(founded);

    // Reopen exactly as the supervisor does for a shard already on disk: no
    // map, because the map may have moved on and the group's own membership
    // is the log's business.
    let reopened =
        crate::shards::open(&config, 7, group, Origin::Reopen).expect("reopening shard 7");
    assert_eq!(
        reopened.voters(),
        BTreeSet::from([1, 2, 3]),
        "the reopened group invented a membership from --peer"
    );
}

/// A node adopting a shard joins as a learner of an empty config, whatever
/// its own flags say.
///
/// The trap: `raft_config_for` hands out `peers = <every --peer>` and this
/// node's `--join` flag, which on a founder is false. `initial_cluster` would
/// then make the adopting node a **voter** of a config containing every node
/// the operator ever named — and a voter campaigns. It would win an election
/// in its own fabricated group and lead a second incarnation of a shard that
/// already has a leader, with a log sharing no history with the real one.
#[tokio::test]
async fn a_founder_adopting_a_shard_never_becomes_its_voter() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config_in(dir.path());
    config.id = 1;
    config.initial_learner = false; // a founder, not a joiner
    config.peers = (2..=5).map(|p| (p, format!("in-process://{p}"))).collect();

    let mut adopted =
        crate::shards::open(&config, 7, group::shard(7), Origin::Adopt).expect("adopting shard 7");

    assert!(adopted.voters().is_empty(), "an adopted group starts with no voters");
    assert_eq!(
        adopted.learners(),
        BTreeSet::from([1]),
        "an adopted group holds itself as a learner and nothing else"
    );

    // And it stays inert. Well past any election timeout, with nobody to talk
    // to, a voter here would have campaigned and won a quorum of one.
    for _ in 0..200 {
        adopted.tick();
    }
    assert_eq!(
        adopted.role(),
        kv_raft::Role::Follower,
        "an adopted group campaigned; it would be a second leader for this shard"
    );
}

/// A node that has already founded — or founded nothing, as a joiner — still
/// hosts a shard a later map places on it.
///
/// This is the receiving half of a migration. Without it a `--join` node is
/// placed in the map, routed to by every other node, and hosts nothing, which
/// is exactly the state M11 left the cluster in.
#[tokio::test]
async fn a_shard_the_map_places_here_later_is_adopted() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = founder(dir.path(), 4, 1);
    config.id = 9;
    // Already founded, at a version that placed nothing here.
    placement::record_founding(&config, 1).unwrap();

    // Version 2 places every shard on node 9, because it is the only node.
    let map = map_of(&[9], 4, 1, 2);
    let supervised = found_with(&config, Some(&map)).await;

    assert_eq!(
        supervised.hosted,
        BTreeSet::from([0, 1, 2, 3]),
        "the supervisor did not adopt the shards the map placed here"
    );
    assert!(supervised.founded.is_empty(), "an adopted shard must not be founded");
}

/// What a stand-in shard driver answers every `ReleaseShard` with.
///
/// The driver's own half of the witness rule has its own tests in
/// `tests::driver`; here it is a knob, so that what this file asserts is the
/// *supervisor's* half — that the map alone never deletes anything.
#[derive(Clone, Copy)]
enum Released {
    Agreed,
    Refused,
}

fn stand_in_driver(answer: Released) -> mpsc::Sender<AdminRequest> {
    let (admin_tx, mut admin) = mpsc::channel::<AdminRequest>(16);
    let released = matches!(answer, Released::Agreed);
    tokio::spawn(async move {
        while let Some(request) = admin.recv().await {
            let _ = request.reply.send(AdminReply::Released { released });
        }
    });
    admin_tx
}

/// Runs a supervisor against `map` with a stand-in driver, and lets it make a
/// few passes.
async fn supervise_with(config: &NodeConfig, map: &ShardMap, answer: Released) {
    let published = PublishedMap::default();
    published.store(Some(Arc::new(map.clone())));
    let (inbox_tx, _inbox) = mpsc::channel(16);
    let (groups_tx, _groups) = mpsc::channel::<Group>(512);

    let supervisor = ShardSupervisor::new(
        config.clone(),
        published,
        GroupRegistry::default(),
        inbox_tx,
        stand_in_driver(answer),
        groups_tx,
        Duration::from_millis(10),
    );
    let task = tokio::spawn(supervisor.run());
    tokio::time::sleep(Duration::from_millis(120)).await;
    task.abort();
}

/// `map` with `node` taken out of `shard`'s replica set, at `version`.
///
/// Built by hand rather than by re-running the ring, so the test controls
/// exactly one slot rather than whatever a fresh placement produces.
fn map_without(map: &ShardMap, shard: ShardId, node: NodeId, version: u64) -> ShardMap {
    let mut next = map.clone();
    next.version = version;
    next.shards[shard as usize].retain(|&id| id != node);
    next
}

/// A stale map alone does not delete a shard's data; the map plus the group's
/// own agreement does.
///
/// Both halves in one test on purpose — a test that only asserted the delete
/// would pass against an implementation with one witness, which is the
/// implementation this rule exists to forbid.
#[tokio::test]
async fn a_shard_is_deleted_only_when_both_witnesses_agree() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = founder(dir.path(), 4, 1);
    config.id = 9;

    // Hosted, with directories on disk.
    let map_v1 = map_of(&[9], 4, 1, 1);
    let hosted = found_with(&config, Some(&map_v1)).await.hosted;
    assert!(hosted.contains(&0), "the founder did not found shard 0");
    assert!(config.shard_dir(0).exists());

    // The map moves shard 0 away, but the group refuses: its config still
    // names us.
    let moved = map_without(&map_v1, 0, 9, 2);
    supervise_with(&config, &moved, Released::Refused).await;
    assert!(config.shard_dir(0).exists(), "a shard was deleted on the map's word alone");

    // Now the group agrees too.
    supervise_with(&config, &moved, Released::Agreed).await;
    assert!(!config.shard_dir(0).exists(), "a shard both witnesses released was not deleted");
}
