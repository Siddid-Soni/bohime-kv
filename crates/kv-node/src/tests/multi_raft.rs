//! One driver hosting many Raft groups over one tick loop (M11.4).
//!
//! 256 shards × one task each is the naive design M11 exists to avoid, so the
//! thing under test is that N groups share one `select!` and one interval and
//! still each keep their own log, their own state machine and their own
//! election.

use std::collections::BTreeMap;
use std::time::Duration;

use kv_raft::RaftNode;
use kv_ring::ShardId;
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::driver::{ClientOp, ClientReply, ClientRequest, Driver, Group, GroupChannels};
use crate::storage::BitcaskStorage;
use crate::tests::support::{SilentPeers, config_in};
use crate::transport::group;
use crate::transport::server::GroupRegistry;

/// A driver hosting one group per shard in `shards`, alone in its cluster so
/// each one elects itself immediately.
async fn driver_hosting(
    config: &crate::config::NodeConfig,
    shards: &[ShardId],
) -> (mpsc::Sender<ClientRequest>, GroupRegistry) {
    let registry = GroupRegistry::default();
    let (inbox_tx, inbox) = mpsc::channel(256);
    let (_replies_tx, peer_replies) = mpsc::channel(256);
    let (requests_tx, requests) = mpsc::channel(64);
    let (_admin_tx, admin) = mpsc::channel(16);
    let (groups_tx, new_groups) = mpsc::channel(64);

    let mut hosted = Vec::new();
    for &shard in shards {
        let group = group::shard(shard);
        std::fs::create_dir_all(config.shard_raft_dir(shard)).unwrap();
        std::fs::create_dir_all(config.shard_state_dir(shard)).unwrap();
        let node = RaftNode::new(
            config.raft_config_for(group),
            BitcaskStorage::open(config.shard_raft_dir(shard)).unwrap(),
        );
        let engine = Engine::open(config.shard_state_dir(shard)).unwrap();
        registry.register(group, inbox_tx.clone());
        hosted.push(Group::new(config, group, node, engine));
    }

    let driver = Driver::new(
        config,
        hosted,
        BTreeMap::new(),
        Box::new(SilentPeers),
        GroupChannels { inbox, peer_replies, requests, admin, new_groups },
    );
    tokio::spawn(driver.run());
    // Hold the senders open: dropping one closes that `select!` arm.
    std::mem::forget((inbox_tx, _replies_tx, _admin_tx, groups_tx));
    (requests_tx, registry)
}

async fn call(requests: &mpsc::Sender<ClientRequest>, group: u32, op: ClientOp) -> ClientReply {
    let (reply, wait) = oneshot::channel();
    requests.send(ClientRequest { group, op, reply }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .expect("the driver answers")
        .expect("the driver does not drop it")
}

/// The keyspaces must not bleed: a value written to shard 3 is not readable
/// from shard 4, because they are different state machines behind different
/// logs. One driver hosting both is what makes this worth asserting.
#[tokio::test]
async fn one_driver_serves_each_group_from_its_own_state_machine() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());
    let (requests, _registry) = driver_hosting(&config, &[3, 4]).await;

    let three = group::shard(3);
    let four = group::shard(4);

    // Both groups elect themselves: one interval drives every group's tick.
    for _ in 0..200 {
        let put = call(&requests, three, ClientOp::put(b"k", b"three")).await;
        if matches!(put, ClientReply::Applied) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(matches!(
        call(&requests, three, ClientOp::put(b"k", b"three")).await,
        ClientReply::Applied
    ));
    assert!(matches!(
        call(&requests, four, ClientOp::put(b"j", b"four")).await,
        ClientReply::Applied
    ));

    match call(&requests, three, ClientOp::get(b"k")).await {
        ClientReply::Value(v) => assert_eq!(v, Some(b"three".to_vec())),
        other => panic!("shard 3 answered its own key with {other:?}"),
    }
    match call(&requests, four, ClientOp::get(b"k")).await {
        ClientReply::Value(v) => assert_eq!(v, None, "shard 4 must not see shard 3's key"),
        other => panic!("shard 4 answered with {other:?}"),
    }
}

/// A request for a group this driver does not host is an ordinary answer, not
/// an error: it happens whenever a client's cached map is a version ahead of
/// the node it asked, and a client that got an error would drop the
/// connection instead of re-routing.
#[tokio::test]
async fn a_request_for_an_unhosted_group_is_redirected_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());
    let (requests, _registry) = driver_hosting(&config, &[3]).await;

    match call(&requests, group::shard(99), ClientOp::get(b"k")).await {
        ClientReply::NotHosted { shard } => assert_eq!(shard, 99),
        other => panic!("expected a redirect, got {other:?}"),
    }
}

/// Groups arrive while the driver is already running — that is how a node
/// founds its shards, off the driver loop, since opening 154 Bitcask
/// instances and replaying their logs would stall every group that was
/// already ticking.
#[tokio::test]
async fn a_group_can_be_handed_to_a_running_driver() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());
    let registry = GroupRegistry::default();
    let (inbox_tx, inbox) = mpsc::channel(256);
    let (replies_tx, peer_replies) = mpsc::channel(256);
    let (requests, requests_rx) = mpsc::channel(64);
    let (admin_tx, admin) = mpsc::channel(16);
    let (groups_tx, new_groups) = mpsc::channel(64);

    let driver = Driver::new(
        &config,
        Vec::new(),
        BTreeMap::new(),
        Box::new(SilentPeers),
        GroupChannels { inbox, peer_replies, requests: requests_rx, admin, new_groups },
    );
    tokio::spawn(driver.run());

    // Nothing hosted yet.
    match call(&requests, group::shard(7), ClientOp::get(b"k")).await {
        ClientReply::NotHosted { shard } => assert_eq!(shard, 7),
        other => panic!("expected a redirect before the group exists, got {other:?}"),
    }

    std::fs::create_dir_all(config.shard_raft_dir(7)).unwrap();
    std::fs::create_dir_all(config.shard_state_dir(7)).unwrap();
    let node = RaftNode::new(
        config.raft_config_for(group::shard(7)),
        BitcaskStorage::open(config.shard_raft_dir(7)).unwrap(),
    );
    let engine = Engine::open(config.shard_state_dir(7)).unwrap();
    registry.register(group::shard(7), inbox_tx.clone());
    groups_tx.send(Group::new(&config, group::shard(7), node, engine)).await.unwrap();

    let mut applied = false;
    for _ in 0..200 {
        if matches!(
            call(&requests, group::shard(7), ClientOp::put(b"k", b"v")).await,
            ClientReply::Applied
        ) {
            applied = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(applied, "a group handed over at runtime must start serving");

    std::mem::forget((inbox_tx, replies_tx, admin_tx, groups_tx));
}

/// M11's gate: the two ✅ criteria from `docs/DESIGN.md`, asserted against a
/// running five-node cluster.
///
/// - 5 nodes × 3 replicas × 8 shards: writes to different shards land on
///   different leaders and proceed concurrently;
/// - killing a node degrades only the shards it led, and only briefly.
pub(crate) mod the_m11_gate {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use kv_raft::NodeId;
    use kv_ring::ShardId;

    use crate::tests::cluster::Cluster;

    const NODES: [NodeId; 5] = [1, 2, 3, 4, 5];
    const SHARDS: u16 = 8;
    const RF: u8 = 3;
    const PATIENCE: Duration = Duration::from_secs(20);

    /// One key per shard. Found by search rather than assumed: `hash % 8` is
    /// not something a test should predict, and a key that happened to land on
    /// the wrong shard would make the gate assert nothing.
    fn key_per_shard(cluster: &Cluster) -> Vec<(ShardId, Vec<u8>)> {
        let map = cluster.placement();
        let mut found: Vec<(ShardId, Vec<u8>)> = Vec::new();
        for i in 0..100_000u32 {
            let key = format!("gate:{i}").into_bytes();
            let shard = map.shard_for_key(&key);
            if !found.iter().any(|(s, _)| *s == shard) {
                found.push((shard, key));
            }
            if found.len() == map.num_shards as usize {
                break;
            }
        }
        assert_eq!(found.len(), SHARDS as usize, "no key found for some shard");
        found.sort();
        found
    }

    /// ✅ **Writes to different shards land on different leaders and proceed
    /// concurrently.**
    ///
    /// Concurrency is shown by issuing every shard's write at once and having
    /// all of them complete — they cannot have queued behind one leader,
    /// because more than one node accepted one.
    #[tokio::test]
    async fn writes_to_different_shards_land_on_different_leaders() {
        let cluster = Cluster::sharded(&NODES, SHARDS, RF);
        let keys = key_per_shard(&cluster);
        cluster.await_shard_leaders(&NODES, PATIENCE).await;

        // Placement really does spread: the shards do not all live on the
        // same three nodes. Worth asserting because a ring that collapsed to
        // one replica set would make the rest of this test vacuous — every
        // write would reach the same three nodes whatever the leaders did.
        //
        // What then makes the *leaders* differ is not placement but
        // independent elections: `NodeConfig::raft_config_for` mixes the group
        // into each group's election seed, precisely so that a node's groups
        // do not campaign in lockstep. Eight groups drawing their own timeouts
        // do not all elect the same node.
        let map = cluster.placement();
        let replica_sets: BTreeSet<Vec<NodeId>> =
            (0..SHARDS).map(|shard| map.replicas(shard).to_vec()).collect();
        assert!(
            replica_sets.len() > 1,
            "every shard has the same replica set {replica_sets:?}; placement spread nothing"
        );

        // Every shard's write, issued at once.
        let mut writes = tokio::task::JoinSet::new();
        for (shard, key) in keys {
            let handle = cluster.client();
            let map = map.clone();
            writes.spawn(async move {
                let accepted = handle
                    .write_to_shard_with(&map, &key, b"v", &NODES, PATIENCE)
                    .await
                    .unwrap_or_else(|| panic!("shard {shard} never accepted a write"));
                (shard, accepted)
            });
        }

        let mut accepted_by: BTreeSet<NodeId> = BTreeSet::new();
        let mut answered = 0;
        while let Some(done) = writes.join_next().await {
            let (shard, node) = done.expect("a write task must not panic");
            assert!(
                map.replicas(shard).contains(&node),
                "shard {shard} was accepted by {node}, which does not replicate it"
            );
            accepted_by.insert(node);
            answered += 1;
        }
        assert_eq!(answered, SHARDS as usize, "not every shard's write completed");
        assert!(
            accepted_by.len() >= 2,
            "every shard's write went to one node ({accepted_by:?}); nothing proceeded in parallel"
        );
    }

    /// ✅ **Killing a node degrades only the shards it led, and only
    /// briefly.**
    ///
    /// Isolation rather than a kill, and it is the harder case: a killed node
    /// is simply gone, while an isolated one goes on believing it leads and
    /// campaigning into the void. What must hold either way is that a shard it
    /// did not lead keeps committing immediately — RF 3 minus one replica is
    /// still a quorum — and that the shards it did lead come back on their own.
    #[tokio::test]
    async fn isolating_a_node_degrades_only_the_shards_it_led() {
        let cluster = Cluster::sharded(&NODES, SHARDS, RF);
        let keys = key_per_shard(&cluster);
        let leaders = cluster.await_shard_leaders(&NODES, PATIENCE).await;

        // The node leading the most shards, so the test exercises the largest
        // disruption this placement allows.
        let victim = *NODES
            .iter()
            .max_by_key(|id| leaders.values().filter(|l| *l == *id).count())
            .expect("five nodes");
        let led: BTreeSet<ShardId> =
            leaders.iter().filter(|(_, l)| **l == victim).map(|(s, _)| *s).collect();
        assert!(!led.is_empty(), "no node led anything");

        let survivors: Vec<NodeId> = NODES.into_iter().filter(|&n| n != victim).collect();
        cluster.switchboard().isolate(&[victim], &NODES);

        // Immediately: a shard the victim did not lead must still commit. Its
        // leader is up and can still reach a quorum, so there is nothing to
        // wait for — a window here would be a window the criterion rules out.
        let map = cluster.placement().clone();
        let mut checked = 0;
        for (shard, key) in &keys {
            if led.contains(shard) {
                continue;
            }
            let accepted = cluster
                .write_to_shard_with(&map, key, b"after", &survivors, Duration::from_secs(2))
                .await;
            assert!(
                accepted.is_some(),
                "shard {shard} was not led by the isolated node {victim} but stopped committing"
            );
            checked += 1;
        }
        assert!(checked > 0, "the isolated node led every shard; nothing was left to check");

        // And briefly: the shards it did lead elect a new leader among the two
        // replicas that remain, which is still a quorum of three.
        for (shard, key) in &keys {
            if !led.contains(shard) {
                continue;
            }
            let accepted =
                cluster.write_to_shard_with(&map, key, b"recovered", &survivors, PATIENCE).await;
            assert!(
                accepted.is_some(),
                "shard {shard} never recovered after its leader {victim} was isolated"
            );
            assert_ne!(accepted, Some(victim));
        }
    }
}

/// Many groups on one tick loop must still all elect.
///
/// This is the regression guard for a bug the rest of the suite could not see.
/// `Driver::drain` used to reconcile the shared peer links after *every*
/// drain, which walks every group — free with one group, and 256 group visits
/// per inbound message with 256 of them. A measured three-node cluster at the
/// default shard count sat at 120% of a core per node and left about a tenth
/// of its shards permanently leaderless, thrashing elections, because the loop
/// could not keep up with its own ticks. Every in-process test hosted one
/// shard, so none of them noticed.
///
/// 256 shards on three nodes is 768 groups across three drivers. The number is
/// load-bearing: at 64 shards this test passes with the bug still in, because
/// the in-process harness has no serialization cost and the loop keeps up
/// anyway. At 256 it takes 0.3s to pass and times out at 30s without the fix —
/// so do not lower it without checking that it still fails for the right
/// reason.
#[tokio::test]
async fn every_shard_elects_a_leader_when_one_loop_drives_many() {
    use crate::tests::cluster::Cluster;

    const NODES: [kv_raft::NodeId; 3] = [1, 2, 3];
    let cluster = Cluster::sharded(&NODES, 256, 3);

    // Panics with the shards that did not elect, which is the useful failure.
    let leaders = cluster.await_shard_leaders(&NODES, Duration::from_secs(30)).await;
    assert_eq!(leaders.len(), 256, "not every shard elected: {leaders:?}");
}
