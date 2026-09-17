//! M10's gate (M10.9): the three ✅ criteria from `docs/DESIGN.md`, asserted
//! against a running cluster rather than against `kv-ring` in isolation.
//!
//! - key→shard is stable across restarts;
//! - adding a node moves ≈`1/N` of shards and no more;
//! - the shard map is itself linearizable.
//!
//! `kv-ring`'s own tests prove the placement *function* has these properties.
//! What this file adds is that the running system actually has them: that a
//! restart replays the same placement, that a real membership change produces
//! it, and that a partitioned meta leader cannot hand out a stale one.

pub(crate) mod the_m10_gate {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use kv_raft::{NodeId, RaftNode};
    use kv_ring::ShardMap;
    use kv_storage::Engine;
    use tokio::sync::mpsc;

    use crate::config::NodeConfig;
    use crate::driver::{AdminOp, AdminReply, ClientOp, ClientReply, Driver, Group, GroupChannels};
    use crate::meta::{MetaReconciler, PublishedMap};
    use crate::shard_map::SHARD_MAP_KEY;
    use crate::storage::BitcaskStorage;
    use crate::tests::cluster::{ALL, Cluster};
    use crate::tests::support::SilentPeers;
    use crate::transport::group::{self, GroupId};

    const PATIENCE: Duration = Duration::from_secs(10);

    /// A lone node running both groups over a directory the caller owns, so a
    /// test can stop it and start another over the same data. One voter, so it
    /// elects itself immediately in both groups and needs no peers.
    struct Node {
        published: PublishedMap,
        _keepalive: Box<dyn std::any::Any + Send>,
    }

    fn config_on(dir: &std::path::Path) -> NodeConfig {
        NodeConfig {
            id: 1,
            listen: "127.0.0.1:0".parse().unwrap(),
            peers: BTreeMap::new(),
            data_dir: dir.to_path_buf(),
            tick: Duration::from_millis(5),
            election_timeout: 4,
            heartbeat_interval: 1,
            lease_reads: false,
            keydir: Default::default(),
            snapshot_threshold: u64::MAX,
            initial_learner: false,
            num_shards: 256,
            // A one-node cluster cannot hold three replicas, and M10 refuses
            // rather than under-replicating — so a lone node is an RF=1
            // cluster by necessity.
            replication_factor: 1,
            vnodes_per_node: kv_ring::DEFAULT_VNODES,
        }
    }

    fn start(dir: &std::path::Path) -> Node {
        let config = config_on(dir);
        let mut keepalive: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
        let mut channels = BTreeMap::new();

        for (group, raft_dir, state_dir) in [
            (group::DATA, config.shard_raft_dir(0), config.shard_state_dir(0)),
            (group::META, config.meta_raft_dir(), config.meta_state_dir()),
        ] {
            std::fs::create_dir_all(&raft_dir).unwrap();
            std::fs::create_dir_all(&state_dir).unwrap();

            let node = RaftNode::new(
                config.raft_config_for(group),
                BitcaskStorage::open(&raft_dir).unwrap(),
            );
            let engine = Engine::open(&state_dir).unwrap();

            let (inbox_tx, inbox) = mpsc::channel(8);
            let (replies_tx, replies) = mpsc::channel(8);
            let (requests_tx, requests) = mpsc::channel(8);
            let (admin_tx, admin) = mpsc::channel(8);
            let (groups_tx, new_groups) = mpsc::channel(8);
            let driver = Driver::new(
                &config,
                vec![Group::new(&config, group, node, engine)],
                BTreeMap::new(),
                Box::new(SilentPeers),
                GroupChannels { inbox, peer_replies: replies, requests, admin, new_groups },
            );
            tokio::spawn(driver.run());

            channels.insert(group, (requests_tx.clone(), admin_tx.clone()));
            keepalive.push(Box::new((inbox_tx, replies_tx, requests_tx, admin_tx, groups_tx)));
        }

        let published = PublishedMap::default();
        tokio::spawn(
            MetaReconciler::new(
                config.clone(),
                channels[&group::META].1.clone(),
                channels[&group::META].0.clone(),
                Arc::clone(&published),
                Duration::from_millis(10),
            )
            .run(),
        );

        Node { published, _keepalive: Box::new(keepalive) }
    }

    impl Node {
        async fn map(&self) -> Arc<ShardMap> {
            let deadline = std::time::Instant::now() + PATIENCE;
            while std::time::Instant::now() < deadline {
                if let Some(map) = self.published.load_full() {
                    return map;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!("the node never published a shard map");
        }
    }

    /// The keys the gate routes. Spread rather than sequential, so a placement
    /// that happened to be right for one arc of the ring is not mistaken for
    /// one that is right everywhere.
    fn sample_keys() -> Vec<Vec<u8>> {
        (0..64u32).map(|i| format!("user:{}", i * 37).into_bytes()).collect()
    }

    /// ✅ **Key→shard is stable across restarts.**
    ///
    /// Two independent things could break this and the test covers both: the
    /// hash could change (it is pinned by golden vectors in `kv-ring`, and
    /// `DefaultHasher` would have made it change on a toolchain upgrade), or
    /// the *map* could fail to survive — it lives in the meta group's state
    /// machine precisely so that a restart replays it instead of recomputing
    /// it from whatever flags the process happened to be given.
    #[tokio::test]
    async fn key_to_shard_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();

        let before = {
            let node = start(dir.path());
            let map = node.map().await;
            let routing: Vec<(u16, Vec<NodeId>)> = sample_keys()
                .iter()
                .map(|k| (map.shard_for_key(k), map.replicas_for_key(k).to_vec()))
                .collect();
            (map.version, map.encode(), routing)
        };

        // Everything above is dropped: the drivers' channels close and the
        // engines are reopened from disk by the next start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let node = start(dir.path());
        let map = node.map().await;

        assert_eq!(map.version, before.0, "the restart bootstrapped a second map");
        assert_eq!(map.encode(), before.1, "the replayed map differs byte for byte");
        for (key, (shard, replicas)) in sample_keys().iter().zip(before.2) {
            assert_eq!(map.shard_for_key(key), shard, "key {key:?} moved shard across a restart");
            assert_eq!(map.replicas_for_key(key), replicas, "key {key:?} moved node");
        }
    }

    /// ✅ **Adding a node moves ≈`1/N` of shards and no more.**
    ///
    /// The "and no more" is the sharp half, and it is stronger than a
    /// statistic: a replica may only move *onto* the new node. Anything moving
    /// between two nodes that both stayed is a Raft membership change and a
    /// full data copy that nothing asked for.
    #[tokio::test]
    async fn adding_a_node_moves_a_fair_share_and_nothing_else() {
        let mut cluster = Cluster::of_three();
        let before = cluster.await_shard_map(PATIENCE).await.expect("a map");
        assert_eq!(before.nodes, ALL.into_iter().collect());

        cluster.start_joining_node(4, &ALL);
        let reply = cluster
            .administer_meta(&ALL, || AdminOp::AddNode { id: 4, address: "in-process://4".into() })
            .await;
        assert!(matches!(reply, AdminReply::Accepted { .. }), "admitting node 4: {reply:?}");

        let want: BTreeSet<NodeId> = [1, 2, 3, 4].into_iter().collect();
        let after = {
            let deadline = std::time::Instant::now() + PATIENCE;
            loop {
                let map = cluster.await_shard_map(PATIENCE).await.expect("a map");
                if map.nodes == want {
                    break map;
                }
                assert!(std::time::Instant::now() < deadline, "the map never grew to four nodes");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };

        // ...and no more: every slot that changed hands went to node 4.
        //
        // The quantity that matters is **replica slots**, not shards touched.
        // With RF=3 on four nodes each shard holds three of the four, so node 4
        // appears in about three quarters of all shards — which says nothing
        // about movement. What moved is one slot per shard it joined, out of
        // `num_shards * rf` in the cluster.
        let rf = before.replication_factor as usize;
        let total_slots = before.num_shards as usize * rf;
        let mut moved_slots = 0usize;
        for shard in 0..before.num_shards {
            let (old, new) = (before.replicas(shard), after.replicas(shard));
            if new.contains(&4) {
                moved_slots += 1;
                let survivors: Vec<NodeId> = new.iter().copied().filter(|&n| n != 4).collect();
                assert_eq!(
                    survivors,
                    old[..rf - 1].to_vec(),
                    "shard {shard}: admitting node 4 disturbed the replicas that stayed"
                );
            } else {
                assert_eq!(new, old, "shard {shard} was reshuffled without gaining node 4");
            }
        }

        // ≈1/N: a fourth node should end up holding about a quarter of the
        // cluster's replica slots, and every one of them is a slot that moved.
        let fair = total_slots as f64 / 4.0;
        assert!(
            (moved_slots as f64) > fair * 0.5 && (moved_slots as f64) < fair * 1.6,
            "node 4 took {moved_slots} of {total_slots} replica slots; \
             a fair share is {fair:.1}"
        );
    }

    /// ✅ **The shard map is itself linearizable.**
    ///
    /// A partitioned meta leader still believes it leads. The only thing
    /// stopping it answering a map read from its own applied state is the
    /// quorum confirmation ReadIndex forces — the same machinery M7 built for
    /// user data, pointed at cluster metadata. Serving a stale *placement*
    /// would route a client's writes to a group that no longer owns the shard.
    #[tokio::test]
    async fn a_partitioned_meta_leader_cannot_serve_the_map() {
        let cluster = Cluster::of_three();
        cluster.await_shard_map(PATIENCE).await.expect("a map");
        let leader = cluster.meta_leader_of(&ALL).await.expect("the meta group elects");

        cluster.switchboard().isolate(&[leader], &ALL);
        cluster.settle(Duration::from_secs(1)).await;

        // It cannot confirm a quorum, so it must not answer — even though its
        // own copy is sitting right there and is very probably correct.
        let reply = cluster
            .try_meta_call(
                leader,
                ClientOp::Get { key: SHARD_MAP_KEY.to_vec() },
                Duration::from_secs(3),
            )
            .await;
        assert!(
            matches!(reply, None | Some(ClientReply::NotLeader { .. })),
            "an isolated meta leader answered a linearizable map read with {reply:?}"
        );

        // The majority side, meanwhile, still answers.
        let survivors: Vec<NodeId> = ALL.into_iter().filter(|&n| n != leader).collect();
        let new_leader =
            cluster.meta_leader_of(&survivors).await.expect("the survivors elect a meta leader");
        assert_ne!(new_leader, leader);
        assert!(
            matches!(
                cluster
                    .try_meta_call(
                        new_leader,
                        ClientOp::Get { key: SHARD_MAP_KEY.to_vec() },
                        PATIENCE
                    )
                    .await,
                Some(ClientReply::Value(Some(_)))
            ),
            "the majority side cannot read its own map"
        );
    }

    /// The gate's quiet precondition: two Raft groups on every node, and
    /// neither one's traffic reaching the other's log.
    #[tokio::test]
    async fn both_groups_run_on_every_node() {
        let cluster = Cluster::of_three();
        cluster.await_shard_map(PATIENCE).await.expect("a map");

        for &id in &ALL {
            let data = cluster.status_of(id).await;
            let meta = cluster.meta_status_of(id).await;
            assert_eq!(data.id, id);
            assert_eq!(meta.id, id);
            assert_eq!(
                meta.voters,
                ALL.to_vec(),
                "node {id}'s meta group does not see the whole cluster"
            );
        }
    }

    /// `GroupId` is not part of `kv-raft`, and this is the reminder why: the
    /// core has no idea it is one of several instances. If this ever needs to
    /// change, the purity boundary M4's determinism rests on is what is being
    /// traded away.
    #[test]
    fn the_core_does_not_know_about_groups() {
        let _: GroupId = group::META;
        assert_ne!(group::DATA, group::META);
    }
}
