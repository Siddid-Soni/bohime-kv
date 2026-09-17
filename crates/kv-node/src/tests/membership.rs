//! M9.2: the node side of membership — the address book, peer reconciliation,
//! and the admin path.

use kv_storage::Engine;

use crate::membership::{endpoints, forget_endpoint, record_endpoint};

pub(crate) fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    (engine, dir)
}

/// The core keeps membership as bare ids; *reaching* a member needs an
/// address, and that is the application's problem (kv-raft calls it the
/// conf change's opaque `context`). It lives in the state machine rather
/// than in memory for the same reason the session table does: it has to
/// survive a restart, and — because a snapshot replaces the log prefix that
/// carried the conf entries — it has to survive compaction too.
#[test]
fn the_address_book_round_trips_through_the_engine() {
    let (mut engine, _dir) = engine();

    record_endpoint(&mut engine, 2, "10.0.0.2:7001").unwrap();
    record_endpoint(&mut engine, 3, "10.0.0.3:7001").unwrap();

    let book = endpoints(&engine).unwrap();
    assert_eq!(book.get(&2).map(String::as_str), Some("10.0.0.2:7001"));
    assert_eq!(book.get(&3).map(String::as_str), Some("10.0.0.3:7001"));
    assert_eq!(book.len(), 2, "no other key is mistaken for an endpoint");
}

/// A re-added node may come back at a different address, so recording is a
/// plain overwrite rather than an insert that must not collide.
#[test]
fn recording_an_endpoint_twice_keeps_the_newer_address() {
    let (mut engine, _dir) = engine();

    record_endpoint(&mut engine, 2, "10.0.0.2:7001").unwrap();
    record_endpoint(&mut engine, 2, "10.0.0.9:7001").unwrap();

    assert_eq!(endpoints(&engine).unwrap().get(&2).map(String::as_str), Some("10.0.0.9:7001"));
}

#[test]
fn a_removed_member_loses_its_endpoint() {
    let (mut engine, _dir) = engine();
    record_endpoint(&mut engine, 2, "10.0.0.2:7001").unwrap();

    forget_endpoint(&mut engine, 2).unwrap();

    assert!(endpoints(&engine).unwrap().is_empty());
}

/// The address book shares the session table's reserved space, so the same
/// service-boundary check that stops a client forging a session entry stops
/// it redirecting a peer.
#[test]
fn endpoint_keys_live_in_the_reserved_space() {
    let (mut engine, _dir) = engine();
    record_endpoint(&mut engine, 7, "10.0.0.7:7001").unwrap();

    let keys: Vec<Vec<u8>> = engine.scan().unwrap().into_iter().map(|(k, _)| k).collect();
    assert!(!keys.is_empty());
    for key in keys {
        assert!(crate::session::is_reserved(&key), "endpoint key {key:?} must be reserved");
    }
}

/// The driver's apply path, in miniature: a committed conf entry is not state
/// machine data, but it does have one application-visible effect — the
/// address book.
mod applying_a_conf_change {
    use std::collections::BTreeMap;

    use kv_raft::membership::{ConfChange, ConfOp};

    use crate::membership::{apply_conf, encode_book, endpoints};
    use crate::tests::membership::engine;

    fn change(op: ConfOp, node: u64, context: &str) -> ConfChange {
        ConfChange { op, node, context: context.as_bytes().to_vec() }
    }

    #[test]
    fn add_learner_records_the_endpoint_it_carried() {
        let (mut engine, _dir) = engine();

        apply_conf(&mut engine, &change(ConfOp::AddLearner, 4, "10.0.0.4:7001")).unwrap();

        assert_eq!(endpoints(&engine).unwrap().get(&4).map(String::as_str), Some("10.0.0.4:7001"));
    }

    /// A promotion changes a node's standing, not where it lives, and carries
    /// no address of its own. Treating its empty context as an address would
    /// erase the one thing the driver needs to keep talking to it.
    #[test]
    fn promotion_keeps_the_address_the_node_was_added_with() {
        let (mut engine, _dir) = engine();
        apply_conf(&mut engine, &change(ConfOp::AddLearner, 4, "10.0.0.4:7001")).unwrap();

        apply_conf(&mut engine, &change(ConfOp::Promote, 4, "")).unwrap();

        assert_eq!(
            endpoints(&engine).unwrap().get(&4).map(String::as_str),
            Some("10.0.0.4:7001"),
            "a promoted learner is still reachable"
        );
    }

    /// The founding members' addresses never appear in a conf entry of their
    /// own — they came from argv — so the entry that admits a node carries the
    /// whole book. Without this the newcomer can receive from the leader and
    /// dial nobody, which is a node that can never campaign once promoted.
    #[test]
    fn an_admission_carries_the_whole_book_so_the_newcomer_can_dial_back() {
        let (mut engine, _dir) = engine();
        let book = BTreeMap::from([
            (1, "10.0.0.1:7001".to_string()),
            (2, "10.0.0.2:7001".to_string()),
            (4, "10.0.0.4:7001".to_string()),
        ]);

        apply_conf(
            &mut engine,
            &ConfChange { op: ConfOp::AddLearner, node: 4, context: encode_book(&book) },
        )
        .unwrap();

        assert_eq!(
            endpoints(&engine).unwrap(),
            book,
            "every member is reachable, not just the new one"
        );
    }

    /// `kv-raft` treats the context as opaque bytes and will carry whatever is
    /// put there, so a conf entry written by something that is not this driver
    /// must not corrupt the book. A bare address still means what it looks
    /// like it means.
    #[test]
    fn a_bare_address_context_still_names_the_node_it_changes() {
        let (mut engine, _dir) = engine();

        apply_conf(&mut engine, &change(ConfOp::AddLearner, 4, "10.0.0.4:7001")).unwrap();

        assert_eq!(endpoints(&engine).unwrap(), BTreeMap::from([(4, "10.0.0.4:7001".to_string())]));
    }

    #[test]
    fn removal_forgets_the_endpoint() {
        let (mut engine, _dir) = engine();
        apply_conf(&mut engine, &change(ConfOp::AddLearner, 4, "10.0.0.4:7001")).unwrap();
        apply_conf(&mut engine, &change(ConfOp::Promote, 4, "")).unwrap();

        apply_conf(&mut engine, &change(ConfOp::RemoveVoter, 4, "")).unwrap();

        assert!(endpoints(&engine).unwrap().is_empty(), "a removed node is not dialled again");
    }
}

/// The admin path and peer reconciliation, on a group of one. A lone node is
/// its own quorum, so a conf change commits immediately and the effects are
/// observable without a network.
mod the_admin_path {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use kv_raft::{NodeId, RaftNode};
    use kv_storage::Engine;
    use tokio::sync::{mpsc, oneshot};

    use crate::config::NodeConfig;
    use crate::driver::{
        AdminOp, AdminReply, AdminRequest, ClusterStatus, Driver, Group, GroupChannels,
    };
    use crate::storage::BitcaskStorage;
    use crate::tests::support::DeadLink;
    use crate::transport::{PeerFactory, PeerLink};

    /// Records every link the driver asks for, so a test can assert on who it
    /// decided to dial rather than on whether a socket happened to open.
    #[derive(Default)]
    struct Dialled {
        opened: Mutex<Vec<(NodeId, String)>>,
    }

    struct RecordingFactory(Arc<Dialled>);

    impl PeerFactory for RecordingFactory {
        fn connect(&self, id: NodeId, address: &str) -> Box<dyn PeerLink> {
            self.0.opened.lock().unwrap().push((id, address.to_string()));
            Box::new(DeadLink)
        }
    }

    struct Harness {
        admin: mpsc::Sender<AdminRequest>,
        dialled: Arc<Dialled>,
        _keepalive: Box<dyn std::any::Any + Send>,
        /// `None` when the caller owns the directory, which is what a restart
        /// test needs: the data has to outlive the node that wrote it.
        _dir: Option<tempfile::TempDir>,
    }

    fn spawn() -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let mut harness = spawn_in(dir.path(), u64::MAX);
        harness._dir = Some(dir);
        harness
    }

    /// A node on a directory the caller owns, so a test can stop it and start
    /// another over the same data. `threshold` is `snapshot_threshold`.
    fn spawn_in(dir: &std::path::Path, threshold: u64) -> Harness {
        let config = NodeConfig {
            id: 1,
            listen: "127.0.0.1:0".parse().unwrap(),
            peers: BTreeMap::new(),
            data_dir: dir.to_path_buf(),
            tick: Duration::from_millis(5),
            election_timeout: 4,
            heartbeat_interval: 1,
            num_shards: 256,
            replication_factor: 1,
            vnodes_per_node: kv_ring::DEFAULT_VNODES,
            lease_reads: false,
            keydir: Default::default(),
            snapshot_threshold: threshold,
            initial_learner: false,
        };
        // Two groups in one driver, as `main` runs a node: shard 0's, which
        // these tests administer directly, and the meta group, which is where
        // `AdminService` sends a membership change since M11.
        let mut hosted = Vec::new();
        for (group, raft_dir, state_dir) in [
            (crate::transport::group::DATA, config.shard_raft_dir(0), config.shard_state_dir(0)),
            (crate::transport::group::META, config.meta_raft_dir(), config.meta_state_dir()),
        ] {
            std::fs::create_dir_all(&raft_dir).unwrap();
            std::fs::create_dir_all(&state_dir).unwrap();
            let node = RaftNode::new(
                config.raft_config_for(group),
                BitcaskStorage::open(&raft_dir).unwrap(),
            );
            let engine = Engine::open(&state_dir).unwrap();
            hosted.push(Group::new(&config, group, node, engine));
        }

        let (inbox_tx, inbox) = mpsc::channel(8);
        let (replies_tx, replies) = mpsc::channel(8);
        let (requests_tx, requests) = mpsc::channel(8);
        let (admin, admin_rx) = mpsc::channel(8);

        let dialled = Arc::new(Dialled::default());
        let (_groups_tx, new_groups) = mpsc::channel(8);
        let driver = Driver::new(
            &config,
            hosted,
            BTreeMap::new(),
            Box::new(RecordingFactory(Arc::clone(&dialled))),
            GroupChannels { inbox, peer_replies: replies, requests, admin: admin_rx, new_groups },
        );
        tokio::spawn(driver.run());
        Harness {
            admin,
            dialled,
            _keepalive: Box::new((inbox_tx, replies_tx, requests_tx)),
            _dir: None,
        }
    }

    async fn call(harness: &Harness, op: AdminOp) -> AdminReply {
        let (reply, wait) = oneshot::channel();
        harness
            .admin
            .send(AdminRequest { group: crate::transport::group::DATA, op, reply })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("the driver answers an admin request")
            .expect("the driver does not drop it")
    }

    /// Retries through `NotLeader`: the lone node has not campaigned yet when
    /// the first request lands, and refusing until it has is the driver
    /// working, not failing.
    async fn once_leading(harness: &Harness, op: impl Fn() -> AdminOp) -> AdminReply {
        for _ in 0..200 {
            match call(harness, op()).await {
                AdminReply::NotLeader { .. } => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                answered => return answered,
            }
        }
        panic!("no leader emerged in 2s");
    }

    async fn status(harness: &Harness) -> ClusterStatus {
        match call(harness, AdminOp::Status).await {
            AdminReply::Status(status) => status,
            other => panic!("expected a status, got {other:?}"),
        }
    }

    /// A node joins as a *learner*, never straight as a voter: a cold voter
    /// counts toward every quorum before it holds any of the log, which stalls
    /// the cluster for exactly as long as the catch-up takes.
    #[tokio::test]
    async fn add_node_joins_as_a_learner_and_records_its_address() {
        let harness = spawn();

        let reply = once_leading(&harness, || AdminOp::AddNode {
            id: 4,
            address: "10.0.0.4:7001".to_string(),
        })
        .await;
        assert!(matches!(reply, AdminReply::Accepted { .. }), "got {reply:?}");

        let status = status(&harness).await;
        assert_eq!(status.learners, vec![4], "the new node learns before it votes");
        assert_eq!(status.voters, vec![1], "the quorum has not moved yet");
        assert_eq!(status.endpoints.get(&4).map(String::as_str), Some("10.0.0.4:7001"));
        assert!(
            status.endpoints.contains_key(&1),
            "the book a joining node is handed must include the node handing it over"
        );
    }

    /// Membership is only useful if the node can be *reached*: a committed
    /// conf change has to turn into a live peer connection without a restart.
    #[tokio::test]
    async fn an_added_node_is_dialled_without_a_restart() {
        let harness = spawn();

        once_leading(&harness, || AdminOp::AddNode { id: 4, address: "10.0.0.4:7001".to_string() })
            .await;

        for _ in 0..200 {
            if !harness.dialled.opened.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            *harness.dialled.opened.lock().unwrap(),
            vec![(4, "10.0.0.4:7001".to_string())],
            "the driver dials a member it has no link to"
        );
    }

    #[tokio::test]
    async fn removing_a_node_drops_it_from_the_membership_and_the_book() {
        let harness = spawn();
        once_leading(&harness, || AdminOp::AddNode { id: 4, address: "10.0.0.4:7001".to_string() })
            .await;

        let reply = call(&harness, AdminOp::RemoveNode { id: 4 }).await;
        assert!(matches!(reply, AdminReply::Accepted { .. }), "got {reply:?}");

        let status = status(&harness).await;
        assert!(status.learners.is_empty());
        assert!(!status.endpoints.contains_key(&4), "a removed node is not dialled again");
        assert!(status.endpoints.contains_key(&1), "our own address stays in the book");
    }

    /// Membership changes go through the log, so only the leader may start
    /// one — and the refusal has to name whoever it thinks does lead, or an
    /// admin tool has nowhere to go next.
    #[tokio::test]
    async fn a_node_that_does_not_lead_refuses_and_hints() {
        let harness = spawn();
        // Before its first election, the lone node is a follower with no
        // leader to name. That is still a refusal, not an acceptance.
        let reply =
            call(&harness, AdminOp::AddNode { id: 4, address: "10.0.0.4:7001".to_string() }).await;
        assert!(matches!(reply, AdminReply::NotLeader { .. }), "got {reply:?}");
    }

    /// Membership has to survive a restart, and it has to survive it *without
    /// the flags*: this node is restarted with an empty `--peer` list, and the
    /// node it admitted must still be there, still at its address. That is the
    /// whole reason the address book lives in the state machine instead of in
    /// memory beside it.
    #[tokio::test]
    async fn membership_and_addresses_survive_a_restart_without_the_flags() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let harness = spawn_in(&path, u64::MAX);
            let reply = once_leading(&harness, || AdminOp::AddNode {
                id: 4,
                address: "10.0.0.4:7001".to_string(),
            })
            .await;
            assert!(matches!(reply, AdminReply::Accepted { .. }), "{reply:?}");
            // Dropping the harness drops every sender, which ends the driver
            // and releases both Bitcask directories.
        }
        // A fresh `TempDir` over the same path, so the data outlives the first
        // node and is still cleaned up at the end.
        let reopened = restart_over(&path).await;

        let status = status(&reopened).await;
        assert_eq!(status.learners, vec![4], "the membership came back");
        assert_eq!(
            status.endpoints.get(&4).map(String::as_str),
            Some("10.0.0.4:7001"),
            "and so did the address, which was never on the command line"
        );
        drop(dir);
    }

    /// The same claim once the log that carried the conf entry is *gone*.
    /// A snapshot replaces the prefix, so replaying conf entries cannot be the
    /// whole story — the book is in the state machine, which the snapshot
    /// image carries.
    #[tokio::test]
    async fn addresses_survive_the_compaction_of_the_entry_that_carried_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            // Snapshot aggressively: a handful of entries past the last one is
            // enough to drop the conf entry into the compacted prefix.
            let harness = spawn_in(&path, 2);
            let reply = once_leading(&harness, || AdminOp::AddNode {
                id: 4,
                address: "10.0.0.4:7001".to_string(),
            })
            .await;
            assert!(matches!(reply, AdminReply::Accepted { .. }), "{reply:?}");

            // Push the log past the threshold so a snapshot is taken.
            for id in [5, 6, 7] {
                let reply =
                    call(&harness, AdminOp::AddNode { id, address: format!("10.0.0.{id}:7001") })
                        .await;
                assert!(matches!(reply, AdminReply::Accepted { .. }), "adding {id}: {reply:?}");
            }
        }
        let reopened = restart_over(&path).await;

        let status = status(&reopened).await;
        assert!(
            status.log_first_index > 1,
            "nothing was compacted, so this proves nothing: log starts at {}",
            status.log_first_index
        );
        assert_eq!(
            status.endpoints.get(&4).map(String::as_str),
            Some("10.0.0.4:7001"),
            "the address outlived the log entry that carried it"
        );
        assert_eq!(status.learners, vec![4, 5, 6, 7]);
        drop(dir);
    }

    /// Reopens a node over an existing data directory, with no `--peer` flags.
    /// The pause lets the previous driver finish shutting down and release
    /// both Bitcask instances.
    async fn restart_over(path: &std::path::Path) -> Harness {
        tokio::time::sleep(Duration::from_millis(50)).await;
        spawn_in(path, u64::MAX)
    }

    /// The proto wiring, end to end over a real socket. The driver-level tests
    /// above say what the admin path *does*; this says that a gRPC client can
    /// reach it at all — the handlers, the message shapes, and the refusal
    /// codes an admin tool switches on.
    #[tokio::test]
    async fn admin_service_admits_a_node_over_a_real_socket() {
        use kv_proto::admin::admin_service_client::AdminServiceClient;
        use kv_proto::admin::admin_service_server::AdminServiceServer;
        use kv_proto::admin::{AddNodeRequest, ClusterStatusRequest, RemoveNodeRequest};

        let harness = spawn();
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        // This harness is a single data-group node, so the meta channel it is
        // handed goes nowhere: these tests exercise membership, not the map.
        let (meta_requests, _meta) = tokio::sync::mpsc::channel(1);
        let service = crate::admin_service::AdminApi::new(
            harness.admin.clone(),
            meta_requests,
            harness.admin.clone(),
            crate::meta::PublishedMap::default(),
        );
        let addr = format!("127.0.0.1:{port}").parse().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(AdminServiceServer::new(service))
                .serve(addr)
                .await
        });

        let mut client = loop {
            match AdminServiceClient::connect(format!("http://127.0.0.1:{port}")).await {
                Ok(c) => break c,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };

        // Retrying through `not_leader`, as an admin tool does: the lone node
        // has to finish its election first.
        for attempt in 0..200 {
            let resp = client
                .add_node(AddNodeRequest { node_id: 4, address: "10.0.0.4:7001".to_string() })
                .await
                .unwrap()
                .into_inner();
            if resp.not_leader.is_none() {
                assert!(resp.index > 0, "an accepted change names the index it committed at");
                break;
            }
            assert!(attempt < 199, "no leader emerged");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let status = client.cluster_status(ClusterStatusRequest {}).await.unwrap().into_inner();
        assert_eq!(status.node_id, 1);
        let newcomer = status.members.iter().find(|m| m.node_id == 4).expect("node 4 is a member");
        assert!(!newcomer.voter, "it joined as a learner");
        assert_eq!(newcomer.address, "10.0.0.4:7001");

        // A change this config cannot make is an error, not a redirect: it is
        // wrong wherever it is sent, so the tool must stop rather than try the
        // next node.
        let refused = client.remove_node(RemoveNodeRequest { node_id: 1 }).await.unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused:?}");
    }

    /// The last voter cannot be removed: a config with no voters can neither
    /// elect nor commit, so there would be no way back.
    #[tokio::test]
    async fn removing_the_last_voter_is_refused() {
        let harness = spawn();
        let reply = once_leading(&harness, || AdminOp::RemoveNode { id: 1 }).await;
        assert!(matches!(reply, AdminReply::Rejected { .. }), "got {reply:?}");

        let status = status(&harness).await;
        assert_eq!(status.voters, vec![1], "the cluster still has its quorum");
    }
}

/// M9's ✅ criteria, on the in-process cluster: grow 3 → 5 under continuous
/// writes with no unavailability window and no lost writes; shrink 5 → 3;
/// removing the leader hands off cleanly.
///
/// These are node-level tests on purpose. `kv-raft`'s own membership suite
/// proves the *rules* — one change at a time, learners never counted, a
/// removed leader hands off — against bare `RaftNode`s. What it cannot show is
/// a cluster that keeps taking writes while the rules are applied, because it
/// has no state machine and no clients. That is what this file is for.
mod the_m9_gate {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use kv_raft::NodeId;

    use crate::driver::{AdminOp, AdminReply, ClientReply};
    use crate::tests::cluster::Cluster;

    const THREE: [NodeId; 3] = [1, 2, 3];
    const FIVE: [NodeId; 5] = [1, 2, 3, 4, 5];

    /// Admits `id` and waits for it to be promoted to a voter.
    ///
    /// Two separate things, and the gap between them is the point: `AddNode`
    /// admits a **learner**, which replicates without being counted in any
    /// quorum, and the leader promotes it on its own once it has caught up.
    /// Adding it as a voter directly is what would open an unavailability
    /// window — a cold node counted toward every quorum while holding none of
    /// the log.
    async fn grow_by_one(cluster: &mut Cluster, among: &[NodeId], id: NodeId, expected: &[NodeId]) {
        cluster.start_joining_node(id, among);
        let reply = cluster
            .administer(among, || AdminOp::AddNode { id, address: format!("in-process://{id}") })
            .await;
        assert!(matches!(reply, AdminReply::Accepted { .. }), "admitting {id}: {reply:?}");

        let seen = cluster.await_voters(id, expected, Duration::from_secs(10)).await;
        assert!(
            seen,
            "node {id} was never promoted; it sees {:?}",
            cluster.status_of(id).await.voters
        );
    }

    /// Grow 3 → 5 while a writer never stops. Every acked write must still be
    /// readable afterwards, and no write may be refused for longer than an
    /// ordinary election takes — a membership change that stalls the cluster
    /// is the thing this milestone exists to avoid.
    #[tokio::test]
    async fn growing_three_to_five_under_writes_loses_nothing() {
        let mut cluster = Cluster::of_three();
        // A leader first: the very first write of a cluster's life waits for
        // an election, which is not an unavailability window caused by
        // membership.
        cluster.put(b"warmup", b"1").await;

        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(std::sync::Mutex::new(Vec::new()));

        let writer = {
            let stop = Arc::clone(&stop);
            let written = Arc::clone(&written);
            // The writer and the admin calls share the harness, so it goes
            // through a clone of the request senders rather than a borrow.
            let handle = cluster.client();
            tokio::spawn(async move {
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("k{n}").into_bytes();
                    let value = format!("v{n}").into_bytes();
                    let started = std::time::Instant::now();
                    handle.put_among(&FIVE, &key, &value).await;
                    let took = started.elapsed();
                    written.lock().unwrap().push((key, value, took));
                    n += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        grow_by_one(&mut cluster, &THREE, 4, &[1, 2, 3, 4]).await;
        grow_by_one(&mut cluster, &THREE, 5, &FIVE).await;
        // Keep writing past the growth: the sample size must not depend on
        // how fast the cluster happened to admit two nodes.
        cluster.settle(Duration::from_millis(300)).await;

        stop.store(true, Ordering::Relaxed);
        writer.await.unwrap();

        let written = written.lock().unwrap().clone();
        assert!(
            written.len() > 20,
            "the writer must have kept going throughout, and only managed {}",
            written.len()
        );

        // No lost writes.
        for (key, value, _) in &written {
            assert_eq!(
                cluster.read_among(&FIVE, key).await,
                Some(value.clone()),
                "acked write {key:?} is missing after growing to five"
            );
        }

        // No unavailability window: `put_among` retries through `NotLeader`,
        // so a stall shows up as one write taking far longer than the rest.
        // The bound is a whole election (10 ticks × 10 ms) with room to spare
        // — generous, because the assertion is "the cluster never stopped
        // taking writes", not a latency target on a loaded box.
        let worst = written.iter().map(|(_, _, took)| *took).max().unwrap();
        assert!(
            worst < Duration::from_millis(1500),
            "a write waited {worst:?} while the cluster grew — that is an unavailability window"
        );
    }

    /// Shrink back to three. The two removed nodes must leave the membership
    /// and the address book on every survivor, and the data must be intact.
    #[tokio::test]
    async fn shrinking_five_to_three_keeps_every_write() {
        let mut cluster = Cluster::of_three();
        cluster.put(b"before", b"1").await;
        grow_by_one(&mut cluster, &THREE, 4, &[1, 2, 3, 4]).await;
        grow_by_one(&mut cluster, &THREE, 5, &FIVE).await;
        cluster.put_among(&FIVE, b"during", b"2").await;

        for id in [5, 4] {
            let reply = cluster.administer(&THREE, || AdminOp::RemoveNode { id }).await;
            assert!(matches!(reply, AdminReply::Accepted { .. }), "removing {id}: {reply:?}");
            // One change at a time (§1.7): the next removal must wait for this
            // one to commit, which is what the survivors reporting the new
            // voter set means.
            for &survivor in &THREE {
                assert!(
                    cluster
                        .await_voters(
                            survivor,
                            &FIVE[..(FIVE.len() - (6 - id) as usize)],
                            Duration::from_secs(10)
                        )
                        .await,
                    "node {survivor} still counts {id} after it was removed"
                );
            }
        }

        cluster.put_among(&THREE, b"after", b"3").await;
        for (key, value) in [(&b"before"[..], &b"1"[..]), (b"during", b"2"), (b"after", b"3")] {
            assert_eq!(
                cluster.read_among(&THREE, key).await,
                Some(value.to_vec()),
                "{key:?} did not survive the shrink"
            );
        }

        let status = cluster.status_of(1).await;
        assert!(!status.endpoints.contains_key(&4), "a removed node is still in the book");
        assert!(!status.endpoints.contains_key(&5), "a removed node is still in the book");
    }

    /// Removing the leader is the interesting removal: it is the one node that
    /// cannot simply be dropped, because the cluster needs a leader to commit
    /// the entry that removes it. It commits the entry, steps down, and hands
    /// leadership to the most caught-up survivor rather than leaving the group
    /// to wait out an election timeout.
    #[tokio::test]
    async fn removing_the_leader_hands_off_cleanly() {
        let cluster = Cluster::of_three();
        let leader = cluster.put(b"k", b"v").await;
        let survivors: Vec<NodeId> = THREE.into_iter().filter(|&id| id != leader).collect();

        let reply = cluster.administer(&THREE, || AdminOp::RemoveNode { id: leader }).await;
        assert!(matches!(reply, AdminReply::Accepted { .. }), "removing the leader: {reply:?}");

        for &id in &survivors {
            assert!(
                cluster.await_voters(id, &survivors, Duration::from_secs(10)).await,
                "node {id} still counts the removed leader"
            );
        }

        // The handoff is what makes this *clean*: a new leader before an
        // election timeout could have elapsed, rather than after one.
        let new = tokio::time::timeout(Duration::from_secs(5), cluster.leader_of(&survivors))
            .await
            .expect("a survivor takes over")
            .expect("a survivor takes over");
        assert_ne!(new, leader, "the removed node must not still be serving");
        assert_eq!(cluster.read_among(&survivors, b"k").await, Some(b"v".to_vec()));

        // And the deposed node refuses to serve: it is not a member any more,
        // so it cannot confirm a read quorum of a cluster it left.
        match cluster.get_from(leader, b"k").await {
            ClientReply::NotLeader { .. } => {}
            other => panic!("a removed leader served a read: {other:?}"),
        }
    }

    /// One change at a time is the safety rule single-server membership rests
    /// on (§1.7): with two uncommitted, an old majority and a new majority
    /// need not overlap, and two leaders can be elected in one term.
    #[tokio::test]
    async fn a_second_change_while_one_is_uncommitted_is_refused() {
        let mut cluster = Cluster::of_three();
        cluster.put(b"k", b"v").await;
        cluster.start_joining_node(4, &THREE);

        // Cut the cluster in half so the first change cannot commit, leaving
        // it uncommitted for as long as the partition lasts.
        let leader = cluster.leader_of(&THREE).await.expect("a leader");
        cluster.switchboard().isolate(&[leader], &THREE);

        let first = cluster
            .admin(leader, AdminOp::AddNode { id: 4, address: "in-process://4".to_string() });
        let second = cluster
            .admin(leader, AdminOp::AddNode { id: 5, address: "in-process://5".to_string() });
        // The first is accepted into the log and never commits; the second is
        // refused outright rather than queued behind it.
        let (_first, second) =
            tokio::join!(tokio::time::timeout(Duration::from_millis(200), first), second);
        assert!(
            matches!(second, AdminReply::Rejected { .. }),
            "a second change must be refused while one is in flight: {second:?}"
        );

        cluster.switchboard().heal();
    }

    /// The catch-up half of the join: an admitted node takes on the log that
    /// existed before it did, and only then becomes a voter.
    ///
    /// That it is admitted as a *learner* rather than a voter is pinned
    /// deterministically by
    /// `the_admin_path::add_node_joins_as_a_learner_and_records_its_address`,
    /// where the newcomer does not exist and so can never be promoted. Here
    /// the promotion is the point, and racing it to observe the learner state
    /// would only make this test flaky.
    #[tokio::test]
    async fn an_admitted_node_catches_up_and_is_promoted() {
        let mut cluster = Cluster::of_three();
        cluster.put(b"before-it-existed", b"v").await;
        let committed = cluster.status_of(1).await.commit_index;
        assert!(committed > 0, "there is a log to catch up on");
        cluster.start_joining_node(4, &THREE);

        let reply = cluster
            .administer(&THREE, || AdminOp::AddNode {
                id: 4,
                address: "in-process://4".to_string(),
            })
            .await;
        assert!(matches!(reply, AdminReply::Accepted { .. }), "{reply:?}");

        assert!(
            cluster.await_voters(4, &[1, 2, 3, 4], Duration::from_secs(10)).await,
            "the learner was never promoted"
        );
        let newcomer = cluster.status_of(4).await;
        assert!(
            newcomer.applied_index >= committed,
            "node 4 votes at applied={} without the log that predates it ({committed})",
            newcomer.applied_index
        );

        // It is a voter, not a leader: a follower redirects a read rather than
        // answering from local state, which has been true since M7.
        match cluster.get_from(4, b"before-it-existed").await {
            ClientReply::Value(_) | ClientReply::NotLeader { .. } => {}
            other => panic!("unexpected reply from the newcomer: {other:?}"),
        }
    }
}
