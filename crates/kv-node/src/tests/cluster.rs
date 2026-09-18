//! An in-process cluster: real `Driver`s, real `RaftNode`s, real Bitcask,
//! wired to each other through channels instead of gRPC, behind a network that
//! can be partitioned.
//!
//! **Why this exists.** M7's gate needs a deposed leader that still believes it
//! leads, which means a partition. `kv-sim` can partition, but it drives bare
//! `RaftNode`s — no state machine, no `Get`, so it cannot observe a stale
//! *read*. M6's end-to-end test drives real processes over loopback, which
//! cannot be partitioned without root. Neither can express the scenario, so
//! this can.
//!
//! **Fidelity.** The real transport is request/response: `PeerClient` sends a
//! request and routes the *reply* back into the driver's `peer_replies`.
//! `TestLink` does exactly the same thing, so what runs here speaks the
//! protocol the node actually speaks rather than a convenient simplification.
//!
//! Time is real here, not virtual. That is deliberate: `kv-sim` already owns
//! deterministic core testing, and a second virtual clock would be a second
//! simulator to keep honest. This harness exists to test the *node*.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kv_raft::{Message, NodeId, RaftNode};
use kv_ring::{ShardId, ShardMap};
use kv_storage::{Engine, ReadHold, ReadViewFactory};
use tokio::sync::{mpsc, oneshot};

use crate::config::NodeConfig;
use crate::driver::{
    AdminOp, AdminReply, AdminRequest, ClientOp, ClientReply, ClientRequest, ClusterStatus, Driver,
    Group, GroupChannels, ShardStatus,
};
use crate::meta::{MetaReconciler, PublishedMap};
use crate::storage::BitcaskStorage;
use crate::transport::group::{self, GroupId};
use crate::transport::peer::SendError;
use crate::transport::server::{GroupRegistry, Inbound};
use crate::transport::{PeerFactory, PeerLink};

pub(crate) const ALL: [NodeId; 3] = [1, 2, 3];

/// The message kinds M8's gate needs to observe on the wire. Only kinds are
/// recorded, never payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Traffic {
    InstallSnapshot,
    AppendEntries { entries: usize },
    InstallSnapshotResp { success: bool },
}

fn classify(msg: &Message) -> Option<Traffic> {
    match msg {
        Message::InstallSnapshot { .. } => Some(Traffic::InstallSnapshot),
        Message::AppendEntries { entries, .. } => {
            Some(Traffic::AppendEntries { entries: entries.len() })
        }
        Message::InstallSnapshotResp { success, .. } => {
            Some(Traffic::InstallSnapshotResp { success: *success })
        }
        _ => None,
    }
}

/// Who can talk to whom. A partition blocks both directions, because a real
/// one does.
pub(crate) struct Switchboard {
    blocked: Mutex<BTreeSet<(NodeId, NodeId)>>,
    traffic: Mutex<Vec<(NodeId, NodeId, Traffic)>>,
}

impl Default for Switchboard {
    fn default() -> Self {
        Self { blocked: Mutex::new(BTreeSet::new()), traffic: Mutex::new(Vec::new()) }
    }
}

impl Switchboard {
    pub(crate) fn allows(&self, from: NodeId, to: NodeId) -> bool {
        !self.blocked.lock().unwrap().contains(&(from, to))
    }

    /// Cuts `group` off from every node outside it, both directions.
    pub(crate) fn isolate(&self, group: &[NodeId], all: &[NodeId]) {
        let mut blocked = self.blocked.lock().unwrap();
        for &a in group {
            for &b in all {
                if !group.contains(&b) {
                    blocked.insert((a, b));
                    blocked.insert((b, a));
                }
            }
        }
    }

    pub(crate) fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }

    /// Records an observable message kind. The M8 gate asserts on this log:
    /// a follower that caught up via snapshot must have been *sent* one, and
    /// must have answered it successfully before any tail appends flowed.
    pub(crate) fn note(&self, from: NodeId, to: NodeId, traffic: Traffic) {
        self.traffic.lock().unwrap().push((from, to, traffic));
    }

    pub(crate) fn traffic(&self) -> Vec<(NodeId, NodeId, Traffic)> {
        self.traffic.lock().unwrap().clone()
    }
}

/// Every group's inbox on every node, by `(node, group)`.
///
/// A registry rather than a link holding its peer's `Sender` directly, because
/// since M9 a node can be *admitted* to a running cluster: the link exists
/// before the node it addresses does, exactly as a `PeerClient` does when the
/// process it dials has not started yet.
///
/// Keyed by group as well as node since M10: a node hosts the data group and
/// the meta group at one address, and a link that ignored the group would
/// deliver one group's AppendEntries into the other's log — which is what the
/// real `RaftService` uses the wire group id to prevent.
#[derive(Default)]
pub(crate) struct Registry {
    inboxes: Mutex<BTreeMap<(NodeId, GroupId), mpsc::Sender<Inbound>>>,
}

impl Registry {
    fn register(&self, id: NodeId, group: GroupId, inbox: mpsc::Sender<Inbound>) {
        self.inboxes.lock().unwrap().insert((id, group), inbox);
    }

    fn inbox(&self, id: NodeId, group: GroupId) -> Option<mpsc::Sender<Inbound>> {
        self.inboxes.lock().unwrap().get(&(id, group)).cloned()
    }
}

/// One node's view of one peer. Mirrors `PeerClient`: send a request, route
/// the reply back into the sender's own `peer_replies`.
struct TestLink {
    me: NodeId,
    peer: NodeId,
    registry: Arc<Registry>,
    my_replies: mpsc::Sender<(GroupId, NodeId, Message)>,
    switchboard: Arc<Switchboard>,
}

/// Opens `TestLink`s on the driver's behalf, so a conf change can bring a peer
/// into an already-running node the same way the real `GrpcPeers` does.
struct TestPeers {
    me: NodeId,
    registry: Arc<Registry>,
    my_replies: mpsc::Sender<(GroupId, NodeId, Message)>,
    switchboard: Arc<Switchboard>,
}

impl PeerFactory for TestPeers {
    fn connect(&self, id: NodeId, _address: &str) -> Box<dyn PeerLink> {
        Box::new(TestLink {
            me: self.me,
            peer: id,
            registry: Arc::clone(&self.registry),
            my_replies: self.my_replies.clone(),
            switchboard: Arc::clone(&self.switchboard),
        })
    }
}

impl PeerLink for TestLink {
    fn try_send(&self, group: GroupId, msg: Message) -> Result<(), SendError> {
        // Outbound drop: the request never reaches the peer.
        if !self.switchboard.allows(self.me, self.peer) {
            return Ok(());
        }
        // A peer that has not started yet: the real client would be retrying a
        // refused connection. Shedding is the same observable outcome.
        let Some(peer_inbox) = self.registry.inbox(self.peer, group) else {
            return Ok(());
        };
        if group == group::DATA
            && let Some(traffic) = classify(&msg)
        {
            self.switchboard.note(self.me, self.peer, traffic);
        }
        let (reply, wait) = oneshot::channel();
        peer_inbox
            .try_send(Inbound { group, from: self.me, msg, reply })
            .map_err(|_| SendError::Full)?;

        let (me, peer) = (self.me, self.peer);
        let replies = self.my_replies.clone();
        let switchboard = Arc::clone(&self.switchboard);
        tokio::spawn(async move {
            if let Ok(answer) = wait.await {
                // Inbound drop: the reply is lost on the way back, which a
                // partition does just as readily as losing the request.
                if switchboard.allows(peer, me) {
                    if group == group::DATA
                        && let Some(traffic) = classify(&answer)
                    {
                        switchboard.note(peer, me, traffic);
                    }
                    let _ = replies.try_send((group, peer, answer));
                }
            }
        });
        Ok(())
    }
}

pub(crate) struct Cluster {
    /// What the *map* describes, which the nodes' flags must agree with or the
    /// reconciler aborts them.
    num_shards: u16,
    replication_factor: u8,
    /// The placement the harness founded its groups from, for a sharded
    /// cluster. `None` for the single-group harnesses, which name their one
    /// group directly.
    placement: Option<ShardMap>,
    requests: BTreeMap<NodeId, mpsc::Sender<ClientRequest>>,
    admin: BTreeMap<NodeId, mpsc::Sender<AdminRequest>>,
    /// The meta group's channels, node for node beside the data group's.
    meta_requests: BTreeMap<NodeId, mpsc::Sender<ClientRequest>>,
    meta_admin: BTreeMap<NodeId, mpsc::Sender<AdminRequest>>,
    /// What each node's reconciler has published for its request handlers.
    published: BTreeMap<NodeId, PublishedMap>,
    /// Each node's reader-side handle on its data group's state machine
    /// (M11.5). The harness keeps one so a test can park a reader inside the
    /// published keydir copy and make the applied-but-not-yet-visible window
    /// deterministic.
    read_views: BTreeMap<NodeId, ReadViewFactory>,
    /// Each supervised node's migration driver, so a test can run a pass on
    /// demand rather than waiting out an interval.
    migrators: BTreeMap<NodeId, Arc<crate::migrate::MigrationDriver>>,
    registry: Arc<Registry>,
    switchboard: Arc<Switchboard>,
    /// Kept alive for the driver's sake: dropping a node's inbox or peer-reply
    /// sender closes that `select!` arm under it.
    _keepalive: Vec<Box<dyn std::any::Any + Send>>,
    _dirs: Vec<tempfile::TempDir>,
}

/// The channels one running driver is reached on. Mirrors `main`'s own
/// struct of the same name, for the same reason: a supervisor needs the inbox
/// and the new-group sender as well as the request and admin ones.
struct DriverHandles {
    requests: mpsc::Sender<ClientRequest>,
    admin: mpsc::Sender<AdminRequest>,
    inbox: mpsc::Sender<Inbound>,
    new_groups: mpsc::Sender<Group>,
}

/// How one node is configured when the harness starts it.
struct Spec {
    id: NodeId,
    /// The members this node is told about at boot. Empty for a node that
    /// joins an existing cluster: it learns the membership from the log.
    peers: Vec<NodeId>,
    /// True for a node admitted at runtime — it starts as a learner rather
    /// than as a founding voter.
    joining: bool,
    lease_reads: bool,
    /// Which keydir this node's state machines hold (M11.5). Every harness
    /// but `Cluster::of_three_on` uses the default, so the whole suite runs
    /// against the implementation production runs.
    keydir: kv_storage::IndexKind,
    snapshot_threshold: u64,
    /// Run the real `ShardSupervisor` and `MigrationDriver` instead of
    /// founding this node's shard groups in the harness (M12.1).
    ///
    /// The only mode in which a shard can arrive at, or leave, a running
    /// node — and therefore the only one M12.1's own tests can use. Every
    /// harness before it founds groups itself, which is precisely the code
    /// path M12.1 replaces.
    supervised: bool,
    /// The shards this node founds a Raft group for.
    ///
    /// Separate from `num_shards`, which is what the *map* describes. The
    /// harness founds groups itself rather than going through
    /// `shards::ShardSupervisor`, so a test can host one shard while the map
    /// still describes 256 — and so that every test written before M11 keeps
    /// exercising the driver and Raft rather than placement.
    shards: Vec<ShardId>,
}

impl Cluster {
    fn empty(num_shards: u16, replication_factor: u8) -> Cluster {
        Cluster {
            num_shards,
            replication_factor,
            placement: None,
            requests: BTreeMap::new(),
            admin: BTreeMap::new(),
            meta_requests: BTreeMap::new(),
            meta_admin: BTreeMap::new(),
            published: BTreeMap::new(),
            read_views: BTreeMap::new(),
            migrators: BTreeMap::new(),
            registry: Arc::new(Registry::default()),
            switchboard: Arc::new(Switchboard::default()),
            _keepalive: Vec::new(),
            _dirs: Vec::new(),
        }
    }

    /// `nodes` nodes, `num_shards` shards, `rf` replicas each — every node
    /// hosting exactly the shard groups the ring gives it (M11.9).
    ///
    /// The placement is computed here with the same `kv-ring` call the meta
    /// reconciler makes from the same inputs, so the map this cluster's own
    /// meta group bootstraps is the identical version 1 and the harness and
    /// the cluster cannot disagree about who holds what.
    pub(crate) fn sharded(nodes: &[NodeId], num_shards: u16, rf: u8) -> Cluster {
        let map =
            ShardMap::build(1, nodes.iter().copied(), num_shards, rf, kv_ring::DEFAULT_VNODES)
                .expect("placeable");
        let mut cluster = Cluster::empty(num_shards, rf);
        cluster.placement = Some(map.clone());
        for &id in nodes {
            cluster.start(Spec {
                id,
                peers: nodes.iter().copied().filter(|&p| p != id).collect(),
                joining: false,
                lease_reads: false,
                keydir: kv_storage::IndexKind::default(),
                snapshot_threshold: u64::MAX,
                shards: map.shards_of(id).collect(),
                supervised: false,
            });
        }
        cluster
    }

    /// The placement this harness founded its groups from.
    pub(crate) fn placement(&self) -> &ShardMap {
        self.placement.as_ref().expect("only a sharded cluster has a placement")
    }

    pub(crate) fn of_three() -> Cluster {
        Cluster::of_three_with(false)
    }

    /// `lease_reads` opts the whole cluster into §1.10's lease reads, which
    /// `tests::linearizability` uses to demonstrate what they cost.
    pub(crate) fn of_three_with(lease_reads: bool) -> Cluster {
        Cluster::with(lease_reads, u64::MAX, kv_storage::IndexKind::default())
    }

    /// A cluster whose state machines hold `keydir` (M11.5). For the tests
    /// that hold the `RwLock<HashMap>` comparison arm to the same behaviour
    /// as the default — it is a supported configuration, not a benchmark
    /// fixture.
    pub(crate) fn of_three_on(keydir: kv_storage::IndexKind) -> Cluster {
        Cluster::with(false, u64::MAX, keydir)
    }

    /// A cluster that snapshots every `threshold` applied entries (M8). The
    /// shared harnesses disable snapshots so timing-sensitive tests never see
    /// a scan pause; M8's own gate opts in here.
    pub(crate) fn with_snapshots(threshold: u64) -> Cluster {
        Cluster::with(false, threshold, kv_storage::IndexKind::default())
    }

    fn with(lease_reads: bool, snapshot_threshold: u64, keydir: kv_storage::IndexKind) -> Cluster {
        let mut cluster = Cluster::empty(256, 3);
        for &id in &ALL {
            cluster.start(Spec {
                id,
                peers: ALL.into_iter().filter(|&p| p != id).collect(),
                joining: false,
                lease_reads,
                keydir,
                snapshot_threshold,
                // One shard group, numbered as shard 0's. Every harness
                // request names it explicitly, so the map's 256 shards are
                // beside the point here: these tests drive the driver, not
                // the router.
                shards: vec![0],
                supervised: false,
            });
        }
        cluster
    }

    /// Starts a node that expects to be *admitted* rather than to found the
    /// cluster: learner from the first tick, never campaigning, until an
    /// `AddNode` brings it into the membership.
    ///
    /// `members` is what the operator would pass as `--peer`: the cluster as
    /// it stands. A joiner needs it for the same reason a founder does — the
    /// founding members' voter-hood came from argv and was never written to
    /// the log, so there is no conf entry to replay it from. What the log
    /// *does* carry is every change since, which is how this node learns
    /// about members admitted after it.
    pub(crate) fn start_joining_node(&mut self, id: NodeId, members: &[NodeId]) {
        self.start(Spec {
            id,
            peers: members.iter().copied().filter(|&p| p != id).collect(),
            joining: true,
            lease_reads: false,
            keydir: kv_storage::IndexKind::default(),
            snapshot_threshold: u64::MAX,
            // A joining node hosts the same one group, as a learner: it is
            // being admitted to that group, so it has to be able to receive
            // its AppendEntries. In production a shard reaching a new node is
            // a migration (M12); here the harness is the migration.
            shards: vec![0],
            supervised: false,
        });
    }

    /// `nodes` nodes running the real supervisor and migration reconciler
    /// (M12.1), founding their own shard groups from the map their own meta
    /// group bootstraps.
    ///
    /// Awaits the first placement before returning, so a test starts
    /// asserting rather than first waiting out an election.
    pub(crate) async fn supervised(nodes: &[NodeId], num_shards: u16, rf: u8) -> Cluster {
        let mut cluster = Cluster::empty(num_shards, rf);
        for &id in nodes {
            cluster.start(Spec {
                id,
                peers: nodes.iter().copied().filter(|&p| p != id).collect(),
                joining: false,
                lease_reads: false,
                keydir: kv_storage::IndexKind::default(),
                snapshot_threshold: u64::MAX,
                // None: the supervisor founds them, which is the point.
                shards: Vec::new(),
                supervised: true,
            });
        }
        cluster
            .await_shard_map_on(nodes[0], Duration::from_secs(10))
            .await
            .expect("a supervised cluster bootstraps a map");
        cluster
    }

    /// Starts a node that joins an existing supervised cluster: it founds
    /// nothing, and its shards arrive by migration.
    pub(crate) fn start_joining_supervised(&mut self, id: NodeId, members: &[NodeId]) {
        self.start(Spec {
            id,
            peers: members.iter().copied().filter(|&p| p != id).collect(),
            joining: true,
            lease_reads: false,
            keydir: kv_storage::IndexKind::default(),
            snapshot_threshold: u64::MAX,
            shards: Vec::new(),
            supervised: true,
        });
    }

    /// Runs node `id`'s migration pass now, as `AdminService::Rebalance`
    /// does, and answers with what is still moving there.
    pub(crate) async fn rebalance(&self, id: NodeId) -> usize {
        self.migrators[&id].pass().await
    }

    /// Whether `id` hosts exactly `shards`, within `within`.
    pub(crate) async fn await_hosted(
        &self,
        id: NodeId,
        shards: &[ShardId],
        within: Duration,
    ) -> bool {
        let want: BTreeSet<ShardId> = shards.iter().copied().collect();
        let deadline = std::time::Instant::now() + within;
        while std::time::Instant::now() < deadline {
            let have: BTreeSet<ShardId> =
                self.shard_statuses_of(id).await.into_iter().map(|s| s.shard).collect();
            if have == want {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    fn start(&mut self, spec: Spec) {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeConfig {
            id: spec.id,
            listen: "127.0.0.1:0".parse().unwrap(),
            peers: spec.peers.iter().map(|&p| (p, format!("in-process://{p}"))).collect(),
            data_dir: dir.path().to_path_buf(),
            // Short but not degenerate: an election must complete in a test's
            // patience, while still leaving a heartbeat comfortably inside the
            // election timeout.
            tick: Duration::from_millis(10),
            election_timeout: 10,
            heartbeat_interval: 2,
            lease_reads: spec.lease_reads,
            keydir: spec.keydir,
            snapshot_threshold: spec.snapshot_threshold,
            initial_learner: spec.joining,
            num_shards: self.num_shards,
            replication_factor: self.replication_factor,
            vnodes_per_node: kv_ring::DEFAULT_VNODES,
            max_migrations: 4,
            log_fsync: crate::config::LogFsync::default().into(),
            state_fsync: crate::config::LogFsync::default().into(),
        };

        // Two drivers, exactly as `main` runs them: the meta group alone in
        // one, every shard group in the other.
        let meta = self.start_driver(&spec, &config, &[group::META]);
        let shard_groups: Vec<GroupId> = spec.shards.iter().copied().map(group::shard).collect();
        let data = self.start_driver(&spec, &config, &shard_groups);

        // The reconciler, exactly as `main` spawns it, but on a test-sized
        // interval: the real one is slow on purpose (placement changes when an
        // operator adds a machine), and a test should not wait out half a
        // second per pass.
        let published = PublishedMap::default();
        tokio::spawn(
            MetaReconciler::new(
                config.clone(),
                meta.admin.clone(),
                meta.requests.clone(),
                Arc::clone(&published),
                Duration::from_millis(20),
            )
            .run(),
        );

        if spec.supervised {
            // The harness `Registry` routes by `(node, group)` and is what the
            // `TestLink`s resolve through; the production `GroupRegistry` is
            // what the supervisor writes. Register every shard group's inbox
            // here up front — the data driver has one inbox for all of them —
            // so a peer's message lands whether or not the supervisor has got
            // to that shard yet. A message for a group the driver does not
            // host is dropped, which is what the real transport's `not_found`
            // becomes on this side of the wire.
            for shard in 0..self.num_shards {
                self.registry.register(spec.id, group::shard(shard), data.inbox.clone());
            }
            tokio::spawn(
                crate::shards::ShardSupervisor::new(
                    config.clone(),
                    Arc::clone(&published),
                    // The supervisor's own routing table. The harness routes
                    // through `Registry` above instead, so nothing reads this
                    // one — it exists because the production supervisor
                    // registers and unregisters groups in it.
                    GroupRegistry::default(),
                    data.inbox.clone(),
                    data.admin.clone(),
                    data.new_groups.clone(),
                    Duration::from_millis(20),
                )
                .run(),
            );

            let migrator = Arc::new(crate::migrate::MigrationDriver::new(
                config.clone(),
                data.admin.clone(),
                meta.admin.clone(),
                Arc::clone(&published),
                Duration::from_millis(20),
            ));
            self.migrators.insert(spec.id, Arc::clone(&migrator));
            tokio::spawn(async move { migrator.run_loop().await });
        }

        self.requests.insert(spec.id, data.requests);
        self.admin.insert(spec.id, data.admin);
        self.meta_requests.insert(spec.id, meta.requests);
        self.meta_admin.insert(spec.id, meta.admin);
        self.published.insert(spec.id, published);
        self._dirs.push(dir);
    }

    /// Starts one driver hosting `groups` on one node.
    ///
    /// Returns its request and admin senders, plus the inbox and new-group
    /// senders a supervisor needs to hand it shards at runtime (M12.1). The
    /// reply sender is parked in `_keepalive`, because dropping it closes a
    /// `select!` arm under the driver; so is a clone of each of the others,
    /// for the same reason.
    fn start_driver(
        &mut self,
        spec: &Spec,
        config: &NodeConfig,
        groups: &[GroupId],
    ) -> DriverHandles {
        let (inbox_tx, inbox) = mpsc::channel(1024);
        let (replies_tx, replies) = mpsc::channel(1024);
        let (req_tx, req_rx) = mpsc::channel(64);
        let (admin_tx, admin_rx) = mpsc::channel(16);
        let (groups_tx, new_groups) = mpsc::channel(512);

        let factory = TestPeers {
            me: spec.id,
            registry: Arc::clone(&self.registry),
            my_replies: replies_tx.clone(),
            switchboard: Arc::clone(&self.switchboard),
        };
        let peers: BTreeMap<NodeId, Box<dyn PeerLink>> = spec
            .peers
            .iter()
            .map(|&p| (p, factory.connect(p, &format!("in-process://{p}"))))
            .collect();

        let mut hosted = Vec::new();
        for &group in groups {
            let (raft_dir, state_dir) = match group::shard_of(group) {
                Some(shard) => (config.shard_raft_dir(shard), config.shard_state_dir(shard)),
                None => (config.meta_raft_dir(), config.meta_state_dir()),
            };
            std::fs::create_dir_all(&raft_dir).unwrap();
            std::fs::create_dir_all(&state_dir).unwrap();

            let mut raft = config.raft_config_for(group);
            // A shard group's members are that shard's replica set, not the
            // whole cluster — the same thing `shards::open` does when the
            // supervisor founds one for real.
            if let (Some(shard), Some(map)) = (group::shard_of(group), self.placement.as_ref()) {
                raft.peers =
                    map.replicas(shard).iter().copied().filter(|&p| p != spec.id).collect();
            }
            let node = RaftNode::new(raft, BitcaskStorage::open(&raft_dir).unwrap());
            let engine = Engine::open_with_config(&state_dir, config.engine_config()).unwrap();
            if group::shard_of(group) == Some(0) {
                self.read_views.insert(spec.id, engine.read_view_factory());
            }
            // Registered before the driver runs: a peer that dials this node
            // during its first tick must find an inbox, not a gap.
            self.registry.register(spec.id, group, inbox_tx.clone());
            hosted.push(Group::new(config, group, node, engine));
        }

        let driver = Driver::new(
            config,
            hosted,
            peers,
            Box::new(factory),
            GroupChannels {
                inbox,
                peer_replies: replies,
                requests: req_rx,
                admin: admin_rx,
                new_groups,
            },
        );
        tokio::spawn(driver.run());

        self._keepalive.push(Box::new((inbox_tx.clone(), replies_tx, groups_tx.clone())));
        DriverHandles { requests: req_tx, admin: admin_tx, inbox: inbox_tx, new_groups: groups_tx }
    }

    pub(crate) fn switchboard(&self) -> &Arc<Switchboard> {
        &self.switchboard
    }

    /// Parks a reader inside node `id`'s published keydir copy until the
    /// returned hold is dropped, which stops the driver's `publish` from
    /// swapping the copies.
    ///
    /// The engine refuses to block a writer on a reader, so while this is
    /// held the node goes on applying and acknowledging writes that no reader
    /// can see. That is left-right's documented cost, and it is the only way
    /// to make the window between *applied* and *visible* a fact rather than
    /// a race a test passes by luck.
    pub(crate) fn hold_read_copy(&self, id: NodeId) -> ReadHold {
        self.read_views[&id].hold_read_copy()
    }

    /// A view of node `id`'s state machine as a reader sees it — published
    /// state only, with no ReadIndex round trip. For asserting on what is
    /// visible, never for serving a linearizable read.
    pub(crate) fn read_view(&self, id: NodeId) -> kv_storage::ReadView {
        self.read_views[&id].view()
    }

    /// A cloneable handle to the same nodes, for a test that drives client
    /// traffic from one task while it administers the cluster from another.
    pub(crate) fn client(&self) -> ClusterClient {
        ClusterClient { requests: self.requests.clone() }
    }

    pub(crate) async fn call(&self, id: NodeId, op: ClientOp) -> ClientReply {
        self.client().call(id, op).await
    }

    pub(crate) async fn try_call(
        &self,
        id: NodeId,
        op: ClientOp,
        within: Duration,
    ) -> Option<ClientReply> {
        self.client().try_call(id, op, within).await
    }

    pub(crate) async fn get_from(&self, id: NodeId, key: &[u8]) -> ClientReply {
        self.client().get_from(id, key).await
    }

    pub(crate) async fn put_among(&self, among: &[NodeId], key: &[u8], value: &[u8]) -> NodeId {
        self.client().put_among(among, key, value).await
    }

    pub(crate) async fn put(&self, key: &[u8], value: &[u8]) -> NodeId {
        self.client().put_among(&ALL, key, value).await
    }

    pub(crate) async fn read_among(&self, among: &[NodeId], key: &[u8]) -> Option<Vec<u8>> {
        self.client().read_among(among, key).await
    }

    pub(crate) async fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.client().read_among(&ALL, key).await
    }

    pub(crate) async fn leader_of(&self, among: &[NodeId]) -> Option<NodeId> {
        self.client().leader_of(among).await
    }

    /// The map node `id` has published, once it has published one.
    pub(crate) async fn await_shard_map_on(
        &self,
        id: NodeId,
        within: Duration,
    ) -> Option<Arc<ShardMap>> {
        let deadline = std::time::Instant::now() + within;
        while std::time::Instant::now() < deadline {
            if let Some(map) = self.published[&id].load_full() {
                return Some(map);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }

    /// The map as any node sees it. For assertions about the map's content
    /// rather than about agreement; `await_shard_map_on` is the per-node form.
    pub(crate) async fn await_shard_map(&self, within: Duration) -> Option<Arc<ShardMap>> {
        self.await_shard_map_on(ALL[0], within).await
    }

    /// One client request to a node's **meta** group.
    pub(crate) async fn meta_call(&self, id: NodeId, op: ClientOp) -> ClientReply {
        let (reply, wait) = oneshot::channel();
        self.meta_requests[&id]
            .send(ClientRequest { group: group::META, op, reply })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("the meta driver answers")
            .expect("the meta driver does not drop it")
    }

    /// A meta-group client request that may go unanswered — the meta
    /// equivalent of `try_call`, for asking a partitioned node something it
    /// cannot honestly answer.
    pub(crate) async fn try_meta_call(
        &self,
        id: NodeId,
        op: ClientOp,
        within: Duration,
    ) -> Option<ClientReply> {
        let (reply, wait) = oneshot::channel();
        self.meta_requests[&id].send(ClientRequest { group: group::META, op, reply }).await.ok()?;
        tokio::time::timeout(within, wait).await.ok()?.ok()
    }

    /// One admin request to a node's meta group.
    pub(crate) async fn meta_admin(&self, id: NodeId, op: AdminOp) -> AdminReply {
        let (reply, wait) = oneshot::channel();
        self.meta_admin[&id].send(AdminRequest { group: group::META, op, reply }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("the meta driver answers an admin request")
            .expect("the meta driver does not drop it")
    }

    pub(crate) async fn meta_status_of(&self, id: NodeId) -> ClusterStatus {
        match self.meta_admin(id, AdminOp::Status).await {
            AdminReply::Status(status) => status,
            other => panic!("node {id}'s meta group answered a status request with {other:?}"),
        }
    }

    /// Whichever of `among` leads the meta group, once one does.
    pub(crate) async fn meta_leader_of(&self, among: &[NodeId]) -> Option<NodeId> {
        for _ in 0..200 {
            for &id in among {
                let status = self.meta_status_of(id).await;
                if let Some(leader) = status.leader
                    && among.contains(&leader)
                {
                    return Some(leader);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }

    /// One admin request to one node's **shard** driver, naming the group
    /// (M12.1). `admin` below names `group::DATA`, which is shard 0's — fine
    /// while a harness hosts one shard and wrong the moment it hosts four.
    pub(crate) async fn shard_admin(&self, id: NodeId, shard: ShardId, op: AdminOp) -> AdminReply {
        let (reply, wait) = oneshot::channel();
        self.admin[&id].send(AdminRequest { group: group::shard(shard), op, reply }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("the shard driver answers an admin request")
            .expect("the shard driver does not drop it")
    }

    /// A membership change against whichever of `among` leads `shard`.
    pub(crate) async fn administer_shard(
        &self,
        shard: ShardId,
        among: &[NodeId],
        op: impl Fn() -> AdminOp,
    ) -> AdminReply {
        for _ in 0..200 {
            for &id in among {
                match self.shard_admin(id, shard, op()).await {
                    AdminReply::NotLeader { .. } => {}
                    answered => return answered,
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no node among {among:?} accepted a change to shard {shard} in 4s");
    }

    /// Whichever of `among` leads `shard`, once one does.
    pub(crate) async fn leader_of_shard(&self, shard: ShardId, among: &[NodeId]) -> Option<NodeId> {
        for _ in 0..200 {
            for &id in among {
                if let Some(status) =
                    self.shard_statuses_of(id).await.into_iter().find(|s| s.shard == shard)
                    && status.leading
                {
                    return Some(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }

    /// One admin request to one node, no retry.
    pub(crate) async fn admin(&self, id: NodeId, op: AdminOp) -> AdminReply {
        let (reply, wait) = oneshot::channel();
        self.admin[&id].send(AdminRequest { group: group::DATA, op, reply }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("the driver answers an admin request")
            .expect("the driver does not drop it")
    }

    pub(crate) async fn status_of(&self, id: NodeId) -> ClusterStatus {
        match self.admin(id, AdminOp::Status).await {
            AdminReply::Status(status) => status,
            other => panic!("node {id} answered a status request with {other:?}"),
        }
    }

    /// Runs a membership change against whichever of `among` leads, retrying
    /// through `NotLeader` exactly as an admin tool does.
    ///
    /// A `Rejected` is returned rather than retried: it means the change is
    /// wrong wherever it is sent.
    pub(crate) async fn administer(
        &self,
        among: &[NodeId],
        op: impl Fn() -> AdminOp,
    ) -> AdminReply {
        for _ in 0..200 {
            for &id in among {
                match self.admin(id, op()).await {
                    AdminReply::NotLeader { .. } => {}
                    answered => return answered,
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no node among {among:?} accepted a membership change in 4s");
    }

    /// Runs a membership change against whichever of `among` leads the
    /// **meta** group.
    ///
    /// Since M11 this is what "the cluster's membership" means: there is no
    /// single data group any more, so the small fixed group that records
    /// placement records who is in the cluster too, and the ring is built from
    /// its members.
    pub(crate) async fn administer_meta(
        &self,
        among: &[NodeId],
        op: impl Fn() -> AdminOp,
    ) -> AdminReply {
        for _ in 0..200 {
            for &id in among {
                match self.meta_admin(id, op()).await {
                    AdminReply::NotLeader { .. } => {}
                    answered => return answered,
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no node among {among:?} accepted a meta membership change in 4s");
    }

    /// Waits until `id` reports exactly `voters`. Returns false on timeout, so
    /// the caller can assert with its own message.
    pub(crate) async fn await_voters(
        &self,
        id: NodeId,
        voters: &[NodeId],
        within: Duration,
    ) -> bool {
        let deadline = std::time::Instant::now() + within;
        let want: Vec<NodeId> = voters.to_vec();
        while std::time::Instant::now() < deadline {
            if self.status_of(id).await.voters == want {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// Every shard group `id` hosts, as that node sees them (M11.6).
    pub(crate) async fn shard_statuses_of(&self, id: NodeId) -> Vec<ShardStatus> {
        let (reply, wait) = oneshot::channel();
        self.admin[&id]
            .send(AdminRequest { group: group::UNSET, op: AdminOp::ShardStatuses, reply })
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("the shard driver answers")
            .expect("the shard driver does not drop it")
        {
            AdminReply::ShardStatuses(statuses) => statuses,
            other => panic!("node {id} answered a shard-status request with {other:?}"),
        }
    }

    /// Who leads each shard, once every shard has a leader among the nodes in
    /// `among`.
    ///
    /// Asked of each shard's own replicas, because a node that does not host a
    /// shard has no view of it at all — which is the point of sharding.
    pub(crate) async fn await_shard_leaders(
        &self,
        among: &[NodeId],
        within: Duration,
    ) -> BTreeMap<ShardId, NodeId> {
        let deadline = std::time::Instant::now() + within;
        let map = self.placement().clone();
        loop {
            let mut leaders = BTreeMap::new();
            for &id in among {
                for status in self.shard_statuses_of(id).await {
                    if status.leading {
                        leaders.insert(status.shard, id);
                    }
                }
            }
            let complete = (0..map.num_shards)
                .filter(|&shard| map.replicas(shard).iter().any(|r| among.contains(r)))
                .all(|shard| leaders.contains_key(&shard));
            if complete {
                return leaders;
            }
            if std::time::Instant::now() >= deadline {
                panic!("not every shard elected a leader among {among:?}; got {leaders:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Writes `key` to its shard, trying that shard's replicas in turn.
    ///
    /// Returns the node that accepted it, or `None` if none did inside
    /// `within` — which is what "this shard is degraded" looks like from
    /// outside.
    pub(crate) async fn write_to_shard_with(
        &self,
        map: &ShardMap,
        key: &[u8],
        value: &[u8],
        among: &[NodeId],
        within: Duration,
    ) -> Option<NodeId> {
        self.client().write_to_shard_with(map, key, value, among, within).await
    }

    /// Real time passes here, so a wait is a real wait.
    pub(crate) async fn settle(&self, how_long: Duration) {
        tokio::time::sleep(how_long).await;
    }
}

/// The client half of the harness, detached from the nodes so it can be moved
/// into a task of its own.
#[derive(Clone)]
pub(crate) struct ClusterClient {
    requests: BTreeMap<NodeId, mpsc::Sender<ClientRequest>>,
}

impl ClusterClient {
    /// Writes `key` to its shard, trying that shard's replicas in turn.
    ///
    /// Takes the map explicitly rather than reading the cluster's, so the
    /// whole call is `Send` and one shard's write can run in its own task
    /// beside every other shard's — which is how the gate shows that they
    /// proceed concurrently rather than queueing behind one leader.
    pub(crate) async fn write_to_shard_with(
        &self,
        map: &ShardMap,
        key: &[u8],
        value: &[u8],
        among: &[NodeId],
        within: Duration,
    ) -> Option<NodeId> {
        let shard = map.shard_for_key(key);
        let group = group::shard(shard);
        let deadline = std::time::Instant::now() + within;
        loop {
            for &id in map.replicas(shard) {
                if !among.contains(&id) {
                    continue;
                }
                let (reply, wait) = oneshot::channel();
                let op = ClientOp::Mutate {
                    ctx: None,
                    op: crate::command::Mutation::Put { key: key.to_vec(), value: value.to_vec() },
                };
                if self.requests[&id].send(ClientRequest { group, op, reply }).await.is_err() {
                    continue;
                }
                if let Ok(Ok(ClientReply::Applied)) =
                    tokio::time::timeout(Duration::from_millis(500), wait).await
                {
                    return Some(id);
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// One op against `shard`'s group, trying `among` in turn until one
    /// answers properly (M12.1).
    ///
    /// Unlike `write_to_shard_with` this takes no map: key→shard and
    /// shard→group never change, so a request routed this way survives
    /// placement moving underneath it — which is the entire point during a
    /// migration. `NotLeader` and `NotHosted` are both re-tries against the
    /// next node, exactly as `kv-client` treats them.
    pub(crate) async fn on_shard(
        &self,
        shard: ShardId,
        op: impl Fn() -> ClientOp,
        among: &[NodeId],
        within: Duration,
    ) -> Option<ClientReply> {
        let group = group::shard(shard);
        let deadline = std::time::Instant::now() + within;
        loop {
            for &id in among {
                let (reply, wait) = oneshot::channel();
                if self.requests[&id].send(ClientRequest { group, op: op(), reply }).await.is_err()
                {
                    continue;
                }
                match tokio::time::timeout(Duration::from_millis(500), wait).await {
                    Ok(Ok(ClientReply::NotLeader { .. } | ClientReply::NotHosted { .. })) => {}
                    Ok(Ok(answered)) => return Some(answered),
                    _ => {}
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// One request to one node, no retry.
    pub(crate) async fn call(&self, id: NodeId, op: ClientOp) -> ClientReply {
        let (reply, wait) = oneshot::channel();
        self.requests[&id].send(ClientRequest { group: group::DATA, op, reply }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("the driver answers")
            .expect("the driver does not drop the request")
    }

    /// One request, with a bound. `None` means the driver never answered —
    /// which is what a partitioned leader does to a write: it still believes
    /// it leads, so `propose` succeeds, and the entry then waits forever for a
    /// quorum that cannot come. "No answer" is emphatically not "committed".
    pub(crate) async fn try_call(
        &self,
        id: NodeId,
        op: ClientOp,
        within: Duration,
    ) -> Option<ClientReply> {
        let (reply, wait) = oneshot::channel();
        self.requests[&id].send(ClientRequest { group: group::DATA, op, reply }).await.unwrap();
        tokio::time::timeout(within, wait).await.ok().map(|r| r.expect("not dropped"))
    }

    pub(crate) async fn get_from(&self, id: NodeId, key: &[u8]) -> ClientReply {
        self.call(id, ClientOp::get(key)).await
    }

    /// Writes via whichever of `among` accepts, retrying through `NotLeader`.
    /// Returns the node that took it.
    pub(crate) async fn put_among(&self, among: &[NodeId], key: &[u8], value: &[u8]) -> NodeId {
        for _ in 0..200 {
            for &id in among {
                let op = ClientOp::put(key, value);
                if let ClientReply::Applied = self.call(id, op).await {
                    return id;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no node among {among:?} accepted a write in 4s");
    }

    /// Reads via whichever of `among` can serve it, retrying through
    /// `NotLeader`. Since M7 that is the leader alone: a linearizable read
    /// needs a leadership quorum, so a follower refuses rather than answering
    /// from local state.
    pub(crate) async fn read_among(&self, among: &[NodeId], key: &[u8]) -> Option<Vec<u8>> {
        for _ in 0..200 {
            for &id in among {
                if let ClientReply::Value(value) = self.get_from(id, key).await {
                    return value;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no node among {among:?} served a read in 4s");
    }

    /// Which of `among` currently leads, found by asking each to serve a read:
    /// since M7 only a leader that can confirm a quorum will.
    pub(crate) async fn leader_of(&self, among: &[NodeId]) -> Option<NodeId> {
        for _ in 0..200 {
            for &id in among {
                if let ClientReply::Value(_) = self.get_from(id, b"\x01probe").await {
                    return Some(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }
}

#[cfg(test)]
mod smoke {
    use super::*;

    /// The harness must work before it is trusted to report a failure.
    #[tokio::test]
    async fn a_three_node_cluster_elects_a_leader_and_replicates() {
        let cluster = Cluster::of_three();
        let leader = cluster.put(b"k", b"v").await;
        assert!(ALL.contains(&leader));

        cluster.settle(Duration::from_millis(500)).await;
        assert_eq!(cluster.read(b"k").await, Some(b"v".to_vec()));
    }

    /// M6 let every node answer a `Get` from local state, and
    /// `tests::linearizability` showed what that permits. Since M7 a read
    /// takes a leadership quorum, so a follower refuses and points the client
    /// at the leader instead. This is a deliberate loss of the M6 property
    /// "get k on any node returns it" — follower reads via forwarding are a
    /// §1.10 stretch goal, not something M7 claims.
    #[tokio::test]
    async fn a_follower_refuses_a_read_and_names_the_leader() {
        let cluster = Cluster::of_three();
        let leader = cluster.put(b"k", b"v").await;
        cluster.settle(Duration::from_millis(300)).await;

        for &id in ALL.iter().filter(|&&id| id != leader) {
            match cluster.get_from(id, b"k").await {
                ClientReply::NotLeader { hint } => {
                    assert_eq!(hint, Some(leader), "node {id} should name the leader");
                }
                other => panic!("follower {id} served a read: {other:?}"),
            }
        }
    }

    /// If this does not pass, the switchboard is not actually blocking and
    /// every partition result after it is worthless. Check this first.
    #[tokio::test]
    async fn isolating_the_leader_lets_the_survivors_elect_a_new_one() {
        let cluster = Cluster::of_three();
        let old = cluster.put(b"k", b"v1").await;
        let survivors: Vec<NodeId> = ALL.into_iter().filter(|n| *n != old).collect();

        cluster.switchboard().isolate(&[old], &ALL);
        cluster.settle(Duration::from_secs(1)).await;

        let new = cluster.put_among(&survivors, b"k", b"v2").await;
        assert_ne!(new, old, "the isolated node must not be the one accepting writes");
        assert!(survivors.contains(&new));
    }

    /// The other half of a real partition: the isolated node cannot commit,
    /// because it cannot reach a quorum. Without this, "isolate" might only be
    /// dropping one direction and the harness would still look correct.
    #[tokio::test]
    async fn an_isolated_minority_cannot_commit() {
        let cluster = Cluster::of_three();
        let old = cluster.put(b"k", b"v1").await;

        cluster.switchboard().isolate(&[old], &ALL);
        cluster.settle(Duration::from_secs(1)).await;

        // Alone on its side of the partition it can never reach a quorum, so a
        // write must not report success. It will not report anything at all:
        // the node still believes it leads, so `propose` is accepted and the
        // entry waits forever. `try_call` returning `None` is that, and it is
        // the correct outcome here — what must never happen is `Applied`.
        for _ in 0..5 {
            let op = ClientOp::put(b"k", b"never");
            if let Some(ClientReply::Applied) =
                cluster.try_call(old, op, Duration::from_millis(200)).await
            {
                panic!("an isolated node committed a write without a quorum");
            }
        }
    }
}
