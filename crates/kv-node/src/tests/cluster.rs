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
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::config::NodeConfig;
use crate::driver::{ClientOp, ClientReply, ClientRequest, Driver};
use crate::storage::BitcaskStorage;
use crate::transport::PeerLink;
use crate::transport::peer::SendError;
use crate::transport::server::Inbound;

pub(crate) const ALL: [NodeId; 3] = [1, 2, 3];

/// Who can talk to whom. A partition blocks both directions, because a real
/// one does.
#[derive(Default)]
pub(crate) struct Switchboard {
    blocked: Mutex<BTreeSet<(NodeId, NodeId)>>,
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

    #[allow(dead_code)]
    pub(crate) fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }
}

/// One node's view of one peer. Mirrors `PeerClient`: send a request, route
/// the reply back into the sender's own `peer_replies`.
struct TestLink {
    me: NodeId,
    peer: NodeId,
    peer_inbox: mpsc::Sender<Inbound>,
    my_replies: mpsc::Sender<(NodeId, Message)>,
    switchboard: Arc<Switchboard>,
}

impl PeerLink for TestLink {
    fn try_send(&self, msg: Message) -> Result<(), SendError> {
        // Outbound drop: the request never reaches the peer.
        if !self.switchboard.allows(self.me, self.peer) {
            return Ok(());
        }
        let (reply, wait) = oneshot::channel();
        self.peer_inbox
            .try_send(Inbound { from: self.me, msg, reply })
            .map_err(|_| SendError::Full)?;

        let (me, peer) = (self.me, self.peer);
        let replies = self.my_replies.clone();
        let switchboard = Arc::clone(&self.switchboard);
        tokio::spawn(async move {
            if let Ok(answer) = wait.await {
                // Inbound drop: the reply is lost on the way back, which a
                // partition does just as readily as losing the request.
                if switchboard.allows(peer, me) {
                    let _ = replies.try_send((peer, answer));
                }
            }
        });
        Ok(())
    }
}

pub(crate) struct Cluster {
    requests: BTreeMap<NodeId, mpsc::Sender<ClientRequest>>,
    switchboard: Arc<Switchboard>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Cluster {
    pub(crate) fn of_three() -> Cluster {
        let switchboard = Arc::new(Switchboard::default());
        let mut dirs = Vec::new();
        let mut configs = BTreeMap::new();

        for &id in &ALL {
            let dir = tempfile::tempdir().unwrap();
            let config = NodeConfig {
                id,
                listen: "127.0.0.1:0".parse().unwrap(),
                peers: ALL
                    .iter()
                    .filter(|&&p| p != id)
                    .map(|&p| (p, format!("in-process://{p}")))
                    .collect(),
                data_dir: dir.path().to_path_buf(),
                // Short but not degenerate: an election must complete in a
                // test's patience, while still leaving a heartbeat comfortably
                // inside the election timeout.
                tick: Duration::from_millis(10),
                election_timeout: 10,
                heartbeat_interval: 2,
            };
            std::fs::create_dir_all(config.raft_dir()).unwrap();
            std::fs::create_dir_all(config.state_dir()).unwrap();
            configs.insert(id, config);
            dirs.push(dir);
        }

        // Inboxes first: a link needs its peer's inbox, so every inbox has to
        // exist before any driver is built.
        let mut inbox_tx = BTreeMap::new();
        let mut inbox_rx = BTreeMap::new();
        let mut replies_tx = BTreeMap::new();
        let mut replies_rx = BTreeMap::new();
        for &id in &ALL {
            let (tx, rx) = mpsc::channel(256);
            inbox_tx.insert(id, tx);
            inbox_rx.insert(id, rx);
            let (tx, rx) = mpsc::channel(256);
            replies_tx.insert(id, tx);
            replies_rx.insert(id, rx);
        }

        let mut requests = BTreeMap::new();
        for &id in &ALL {
            let config = &configs[&id];
            let node = RaftNode::new(
                config.raft_config(),
                BitcaskStorage::open(config.raft_dir()).unwrap(),
            );
            let engine = Engine::open(config.state_dir()).unwrap();

            let peers: BTreeMap<NodeId, Box<dyn PeerLink>> = ALL
                .iter()
                .filter(|&&p| p != id)
                .map(|&p| {
                    let link = TestLink {
                        me: id,
                        peer: p,
                        peer_inbox: inbox_tx[&p].clone(),
                        my_replies: replies_tx[&id].clone(),
                        switchboard: Arc::clone(&switchboard),
                    };
                    (p, Box::new(link) as Box<dyn PeerLink>)
                })
                .collect();

            let (req_tx, req_rx) = mpsc::channel(64);
            let driver = Driver::new(
                config,
                node,
                engine,
                peers,
                inbox_rx.remove(&id).unwrap(),
                replies_rx.remove(&id).unwrap(),
                req_rx,
            );
            tokio::spawn(driver.run());
            requests.insert(id, req_tx);
        }

        Cluster { requests, switchboard, _dirs: dirs }
    }

    pub(crate) fn switchboard(&self) -> &Arc<Switchboard> {
        &self.switchboard
    }

    /// One request to one node, no retry.
    pub(crate) async fn call(&self, id: NodeId, op: ClientOp) -> ClientReply {
        let (reply, wait) = oneshot::channel();
        self.requests[&id].send(ClientRequest { op, reply }).await.unwrap();
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
        self.requests[&id].send(ClientRequest { op, reply }).await.unwrap();
        tokio::time::timeout(within, wait).await.ok().map(|r| r.expect("not dropped"))
    }

    pub(crate) async fn get_from(&self, id: NodeId, key: &[u8]) -> ClientReply {
        self.call(id, ClientOp::Get { key: key.to_vec() }).await
    }

    /// Writes via whichever of `among` accepts, retrying through `NotLeader`.
    /// Returns the node that took it.
    pub(crate) async fn put_among(&self, among: &[NodeId], key: &[u8], value: &[u8]) -> NodeId {
        for _ in 0..200 {
            for &id in among {
                let op = ClientOp::Put { key: key.to_vec(), value: value.to_vec() };
                if let ClientReply::Applied = self.call(id, op).await {
                    return id;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no node among {among:?} accepted a write in 4s");
    }

    pub(crate) async fn put(&self, key: &[u8], value: &[u8]) -> NodeId {
        self.put_among(&ALL, key, value).await
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

    pub(crate) async fn read(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.read_among(&ALL, key).await
    }

    /// Real time passes here, so a wait is a real wait.
    pub(crate) async fn settle(&self, how_long: Duration) {
        tokio::time::sleep(how_long).await;
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
            let op = ClientOp::Put { key: b"k".to_vec(), value: b"never".to_vec() };
            if let Some(ClientReply::Applied) =
                cluster.try_call(old, op, Duration::from_millis(200)).await
            {
                panic!("an isolated node committed a write without a quorum");
            }
        }
    }
}
