//! The driver, exercised as a group of one: no sockets, no peers, and the
//! whole tick/persist/send/apply cycle still runs. kv-raft's single-node
//! commit fix is what makes a lone node commit, so this is a real end-to-end
//! path rather than a stub.

use std::collections::BTreeMap;
use std::time::Duration;

use kv_raft::RaftNode;
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::config::NodeConfig;
use crate::driver::{ClientOp, ClientReply, ClientRequest, Driver, Group, GroupChannels};
use crate::storage::BitcaskStorage;
use crate::tests::support::SilentPeers;

fn config(dir: &std::path::Path, peers: BTreeMap<u64, String>) -> NodeConfig {
    let config = NodeConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().unwrap(),
        peers,
        data_dir: dir.to_path_buf(),
        tick: Duration::from_millis(5),
        election_timeout: 4,
        heartbeat_interval: 1,
        lease_reads: false,
        keydir: Default::default(),
        snapshot_threshold: 10_000,
        initial_learner: false,
        num_shards: 256,
        replication_factor: 3,
        vnodes_per_node: kv_ring::DEFAULT_VNODES,
    };
    std::fs::create_dir_all(config.shard_raft_dir(0)).unwrap();
    std::fs::create_dir_all(config.shard_state_dir(0)).unwrap();
    config
}

/// Starts a driver and returns the request channel. The inbox and peer-reply
/// senders are returned too: dropping them would close those `select!` arms
/// and shut the loop down mid-test.
fn spawn(config: &NodeConfig) -> (mpsc::Sender<ClientRequest>, Box<dyn std::any::Any + Send>) {
    let node = RaftNode::new(
        config.raft_config(),
        BitcaskStorage::open(config.shard_raft_dir(0)).unwrap(),
    );
    let engine = Engine::open(config.shard_state_dir(0)).unwrap();

    let (inbox_tx, inbox) = mpsc::channel(8);
    let (replies_tx, replies) = mpsc::channel(8);
    let (requests, requests_rx) = mpsc::channel(8);
    let (admin_tx, admin) = mpsc::channel(8);

    let (groups_tx, new_groups) = mpsc::channel(8);
    let driver = Driver::new(
        config,
        vec![Group::new(config, crate::transport::group::DATA, node, engine)],
        BTreeMap::new(),
        Box::new(SilentPeers),
        GroupChannels { inbox, peer_replies: replies, requests: requests_rx, admin, new_groups },
    );
    tokio::spawn(driver.run());
    (requests, Box::new((inbox_tx, replies_tx, admin_tx, groups_tx)))
}

/// One request, one answer, no retry. Used where the refusal *is* the
/// assertion.
async fn call(tx: &mpsc::Sender<ClientRequest>, op: ClientOp) -> ClientReply {
    let (reply, wait) = oneshot::channel();
    tx.send(ClientRequest { group: crate::transport::group::DATA, op, reply }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the driver answers")
        .expect("the driver does not drop the request")
}

/// Retries through `NotLeader`, which is what a real client does (kv-client
/// does exactly this). A request issued before the lone node's election
/// timeout has elapsed is legitimately refused — that is the driver working,
/// not failing, so the test must not treat the first refusal as the answer.
///
/// Since M7 this applies to reads too: a `Get` goes through ReadIndex, so only
/// a leader that can confirm a quorum answers one.
async fn retrying(tx: &mpsc::Sender<ClientRequest>, op: impl Fn() -> ClientOp) -> ClientReply {
    for _ in 0..200 {
        match call(tx, op()).await {
            ClientReply::NotLeader { .. } => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            answered => return answered,
        }
    }
    panic!("no leader emerged in 2s");
}

async fn read(tx: &mpsc::Sender<ClientRequest>, key: &[u8]) -> ClientReply {
    retrying(tx, || ClientOp::get(key)).await
}

#[tokio::test]
async fn a_put_is_committed_then_readable() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let reply = retrying(&tx, || ClientOp::put(b"k", b"v")).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");

    let reply = read(&tx, b"k").await;
    assert!(matches!(reply, ClientReply::Value(Some(ref v)) if v == b"v"), "got {reply:?}");
}

#[tokio::test]
async fn a_delete_removes_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    retrying(&tx, || ClientOp::put(b"k", b"v")).await;
    let reply = retrying(&tx, || ClientOp::delete(b"k")).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");

    let reply = read(&tx, b"k").await;
    assert!(matches!(reply, ClientReply::Value(None)), "got {reply:?}");
}

#[tokio::test]
async fn a_missing_key_reads_as_none() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let reply = read(&tx, b"absent").await;
    assert!(matches!(reply, ClientReply::Value(None)), "got {reply:?}");
}

/// Everything the client was told was `Applied` must survive losing the
/// process, because that is the only promise a write ever made.
#[tokio::test]
async fn applied_writes_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), BTreeMap::new());

    let (first, keep) = spawn(&config);
    let reply = retrying(&first, || ClientOp::put(b"k", b"v")).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");
    drop((first, keep));
    // Let the old driver observe its closed channels and release the Bitcask
    // directories before the replacement opens them.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (second, _keep) = spawn(&config);
    let reply = read(&second, b"k").await;
    assert!(matches!(reply, ClientReply::Value(Some(ref v)) if v == b"v"), "got {reply:?}");
}

/// Snapshots are taken from live state once the log past them is long enough:
/// the prefix is dropped from the Raft log and reads keep serving from the
/// same state.
#[tokio::test]
async fn a_snapshot_is_taken_and_the_log_prefix_is_dropped() {
    use kv_raft::storage::RaftStorage;

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), BTreeMap::new());
    cfg.snapshot_threshold = 3;
    let (tx, _keep) = spawn(&cfg);

    for i in 0..4 {
        let reply = retrying(&tx, || ClientOp::put(format!("k{i}").as_bytes(), b"v")).await;
        assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");
    }
    // No-op at 1 plus four puts: applied reaches 5, past the threshold.
    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let s = BitcaskStorage::open(cfg.shard_raft_dir(0)).unwrap();
    let snap = s.snapshot().unwrap().expect("a snapshot must have been taken");
    assert!(snap.last_included_index >= 3, "got {}", snap.last_included_index);
    assert!(s.first_index().unwrap() > 1, "the prefix must be gone");
}

/// Restart-from-snapshot equals restart-from-full-log: everything the client
/// was told was `Applied` reads back, and the node keeps committing on top.
#[tokio::test]
async fn restart_from_snapshot_recovers_all_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), BTreeMap::new());
    cfg.snapshot_threshold = 2;

    let (first, keep) = spawn(&cfg);
    for i in 0..6 {
        let reply = retrying(&first, || {
            ClientOp::put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
        })
        .await;
        assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");
    }
    drop((first, keep));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (second, _keep) = spawn(&cfg);
    for i in 0..6 {
        let reply = read(&second, format!("k{i}").as_bytes()).await;
        assert!(
            matches!(reply, ClientReply::Value(Some(ref v)) if v == format!("v{i}").as_bytes()),
            "k{i} lost across a snapshot restart, got {reply:?}"
        );
    }
    // And the restarted node still commits: its log continues past the snapshot.
    let reply = retrying(&second, || ClientOp::put(b"after", b"restart")).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");
    let reply = read(&second, b"after").await;
    assert!(matches!(reply, ClientReply::Value(Some(ref v)) if v == b"restart"), "got {reply:?}");
}

/// A node that does not lead must refuse the write and name who does, rather
/// than apply it locally. Two configured peers that never answer means this
/// node can never win an election.
#[tokio::test]
async fn a_follower_refuses_a_write_instead_of_applying_it() {
    let dir = tempfile::tempdir().unwrap();
    let peers = [(2, "http://127.0.0.1:1".to_string()), (3, "http://127.0.0.1:2".to_string())]
        .into_iter()
        .collect();
    let (tx, _keep) = spawn(&config(dir.path(), peers));

    let reply = call(&tx, ClientOp::put(b"k", b"v")).await;
    assert!(matches!(reply, ClientReply::NotLeader { .. }), "got {reply:?}");

    // Since M7 it cannot serve the read either: a linearizable read needs a
    // leadership quorum, and this node has no peers that answer. Before M7 it
    // would have answered from local state, which is exactly the behaviour
    // `tests::linearizability` showed to be unsafe.
    let reply = call(&tx, ClientOp::get(b"k")).await;
    assert!(
        matches!(reply, ClientReply::NotLeader { .. }),
        "a node that cannot reach a quorum must not serve a read: {reply:?}"
    );
}

#[tokio::test]
async fn kv_service_puts_and_gets_over_a_real_socket() {
    use kv_proto::kv::kv_service_client::KvServiceClient;
    use kv_proto::kv::kv_service_server::KvServiceServer;
    use kv_proto::kv::{GetRequest, PutRequest};

    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let service =
        crate::kv_service::KvApi::new(tx.clone(), 1, crate::tests::support::published_one_shard(1));
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServiceServer::new(service))
            .serve(addr)
            .await
    });

    let mut client = loop {
        match KvServiceClient::connect(format!("http://127.0.0.1:{port}")).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };

    // Retrying through NotLeader, as a real client does: the lone node has to
    // finish its election first.
    for attempt in 0..200 {
        let resp = client
            .put(PutRequest { ctx: None, key: b"k".to_vec(), value: b"v".to_vec() })
            .await
            .unwrap()
            .into_inner();
        if resp.not_leader.is_none() {
            break;
        }
        assert!(attempt < 199, "no leader emerged");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let resp = client.get(GetRequest { key: b"k".to_vec() }).await.unwrap().into_inner();
    assert_eq!(resp.value, Some(b"v".to_vec()));
}

/// The session table shares a key space with user data under a reserved
/// prefix, so a client must not be able to write there — forging a session
/// entry would let it claim any request had already been answered.
///
/// Only reachable over gRPC: a NUL byte cannot survive `argv`, so the CLI can
/// never produce such a key and cannot test this.
#[tokio::test]
async fn a_reserved_key_is_refused_at_the_service_boundary() {
    use kv_proto::kv::kv_service_client::KvServiceClient;
    use kv_proto::kv::kv_service_server::KvServiceServer;
    use kv_proto::kv::{GetRequest, PutRequest};

    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let service =
        crate::kv_service::KvApi::new(tx.clone(), 1, crate::tests::support::published_one_shard(1));
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(KvServiceServer::new(service))
            .serve(addr)
            .await
    });

    let mut client = loop {
        match KvServiceClient::connect(format!("http://127.0.0.1:{port}")).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };

    let reserved = b"\x00session/forged".to_vec();
    let err = client
        .put(PutRequest { ctx: None, key: reserved.clone(), value: b"x".to_vec() })
        .await
        .expect_err("a reserved key must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got {err:?}");

    let err = client
        .get(GetRequest { key: reserved })
        .await
        .expect_err("reading the reserved space is refused too");
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got {err:?}");

    // An ordinary key with a zero byte later in it is fine — only the first
    // byte is reserved, so binary keys still work.
    // Accepted or redirected, but never rejected: that is what `expect`
    // proves here. Whether this lone node has finished its election yet is
    // beside the point.
    let binary = b"bin\x00ary".to_vec();
    client
        .put(PutRequest { ctx: None, key: binary, value: b"ok".to_vec() })
        .await
        .expect("a zero byte elsewhere in the key is not reserved");
}
