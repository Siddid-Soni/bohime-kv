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
use crate::driver::{ClientOp, ClientReply, ClientRequest, Driver};
use crate::storage::BitcaskStorage;

fn config(dir: &std::path::Path, peers: BTreeMap<u64, String>) -> NodeConfig {
    let config = NodeConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().unwrap(),
        peers,
        data_dir: dir.to_path_buf(),
        tick: Duration::from_millis(5),
        election_timeout: 4,
        heartbeat_interval: 1,
    };
    std::fs::create_dir_all(config.raft_dir()).unwrap();
    std::fs::create_dir_all(config.state_dir()).unwrap();
    config
}

/// Starts a driver and returns the request channel. The inbox and peer-reply
/// senders are returned too: dropping them would close those `select!` arms
/// and shut the loop down mid-test.
fn spawn(config: &NodeConfig) -> (mpsc::Sender<ClientRequest>, Box<dyn std::any::Any + Send>) {
    let node =
        RaftNode::new(config.raft_config(), BitcaskStorage::open(config.raft_dir()).unwrap());
    let engine = Engine::open(config.state_dir()).unwrap();

    let (inbox_tx, inbox) = mpsc::channel(8);
    let (replies_tx, replies) = mpsc::channel(8);
    let (requests, requests_rx) = mpsc::channel(8);

    let driver = Driver::new(config, node, engine, BTreeMap::new(), inbox, replies, requests_rx);
    tokio::spawn(driver.run());
    (requests, Box::new((inbox_tx, replies_tx)))
}

/// One request, one answer, no retry. Used where the refusal *is* the
/// assertion.
async fn call(tx: &mpsc::Sender<ClientRequest>, op: ClientOp) -> ClientReply {
    let (reply, wait) = oneshot::channel();
    tx.send(ClientRequest { op, reply }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("the driver answers")
        .expect("the driver does not drop the request")
}

/// Retries through `NotLeader`, which is what a real client does (kv-client
/// does exactly this). A write issued before the lone node's election timeout
/// has elapsed is legitimately refused — that is the driver working, not
/// failing, so the test must not treat the first refusal as the answer.
async fn write(tx: &mpsc::Sender<ClientRequest>, op: impl Fn() -> ClientOp) -> ClientReply {
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

#[tokio::test]
async fn a_put_is_committed_then_readable() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let reply = write(&tx, || ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");

    let reply = call(&tx, ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(Some(ref v)) if v == b"v"), "got {reply:?}");
}

#[tokio::test]
async fn a_delete_removes_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    write(&tx, || ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }).await;
    let reply = write(&tx, || ClientOp::Delete { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");

    let reply = call(&tx, ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(None)), "got {reply:?}");
}

#[tokio::test]
async fn a_missing_key_reads_as_none() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let reply = call(&tx, ClientOp::Get { key: b"absent".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(None)), "got {reply:?}");
}

/// Everything the client was told was `Applied` must survive losing the
/// process, because that is the only promise a write ever made.
#[tokio::test]
async fn applied_writes_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), BTreeMap::new());

    let (first, keep) = spawn(&config);
    let reply = write(&first, || ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Applied), "got {reply:?}");
    drop((first, keep));
    // Let the old driver observe its closed channels and release the Bitcask
    // directories before the replacement opens them.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (second, _keep) = spawn(&config);
    let reply = call(&second, ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(Some(ref v)) if v == b"v"), "got {reply:?}");
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

    let reply = call(&tx, ClientOp::Put { key: b"k".to_vec(), value: b"v".to_vec() }).await;
    assert!(matches!(reply, ClientReply::NotLeader { .. }), "got {reply:?}");

    // And it did not quietly apply anyway.
    let reply = call(&tx, ClientOp::Get { key: b"k".to_vec() }).await;
    assert!(matches!(reply, ClientReply::Value(None)), "a refused write must not land: {reply:?}");
}

#[tokio::test]
async fn kv_service_puts_and_gets_over_a_real_socket() {
    use kv_proto::kv::kv_service_client::KvServiceClient;
    use kv_proto::kv::kv_service_server::KvServiceServer;
    use kv_proto::kv::{GetRequest, PutRequest};

    let dir = tempfile::tempdir().unwrap();
    let (tx, _keep) = spawn(&config(dir.path(), BTreeMap::new()));

    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let service = crate::kv_service::KvApi::new(tx.clone());
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
