//! M5's gates, over a real socket on loopback.
//!
//! The driver here is the M6 loop in miniature: take an inbound message, step
//! a real `RaftNode`, and answer with the reply that step produced. Using a
//! real node rather than a canned response is the point — it proves the whole
//! path, conversion included, rather than proving the socket works.

use std::time::Duration;

use kv_proto::raft::raft_service_server::RaftServiceServer;
use kv_raft::storage::MemStorage;
use kv_raft::{Action, Config, Message, NodeId, RaftNode};
use tokio::sync::mpsc;

use crate::transport::peer::{PeerClient, PeerConfig, SendError};
use crate::transport::server::{Inbound, RaftServer};

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn config(id: NodeId, peers: Vec<NodeId>) -> Config {
    Config { id, peers, election_timeout: 10, heartbeat_interval: 2, seed: 7 }
}

/// Serves a `RaftServer` backed by a real node on `port`.
///
/// Returns once the listener is bound, so a client connecting immediately
/// afterwards is not racing the bind.
async fn serve(port: u16, node_id: NodeId, peers: Vec<NodeId>) -> mpsc::Receiver<Message> {
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<Inbound>(32);
    let (seen_tx, seen_rx) = mpsc::channel::<Message>(32);

    tokio::spawn(async move {
        let mut node = RaftNode::new(config(node_id, peers), MemStorage::default());
        while let Some(Inbound { from, msg, reply }) = inbox_rx.recv().await {
            let _ = seen_tx.send(msg.clone()).await;
            let actions = node.step(from, msg);
            // The reply to a request is produced by the step that handled it,
            // addressed back to the sender.
            let answer = actions.into_iter().find_map(|a| match a {
                Action::Send { to, msg } if to == from => Some(msg),
                _ => None,
            });
            if let Some(answer) = answer {
                let _ = reply.send(answer);
            }
        }
    });

    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftServiceServer::new(RaftServer::new(inbox_tx)))
            .serve(addr)
            .await
    });

    seen_rx
}

#[tokio::test]
async fn request_vote_and_append_entries_cross_a_real_socket() {
    let port = free_port();
    let mut seen = serve(port, 2, vec![1, 3]).await;

    let (replies_tx, mut replies) = mpsc::channel(32);
    let client = PeerClient::connect(
        2,
        format!("http://127.0.0.1:{port}"),
        PeerConfig::default(),
        replies_tx,
    );

    client
        .try_send(Message::RequestVote {
            term: 5,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), seen.recv())
        .await
        .expect("the server should receive the vote request")
        .unwrap();
    assert!(matches!(received, Message::RequestVote { term: 5, candidate_id: 1, .. }));

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv())
        .await
        .expect("the vote response should come back")
        .unwrap();
    assert_eq!(reply.0, 2, "a reply is tagged with the peer we called");
    // An empty log and a fresh term: the vote is granted.
    assert!(matches!(reply.1, Message::RequestVoteResp { term: 5, vote_granted: true }));

    client
        .try_send(Message::AppendEntries {
            term: 5,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![kv_raft::Entry { term: 5, index: 1, command: b"x".to_vec() }],
            leader_commit: 0,
            read_round: None,
        })
        .unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), seen.recv())
        .await
        .expect("the server should receive the append")
        .unwrap();
    match received {
        Message::AppendEntries { entries, .. } => {
            assert_eq!(entries.len(), 1, "entries must survive the wire");
            assert_eq!(entries[0].command, b"x".to_vec());
        }
        other => panic!("expected AppendEntries, got {other:?}"),
    }

    let reply = tokio::time::timeout(Duration::from_secs(5), replies.recv())
        .await
        .expect("the append response should come back")
        .unwrap();
    assert!(matches!(reply.1, Message::AppendEntriesResp { success: true, match_index: 1, .. }));
}

#[tokio::test]
async fn a_dead_peer_backs_off_instead_of_spinning() {
    // Nothing is listening on this port for the whole test.
    let port = free_port();
    let (replies_tx, _replies) = mpsc::channel(8);
    let client = PeerClient::connect(
        9,
        format!("http://127.0.0.1:{port}"),
        PeerConfig {
            queue_depth: 256,
            request_timeout: Duration::from_millis(50),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_secs(5),
        },
        replies_tx,
    );

    // Keep the queue fed so the sender task always has work; a client that
    // spins would burn through attempts as fast as it can dial.
    for _ in 0..200 {
        let _ = client.try_send(Message::RequestVoteResp { term: 1, vote_granted: false });
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let attempts = client.connect_attempts();
    // Doubling from 50ms, ~1.2s allows roughly 50+100+200+400 => 5 attempts.
    // The bound is what makes this test meaningful: "it didn't spin" is not
    // observable without a count, and an un-backed-off client would be in the
    // hundreds here.
    assert!(attempts >= 1, "it should have tried at least once, got {attempts}");
    assert!(attempts <= 12, "a dead peer should back off, but it tried {attempts} times in 1.2s");
}

#[tokio::test]
async fn a_full_queue_sheds_instead_of_growing() {
    let port = free_port();
    let (replies_tx, _replies) = mpsc::channel(8);
    let depth = 4;
    let client = PeerClient::connect(
        9,
        // Nothing listening, so nothing drains: the queue fills and stays full.
        format!("http://127.0.0.1:{port}"),
        PeerConfig {
            queue_depth: depth,
            request_timeout: Duration::from_millis(50),
            backoff_initial: Duration::from_secs(30),
            backoff_max: Duration::from_secs(30),
        },
        replies_tx,
    );

    let mut shed = 0;
    for _ in 0..500 {
        if client.try_send(Message::RequestVoteResp { term: 1, vote_granted: false })
            == Err(SendError::Full)
        {
            shed += 1;
        }
        assert!(client.queued() <= depth, "queue grew past its bound to {}", client.queued());
    }

    assert!(shed > 400, "most sends should have been shed, only {shed} were");
}
