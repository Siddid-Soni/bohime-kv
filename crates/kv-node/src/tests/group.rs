//! Addressing a message to a Raft *group* (M10.4).
//!
//! Until M10 a process held exactly one `RaftNode`, so `RaftService` could
//! shovel every inbound message into one inbox. The meta group makes that
//! ambiguous: two groups share a node, a port and a peer address, and only
//! the group id distinguishes an AppendEntries for the shard map from one for
//! user data. Misrouting one is not a dropped message — it is a message
//! applied to the wrong log.

use std::collections::BTreeMap;

use kv_proto::raft as pb;
use kv_proto::raft::raft_service_client::RaftServiceClient;
use kv_proto::raft::raft_service_server::RaftServiceServer;
use kv_raft::{Message, NodeId};
use tokio::sync::mpsc;

use crate::transport::group::{self, GroupId};
use crate::transport::peer::{PeerClient, PeerConfig};
use crate::transport::server::{Inbound, RaftServer};

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Serves one `RaftServer` fronting an inbox per group. Each group's
/// responder answers with a canned reply, so the test observes *routing*
/// rather than any particular node's behaviour.
async fn serve(port: u16, groups: &[GroupId]) -> BTreeMap<GroupId, mpsc::Receiver<Message>> {
    let mut inboxes = BTreeMap::new();
    let mut seen = BTreeMap::new();

    for &g in groups {
        let (inbox_tx, mut inbox_rx) = mpsc::channel::<Inbound>(32);
        let (seen_tx, seen_rx) = mpsc::channel::<Message>(32);
        tokio::spawn(async move {
            while let Some(Inbound { msg, reply, .. }) = inbox_rx.recv().await {
                let _ = seen_tx.send(msg).await;
                let _ = reply.send(Message::RequestVoteResp { term: 1, vote_granted: true });
            }
        });
        inboxes.insert(g, inbox_tx);
        seen.insert(g, seen_rx);
    }

    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftServiceServer::new(RaftServer::with_groups(inboxes)))
            .serve(addr)
            .await
    });
    // Let the listener bind before anyone dials it.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    seen
}

/// A `RaftService` that counts the RPCs it is called with, so a test can tell
/// "200 messages arrived" from "200 messages arrived in 200 round trips".
/// Test-owned rather than a counter bolted onto `RaftServer`: the number is a
/// property of this test, not of the server.
struct CountingService {
    rpcs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    seen: mpsc::Sender<pb::RaftEnvelope>,
}

#[tonic::async_trait]
impl pb::raft_service_server::RaftService for CountingService {
    async fn batch(
        &self,
        request: tonic::Request<pb::BatchRequest>,
    ) -> Result<tonic::Response<pb::BatchResponse>, tonic::Status> {
        self.rpcs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut msgs = Vec::new();
        for envelope in request.into_inner().msgs {
            let group = envelope.group;
            let _ = self.seen.send(envelope).await;
            msgs.push(pb::RaftEnvelope {
                group,
                msg: Some(pb::raft_envelope::Msg::VoteResp(pb::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                })),
            });
        }
        Ok(tonic::Response::new(pb::BatchResponse { msgs }))
    }

    async fn request_vote(
        &self,
        _request: tonic::Request<pb::RequestVoteRequest>,
    ) -> Result<tonic::Response<pb::RequestVoteResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("this stub only counts batches"))
    }

    async fn append_entries(
        &self,
        _request: tonic::Request<pb::AppendEntriesRequest>,
    ) -> Result<tonic::Response<pb::AppendEntriesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("this stub only counts batches"))
    }

    async fn install_snapshot(
        &self,
        _request: tonic::Request<tonic::Streaming<pb::InstallSnapshotChunk>>,
    ) -> Result<tonic::Response<pb::InstallSnapshotResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("this stub only counts batches"))
    }

    async fn timeout_now(
        &self,
        _request: tonic::Request<pb::TimeoutNowRequest>,
    ) -> Result<tonic::Response<pb::TimeoutNowResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("this stub only counts batches"))
    }
}

async fn serve_counting(
    port: u16,
    rpcs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    seen: mpsc::Sender<pb::RaftEnvelope>,
) {
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftServiceServer::new(CountingService { rpcs, seen }))
            .serve(addr)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

fn vote_from(candidate: NodeId, group: GroupId) -> pb::RequestVoteRequest {
    pb::RequestVoteRequest {
        group,
        term: 1,
        candidate_id: candidate,
        last_log_index: 0,
        last_log_term: 0,
    }
}

/// Group ids must be distinct and must not collide with the reserved value.
#[test]
fn the_group_numbering_leaves_zero_unused() {
    assert_ne!(group::META, group::UNSET);
    assert_ne!(group::shard(0), group::UNSET);
    assert_ne!(group::shard(0), group::META);
    assert_eq!(group::DATA, group::shard(0));
    // Distinct shards get distinct groups, with no wraparound at the top.
    assert_ne!(group::shard(0), group::shard(1));
    assert_ne!(group::shard(u16::MAX), group::shard(0));
}

#[tokio::test]
async fn each_group_receives_only_its_own_messages() {
    let port = free_port();
    let mut seen = serve(port, &[group::DATA, group::META]).await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut client = RaftServiceClient::connect(endpoint).await.expect("connect");

    client.request_vote(vote_from(7, group::META)).await.expect("meta vote");

    let meta = seen.get_mut(&group::META).unwrap().recv().await.expect("meta inbox");
    assert!(matches!(meta, Message::RequestVote { candidate_id: 7, .. }));

    // The data group must not have seen it.
    assert!(
        seen.get_mut(&group::DATA).unwrap().try_recv().is_err(),
        "a message for the meta group reached the data group"
    );
}

#[tokio::test]
async fn a_message_for_an_unknown_group_is_refused() {
    let port = free_port();
    let mut seen = serve(port, &[group::DATA]).await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut client = RaftServiceClient::connect(endpoint).await.expect("connect");

    let status = client
        .request_vote(vote_from(7, group::shard(500)))
        .await
        .expect_err("a group this process does not serve must be refused");
    assert_eq!(status.code(), tonic::Code::NotFound, "{status:?}");

    assert!(
        seen.get_mut(&group::DATA).unwrap().try_recv().is_err(),
        "an unroutable message was delivered to some other group"
    );
}

/// proto3 gives an absent field its zero value, so a sender that forgot to
/// set the group is indistinguishable from one that set it to 0. Reserving 0
/// turns that into a loud error instead of a silent delivery to whichever
/// group happened to be numbered first — the same reasoning that made M5's
/// conflict hints `optional` rather than sentinel-zero.
#[tokio::test]
async fn an_unset_group_is_refused_rather_than_guessed() {
    let port = free_port();
    let mut seen = serve(port, &[group::DATA, group::META]).await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut client = RaftServiceClient::connect(endpoint).await.expect("connect");

    let status = client
        .request_vote(vote_from(7, group::UNSET))
        .await
        .expect_err("group 0 is reserved and must never route");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");

    for g in [group::DATA, group::META] {
        assert!(
            seen.get_mut(&g).unwrap().try_recv().is_err(),
            "group {g} received a groupless message"
        );
    }
}

/// The peer client is what the driver actually sends through, so the group
/// has to survive all the way onto the wire — and since M11.2 the group
/// travels with the *message*, not with the link.
///
/// One link per peer rather than one per `(peer, group)` is not tidiness: a
/// node hosting 154 shard groups (256 shards, RF 3, 5 nodes) would otherwise
/// open 154 connections to each of its peers, each with its own queue and its
/// own reconnect loop.
#[tokio::test]
async fn one_link_carries_every_groups_traffic_and_tags_the_replies() {
    let port = free_port();
    let mut seen = serve(port, &[group::DATA, group::META]).await;
    let (replies_tx, mut replies) = mpsc::channel(32);

    let peer = PeerClient::connect(
        2,
        format!("http://127.0.0.1:{port}"),
        PeerConfig::default(),
        replies_tx,
    );

    for group in [group::META, group::DATA] {
        peer.try_send(
            group,
            Message::RequestVote { term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0 },
        )
        .expect("queued");
    }

    for group in [group::META, group::DATA] {
        let got = seen.get_mut(&group).unwrap().recv().await.expect("group inbox");
        assert!(
            matches!(got, Message::RequestVote { candidate_id: 1, .. }),
            "group {group} received {got:?}"
        );
    }

    // A response carries no group of its own, so the link has to remember
    // which group asked — otherwise a shard's vote reply would be stepped into
    // whichever group happened to read the channel first.
    let mut answered = std::collections::BTreeSet::new();
    for _ in 0..2 {
        let (group, from, msg) = replies.recv().await.expect("a reply per request");
        assert_eq!(from, 2, "the reply must name the peer that answered");
        assert!(matches!(msg, Message::RequestVoteResp { .. }), "{msg:?}");
        answered.insert(group);
    }
    assert_eq!(
        answered,
        std::collections::BTreeSet::from([group::DATA, group::META]),
        "each group must get its own reply back"
    );
}

/// One RPC carrying many groups' messages (M11.3).
///
/// The sender loop awaits a round trip per RPC, so one message per RPC caps a
/// link at roughly one message per network round trip. A node hosting 154
/// shard groups — 256 shards, RF 3, five nodes — has to heartbeat all of them
/// inside one election timeout, which that cap makes impossible. Batching
/// turns it into one round trip per drain instead of one per message.
#[tokio::test]
async fn one_batch_is_demultiplexed_and_every_group_answers() {
    let port = free_port();
    let mut seen = serve(port, &[group::DATA, group::META, group::shard(7)]).await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut client = RaftServiceClient::connect(endpoint).await.expect("connect");

    let batch = pb::BatchRequest {
        msgs: [group::META, group::shard(7), group::DATA]
            .into_iter()
            .map(|group| pb::RaftEnvelope {
                group,
                msg: Some(pb::raft_envelope::Msg::Vote(vote_from(7, group))),
            })
            .collect(),
    };
    let replies = client.batch(batch).await.expect("batch").into_inner();

    for group in [group::DATA, group::META, group::shard(7)] {
        let got = seen.get_mut(&group).unwrap().recv().await.expect("group inbox");
        assert!(
            matches!(got, Message::RequestVote { candidate_id: 7, .. }),
            "group {group} received {got:?}"
        );
    }

    let answered: std::collections::BTreeSet<GroupId> =
        replies.msgs.iter().map(|e| e.group).collect();
    assert_eq!(
        answered,
        std::collections::BTreeSet::from([group::DATA, group::META, group::shard(7)]),
        "every group in the batch must answer in the batch"
    );
    for envelope in &replies.msgs {
        assert!(
            matches!(envelope.msg, Some(pb::raft_envelope::Msg::VoteResp(_))),
            "group {} answered with {:?}",
            envelope.group,
            envelope.msg
        );
    }
}

/// A batch is not all-or-nothing. One group being absent — a shard that moved,
/// or a process still starting — must not cost the other groups in the same
/// RPC their round trip, which is exactly the coupling batching would
/// otherwise introduce.
#[tokio::test]
async fn an_unroutable_envelope_does_not_fail_the_whole_batch() {
    let port = free_port();
    let mut seen = serve(port, &[group::DATA]).await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let mut client = RaftServiceClient::connect(endpoint).await.expect("connect");

    let batch = pb::BatchRequest {
        msgs: vec![
            pb::RaftEnvelope {
                group: group::shard(500),
                msg: Some(pb::raft_envelope::Msg::Vote(vote_from(7, group::shard(500)))),
            },
            pb::RaftEnvelope {
                group: group::DATA,
                msg: Some(pb::raft_envelope::Msg::Vote(vote_from(7, group::DATA))),
            },
        ],
    };
    let replies = client.batch(batch).await.expect("the batch itself must succeed").into_inner();

    let got = seen.get_mut(&group::DATA).unwrap().recv().await.expect("data inbox");
    assert!(matches!(got, Message::RequestVote { candidate_id: 7, .. }), "{got:?}");
    assert_eq!(replies.msgs.len(), 1, "only the routable group answers");
    assert_eq!(replies.msgs[0].group, group::DATA);
}

/// The whole point of the batch is that the *client* uses it: a queue that
/// drained one message per RPC would still be capped at one message per round
/// trip however well the server batched.
#[tokio::test]
async fn a_peer_client_coalesces_its_queue_into_one_rpc() {
    let port = free_port();
    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (seen_tx, mut seen) = mpsc::channel(512);
    serve_counting(port, std::sync::Arc::clone(&counter), seen_tx).await;

    let (replies_tx, _replies) = mpsc::channel(512);
    let peer = PeerClient::connect(
        2,
        format!("http://127.0.0.1:{port}"),
        PeerConfig { queue_depth: 512, ..PeerConfig::default() },
        replies_tx,
    );

    const SENT: usize = 200;
    for i in 0..SENT {
        peer.try_send(
            group::shard(i as u16),
            Message::RequestVote { term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0 },
        )
        .expect("queued");
    }

    let mut arrived = 0;
    while arrived < SENT {
        seen.recv().await.expect("every queued message must arrive");
        arrived += 1;
    }
    let rpcs = counter.load(std::sync::atomic::Ordering::SeqCst);
    assert!(rpcs >= 1, "nothing was sent");
    assert!(
        rpcs < SENT / 4,
        "{SENT} queued messages should coalesce into far fewer than {SENT} RPCs, got {rpcs}"
    );
}
