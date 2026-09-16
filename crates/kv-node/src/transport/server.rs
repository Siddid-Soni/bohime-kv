//! The `RaftService` server (M5, task 2): inbound RPCs become `Message`s on a
//! bounded inbox, and the driver's reply travels back as the RPC response.
//!
//! **Deviation from the M5 sketch,** which had the inbox carry a bare
//! `(NodeId, Message)`: that cannot answer the RPC. The proto models Raft as
//! request/response, so something has to carry the reply back to this handler.
//!
//! A `oneshot` per request works because the Raft core is synchronous — the
//! reply to a RequestVote is produced by the very `step()` call that handles
//! it, never later — so the driver can always fulfil the channel immediately.
//! The alternative, making the service one-way and sending replies as fresh
//! RPCs in the other direction, needs every response type to become a request
//! type too, and leaves replies with no backpressure of their own.

use kv_proto::raft as pb;
use kv_proto::raft::raft_service_server::RaftService;
use kv_raft::{Message, NodeId};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};

use super::convert::Outbound;

/// One inbound message, with the channel its reply must go back on.
#[derive(Debug)]
pub struct Inbound {
    pub from: NodeId,
    pub msg: Message,
    pub reply: oneshot::Sender<Message>,
}

pub struct RaftServer {
    inbox: mpsc::Sender<Inbound>,
}

impl RaftServer {
    pub fn new(inbox: mpsc::Sender<Inbound>) -> Self {
        Self { inbox }
    }

    /// Hands `msg` to the driver and waits for its reply.
    ///
    /// A full inbox is shed as `resource_exhausted` rather than awaited:
    /// blocking here would let a slow driver pin every inbound connection, and
    /// Raft is built to tolerate a dropped message — the sender retries on its
    /// next heartbeat. M4 proved the core survives 20% loss.
    async fn exchange(&self, from: NodeId, msg: Message) -> Result<Message, Status> {
        let (reply, wait) = oneshot::channel();
        self.inbox
            .try_send(Inbound { from, msg, reply })
            .map_err(|_| Status::resource_exhausted("raft inbox full"))?;
        wait.await.map_err(|_| Status::unavailable("raft driver stopped"))
    }
}

/// Unwraps the driver's reply, which must be the response variant this RPC
/// promised. A mismatch is a bug in the driver, not a peer error.
fn expect_response(msg: Message) -> Result<Outbound, Status> {
    match Outbound::from(msg) {
        out @ (Outbound::RequestVoteResp(_)
        | Outbound::AppendEntriesResp(_)
        | Outbound::InstallSnapshotResp(_)) => Ok(out),
        _ => Err(Status::internal("raft driver replied with a request, not a response")),
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn request_vote(
        &self,
        request: Request<pb::RequestVoteRequest>,
    ) -> Result<Response<pb::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let from = req.candidate_id;
        let msg = Message::try_from(Outbound::RequestVote(req))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match expect_response(self.exchange(from, msg).await?)? {
            Outbound::RequestVoteResp(r) => Ok(Response::new(r)),
            _ => Err(Status::internal("expected a RequestVote response")),
        }
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let from = req.leader_id;
        let msg = Message::try_from(Outbound::AppendEntries(req))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match expect_response(self.exchange(from, msg).await?)? {
            Outbound::AppendEntriesResp(r) => Ok(Response::new(r)),
            _ => Err(Status::internal("expected an AppendEntries response")),
        }
    }

    /// Chunked snapshot transfer is M8. Refused explicitly rather than
    /// half-implemented: accepting one chunk and treating it as a whole
    /// snapshot would install a truncated state machine.
    async fn install_snapshot(
        &self,
        _request: Request<Streaming<pb::InstallSnapshotChunk>>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        Err(Status::unimplemented("InstallSnapshot arrives at M8"))
    }
}
