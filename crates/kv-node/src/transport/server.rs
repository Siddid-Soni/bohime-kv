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

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use kv_proto::raft as pb;
use kv_proto::raft::raft_service_server::RaftService;
use kv_raft::{Message, NodeId};
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};

use super::convert::{Outbound, assemble_snapshot, envelope, sender_of, unwrap_envelope};
use super::group::{self, GroupId};

/// One inbound message, with the channel its reply must go back on.
#[derive(Debug)]
pub struct Inbound {
    /// Which group it is for. Carried on the message rather than implied by
    /// the channel (M11.4), because one driver hosts every shard group in this
    /// process and reads them all off one inbox.
    pub group: GroupId,
    pub from: NodeId,
    pub msg: Message,
    pub reply: oneshot::Sender<Message>,
}

/// The group → inbox table the `RaftService` routes by, and the handle a
/// driver uses to put its own groups in it (M11.4).
///
/// Shared and republished rather than fixed at construction, because the set
/// moves at runtime: a node founds its shard groups once the shard map
/// arrives, which is after the listener is already serving. `ArcSwap` for the
/// reason the shard map uses one — replaced wholesale, read on every inbound
/// message, and no reader may ever block behind a writer.
#[derive(Clone, Default)]
pub struct GroupRegistry {
    table: Arc<ArcSwap<BTreeMap<GroupId, mpsc::Sender<Inbound>>>>,
}

impl GroupRegistry {
    /// Makes `group` routable to `inbox`. Idempotent: founding is a one-time
    /// act, but a restarted supervisor pass must not be an error.
    pub fn register(&self, group: GroupId, inbox: mpsc::Sender<Inbound>) {
        self.table.rcu(|current| {
            let mut next = BTreeMap::clone(current);
            next.insert(group, inbox.clone());
            next
        });
    }

    /// Stops routing to `group`. Used when a shard leaves this node (M12); a
    /// message that arrives meanwhile is refused as `not_found`, which is
    /// what the sender retries against the new owner.
    pub fn unregister(&self, group: GroupId) {
        self.table.rcu(|current| {
            let mut next = BTreeMap::clone(current);
            next.remove(&group);
            next
        });
    }

    fn get(&self, group: GroupId) -> Option<mpsc::Sender<Inbound>> {
        self.table.load().get(&group).cloned()
    }

    #[cfg(test)]
    pub fn groups(&self) -> Vec<GroupId> {
        self.table.load().keys().copied().collect()
    }
}

/// Fronts every Raft group this process hosts (M10.4, dynamic since M11.4).
pub struct RaftServer {
    groups: GroupRegistry,
}

impl RaftServer {
    /// A process serving only one group — the shape the M5/M6 transport tests
    /// are written against.
    #[cfg(test)]
    pub fn new(inbox: mpsc::Sender<Inbound>) -> Self {
        Self::with_groups(BTreeMap::from([(group::DATA, inbox)]))
    }

    #[cfg(test)]
    pub fn with_groups(inboxes: BTreeMap<GroupId, mpsc::Sender<Inbound>>) -> Self {
        let groups = GroupRegistry::default();
        for (group, inbox) in inboxes {
            groups.register(group, inbox);
        }
        Self { groups }
    }

    pub fn routing(groups: GroupRegistry) -> Self {
        Self { groups }
    }

    /// Resolves a wire group id to the inbox that owns it.
    ///
    /// The two rejections are deliberately different codes because they are
    /// different operator problems: `invalid_argument` is a malformed sender,
    /// `not_found` is a message that arrived at a process which does not host
    /// that group — routine while a cluster is starting or a shard is moving,
    /// and the peer will retry.
    fn inbox(&self, group: GroupId) -> Result<mpsc::Sender<Inbound>, Status> {
        if group == group::UNSET {
            return Err(Status::invalid_argument(
                "raft message carries no group id (0 is reserved)",
            ));
        }
        self.groups
            .get(group)
            .ok_or_else(|| Status::not_found(format!("this process does not serve group {group}")))
    }

    /// Queues `msg` for the driver and hands back the channel its reply will
    /// arrive on.
    ///
    /// Split out from [`Self::exchange`] for the batch handler's sake: every
    /// envelope in a batch has to be queued *before* any of them is awaited,
    /// or the batch costs one driver pass per message and the round trip it
    /// saved is spent again on latency.
    fn submit(
        &self,
        group: GroupId,
        from: NodeId,
        msg: Message,
    ) -> Result<oneshot::Receiver<Message>, Status> {
        let (reply, wait) = oneshot::channel();
        self.inbox(group)?
            .try_send(Inbound { group, from, msg, reply })
            .map_err(|_| Status::resource_exhausted("raft inbox full"))?;
        Ok(wait)
    }

    /// Hands `msg` to the driver and waits for its reply.
    ///
    /// A full inbox is shed as `resource_exhausted` rather than awaited:
    /// blocking here would let a slow driver pin every inbound connection, and
    /// Raft is built to tolerate a dropped message — the sender retries on its
    /// next heartbeat. M4 proved the core survives 20% loss.
    async fn exchange(
        &self,
        group: GroupId,
        from: NodeId,
        msg: Message,
    ) -> Result<Message, Status> {
        self.submit(group, from, msg)?.await.map_err(|_| Status::unavailable("raft driver stopped"))
    }
}

/// Unwraps the driver's reply, which must be the response variant this RPC
/// promised. A mismatch is a bug in the driver, not a peer error.
fn expect_response(msg: Message) -> Result<Outbound, Status> {
    match Outbound::from(msg) {
        out @ (Outbound::RequestVoteResp(_)
        | Outbound::AppendEntriesResp(_)
        | Outbound::InstallSnapshotResp(_)
        | Outbound::TimeoutNowResp(_)) => Ok(out),
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
        let (group, from) = (req.group, req.candidate_id);
        let msg = Message::try_from(Outbound::RequestVote(req))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match expect_response(self.exchange(group, from, msg).await?)? {
            Outbound::RequestVoteResp(r) => Ok(Response::new(r)),
            _ => Err(Status::internal("expected a RequestVote response")),
        }
    }

    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let (group, from) = (req.group, req.leader_id);
        let msg = Message::try_from(Outbound::AppendEntries(req))
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match expect_response(self.exchange(group, from, msg).await?)? {
            Outbound::AppendEntriesResp(r) => Ok(Response::new(r)),
            _ => Err(Status::internal("expected an AppendEntries response")),
        }
    }

    /// Reassembles the chunk stream into one snapshot message and hands it to
    /// the driver like any other inbound message. A stream that does not
    /// assemble is rejected before anything reaches the core: installing a
    /// gapped or truncated image would corrupt the state machine and look
    /// like a storage bug.
    async fn install_snapshot(
        &self,
        request: Request<Streaming<pb::InstallSnapshotChunk>>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let mut stream = request.into_inner();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.message().await? {
            chunks.push(chunk);
        }
        // Every chunk repeats the group, and `assemble_snapshot` already
        // refuses a stream whose chunks disagree, so the first one speaks for
        // the transfer.
        let group = chunks.first().map(|c| c.group).unwrap_or(group::UNSET);
        let msg =
            assemble_snapshot(&chunks).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let Message::InstallSnapshot { leader_id, .. } = &msg else {
            return Err(Status::internal("assembled chunks must form a snapshot"));
        };
        match expect_response(self.exchange(group, *leader_id, msg).await?)? {
            Outbound::InstallSnapshotResp(r) => Ok(Response::new(r)),
            _ => Err(Status::internal("expected an InstallSnapshot response")),
        }
    }

    /// Every group's traffic in one round trip (M11.3).
    ///
    /// Deliberately **not** all-or-nothing. An envelope for a group this
    /// process does not host, or whose inbox is full, is dropped and simply
    /// gets no reply — which is what a lost Raft message already is, and what
    /// the sender's next heartbeat re-drives. Failing the RPC instead would
    /// make one absent shard cost every other group in the same batch its
    /// round trip, which is the coupling batching would otherwise introduce.
    async fn batch(
        &self,
        request: Request<pb::BatchRequest>,
    ) -> Result<Response<pb::BatchResponse>, Status> {
        let envelopes = request.into_inner().msgs;
        // Queue everything first, then await: awaiting each in turn would
        // cost one driver pass per message and spend the saved round trip on
        // local latency instead.
        let mut waits = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            let (group, msg) = match unwrap_envelope(envelope) {
                Ok(pair) => pair,
                // A malformed envelope is the sender's bug, and refusing the
                // whole batch for it would punish the groups that were fine.
                Err(e) => {
                    tracing::debug!(error = %e, "dropping an undecodable batch envelope");
                    continue;
                }
            };
            let Some(from) = sender_of(&msg) else {
                // A response sent as a request: the peer addressed a reply to
                // us instead of answering an RPC. Nothing to step it as.
                tracing::debug!("dropping a response sent in a request batch");
                continue;
            };
            match self.submit(group, from, msg) {
                Ok(wait) => waits.push((group, wait)),
                Err(status) => {
                    tracing::debug!(group, %status, "dropping a batched message");
                }
            }
        }

        let mut msgs = Vec::with_capacity(waits.len());
        for (group, wait) in waits {
            match wait.await {
                Ok(reply) => msgs.push(envelope(group, reply)),
                // The driver stopped between queueing and answering. The rest
                // of the batch is still worth returning.
                Err(_) => tracing::debug!(group, "the driver dropped a batched reply"),
            }
        }
        Ok(Response::new(pb::BatchResponse { msgs }))
    }

    /// A leadership handoff: the recipient campaigns at once (the core
    /// answers with a transport ack, which is what this returns). Refused
    /// only when the driver is gone; an ack here promises nothing about the
    /// election that follows.
    async fn timeout_now(
        &self,
        request: Request<pb::TimeoutNowRequest>,
    ) -> Result<Response<pb::TimeoutNowResponse>, Status> {
        let req = request.into_inner();
        let msg = Message::TimeoutNow { term: req.term, leader_id: req.leader_id };
        match expect_response(self.exchange(req.group, req.leader_id, msg).await?)? {
            Outbound::TimeoutNowResp(r) => Ok(Response::new(r)),
            _ => Err(Status::internal("expected a TimeoutNow response")),
        }
    }
}
