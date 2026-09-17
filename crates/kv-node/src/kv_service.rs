//! The client-facing gRPC service (M6). Every handler turns its request into a
//! `ClientRequest`, hands it to the driver and waits on a `oneshot` — the
//! driver is the only thing allowed to touch the Raft node or the state
//! machine.

// Every fallible call here returns `tonic::Status`, which is ~176 bytes and so
// trips `result_large_err`. Boxing it would mean unwrapping at every tonic
// boundary for no benefit; the transport and kv-proto carry the same allow.
#![allow(clippy::result_large_err)]

use kv_proto::kv as pb;
use kv_proto::kv::kv_service_server::KvService;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use kv_raft::NodeId;
use kv_ring::ShardId;

use crate::command::Mutation;
use crate::driver::{ClientOp, ClientReply, ClientRequest};
use crate::meta::PublishedMap;
use crate::router::{Route, route};
use crate::session::{self, RequestCtx};
use crate::transport::group::GroupId;

pub struct KvApi {
    /// The shard driver's request channel: one driver hosting every shard
    /// group this node replicates.
    requests: mpsc::Sender<ClientRequest>,
    /// This node's id, to ask the map whether a key is ours.
    me: NodeId,
    /// The placement every request is routed by — read on every request,
    /// written only when the meta group republishes (§1.15).
    published: PublishedMap,
}

/// How a request was routed, when it did not reach a group.
struct Misrouted {
    shard: ShardId,
    replicas: Vec<NodeId>,
    map_version: u64,
}

impl KvApi {
    pub fn new(requests: mpsc::Sender<ClientRequest>, me: NodeId, published: PublishedMap) -> Self {
        Self { requests, me, published }
    }

    /// Resolves `key` to the local group that holds it.
    ///
    /// The routing decision is made here rather than in the driver so that a
    /// key belonging elsewhere never occupies a slot in the driver's queue —
    /// under a stale client map that would be the majority of the traffic.
    fn resolve(&self, key: &[u8]) -> Result<Result<GroupId, Misrouted>, Status> {
        let map = self.published.load_full();
        match route(map.as_deref(), self.me, key) {
            Route::Local { group, .. } => Ok(Ok(group)),
            Route::Elsewhere { shard, replicas } => Ok(Err(Misrouted {
                shard,
                replicas,
                map_version: map.as_ref().map(|m| m.version).unwrap_or(0),
            })),
            // Nowhere to point the client: this node does not yet know where
            // any key belongs. `unavailable` rather than an answer, because
            // every answer available here would be invented.
            Route::NoMap => {
                Err(Status::unavailable("this node has no shard map yet; placement is not known"))
            }
        }
    }

    /// A full queue is shed as `resource_exhausted` rather than awaited, for
    /// the same reason the Raft inbox sheds (M5): blocking here would let a
    /// slow driver pin every inbound connection.
    async fn call(&self, group: GroupId, op: ClientOp) -> Result<ClientReply, Status> {
        let (reply, wait) = oneshot::channel();
        self.requests
            .try_send(ClientRequest { group, op, reply })
            .map_err(|_| Status::resource_exhausted("client request queue full"))?;
        wait.await.map_err(|_| Status::unavailable("node is shutting down"))
    }
}

fn not_hosted(misrouted: Misrouted) -> pb::NotHosted {
    pb::NotHosted {
        shard: misrouted.shard as u32,
        replicas: misrouted.replicas,
        map_version: misrouted.map_version,
    }
}

/// The driver's own `NotHosted`, for the window where the service's map still
/// says a shard is ours and the driver has already let it go. The replica list
/// is empty because the map that would fill it is the one that is behind.
fn not_hosted_by_driver(shard: ShardId) -> pb::NotHosted {
    pb::NotHosted { shard: shard as u32, replicas: Vec::new(), map_version: 0 }
}

/// Node ids start at 1 throughout (`NodeConfig`), so 0 doubles as "this node
/// knows of no leader either" — the client then scans instead of following a
/// hint to nowhere.
/// The session table lives in the same key space as user data, under a
/// reserved prefix, so a client must not be able to write there. Rejected at
/// the boundary rather than escaped, because escaping would change the
/// on-disk layout of every existing key for a case no real client wants.
fn check_key(key: &[u8]) -> Result<(), Status> {
    if session::is_reserved(key) {
        return Err(Status::invalid_argument("keys may not begin with a zero byte (reserved)"));
    }
    Ok(())
}

/// `None` when the client sent no context, meaning it is not tracking
/// retries. Not a sentinel id: client 0 is a real client.
fn ctx_from(ctx: Option<pb::ClientContext>) -> Option<RequestCtx> {
    ctx.map(|c| RequestCtx { client_id: c.client_id, sequence: c.sequence_number })
}

fn not_leader(hint: Option<u64>) -> pb::NotLeader {
    pb::NotLeader { leader_hint: hint.unwrap_or(0) }
}

#[tonic::async_trait]
impl KvService for KvApi {
    /// Linearizable since M7: served through ReadIndex, so only a leader that
    /// can confirm a quorum answers. A follower redirects.
    ///
    /// M6 read local state here, which let a deposed leader serve a value that
    /// had already been overwritten — `tests::linearizability` reproduces that
    /// exact violation and now guards against it.
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        let key = request.into_inner().key;
        check_key(&key)?;
        let group = match self.resolve(&key)? {
            Ok(group) => group,
            Err(misrouted) => {
                return Ok(Response::new(pb::GetResponse {
                    value: None,
                    not_leader: None,
                    not_hosted: Some(not_hosted(misrouted)),
                }));
            }
        };
        match self.call(group, ClientOp::Get { key }).await? {
            ClientReply::Value(value) => {
                Ok(Response::new(pb::GetResponse { value, not_leader: None, not_hosted: None }))
            }
            // Since M7 a read needs a leadership quorum, so a follower
            // redirects exactly as it does for a write. This is an ordinary
            // answer, not an error: reporting it as one would have the client
            // drop the connection instead of following the hint.
            ClientReply::NotLeader { hint } => Ok(Response::new(pb::GetResponse {
                value: None,
                not_leader: Some(not_leader(hint)),
                not_hosted: None,
            })),
            ClientReply::NotHosted { shard } => Ok(Response::new(pb::GetResponse {
                value: None,
                not_leader: None,
                not_hosted: Some(not_hosted_by_driver(shard)),
            })),
            other => Err(Status::internal(format!("driver answered a get with {other:?}"))),
        }
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let req = request.into_inner();
        check_key(&req.key)?;
        let group = match self.resolve(&req.key)? {
            Ok(group) => group,
            Err(misrouted) => {
                return Ok(Response::new(pb::PutResponse {
                    not_leader: None,
                    not_hosted: Some(not_hosted(misrouted)),
                }));
            }
        };
        let op = Mutation::Put { key: req.key, value: req.value };
        match self.call(group, ClientOp::Mutate { ctx: ctx_from(req.ctx), op }).await? {
            ClientReply::Applied => {
                Ok(Response::new(pb::PutResponse { not_leader: None, not_hosted: None }))
            }
            ClientReply::NotLeader { hint } => Ok(Response::new(pb::PutResponse {
                not_leader: Some(not_leader(hint)),
                not_hosted: None,
            })),
            ClientReply::NotHosted { shard } => Ok(Response::new(pb::PutResponse {
                not_leader: None,
                not_hosted: Some(not_hosted_by_driver(shard)),
            })),
            other => Err(Status::internal(format!("driver answered a put with {other:?}"))),
        }
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        let req = request.into_inner();
        check_key(&req.key)?;
        let group = match self.resolve(&req.key)? {
            Ok(group) => group,
            Err(misrouted) => {
                return Ok(Response::new(pb::DeleteResponse {
                    not_leader: None,
                    not_hosted: Some(not_hosted(misrouted)),
                }));
            }
        };
        let op = Mutation::Delete { key: req.key };
        match self.call(group, ClientOp::Mutate { ctx: ctx_from(req.ctx), op }).await? {
            ClientReply::Applied => {
                Ok(Response::new(pb::DeleteResponse { not_leader: None, not_hosted: None }))
            }
            ClientReply::NotLeader { hint } => Ok(Response::new(pb::DeleteResponse {
                not_leader: Some(not_leader(hint)),
                not_hosted: None,
            })),
            ClientReply::NotHosted { shard } => Ok(Response::new(pb::DeleteResponse {
                not_leader: None,
                not_hosted: Some(not_hosted_by_driver(shard)),
            })),
            other => Err(Status::internal(format!("driver answered a delete with {other:?}"))),
        }
    }

    /// Compare-and-swap, the operation that makes the session table earn its
    /// keep. `expected: None` means "only if absent".
    ///
    /// A client that retries a `Cas` without a `ClientContext` gets a second
    /// evaluation, and the second one sees the value the first swapped in — so
    /// it answers `swapped: false` for an operation that did take effect. That
    /// is a wrong answer, not a wasted write, which is why the context is what
    /// makes retry safe here and merely tidy for `Put`.
    async fn cas(
        &self,
        request: Request<pb::CasRequest>,
    ) -> Result<Response<pb::CasResponse>, Status> {
        let req = request.into_inner();
        check_key(&req.key)?;
        let group = match self.resolve(&req.key)? {
            Ok(group) => group,
            Err(misrouted) => {
                return Ok(Response::new(pb::CasResponse {
                    swapped: false,
                    not_leader: None,
                    not_hosted: Some(not_hosted(misrouted)),
                }));
            }
        };
        let op = Mutation::Cas { key: req.key, expected: req.expected, new_value: req.new_value };
        match self.call(group, ClientOp::Mutate { ctx: ctx_from(req.ctx), op }).await? {
            ClientReply::Swapped(swapped) => {
                Ok(Response::new(pb::CasResponse { swapped, not_leader: None, not_hosted: None }))
            }
            ClientReply::NotLeader { hint } => Ok(Response::new(pb::CasResponse {
                swapped: false,
                not_leader: Some(not_leader(hint)),
                not_hosted: None,
            })),
            ClientReply::NotHosted { shard } => Ok(Response::new(pb::CasResponse {
                swapped: false,
                not_leader: None,
                not_hosted: Some(not_hosted_by_driver(shard)),
            })),
            other => Err(Status::internal(format!("driver answered a cas with {other:?}"))),
        }
    }
}
