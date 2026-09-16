//! The client-facing gRPC service (M6). Every handler turns its request into a
//! `ClientRequest`, hands it to the driver and waits on a `oneshot` — the
//! driver is the only thing allowed to touch the Raft node or the state
//! machine.

use kv_proto::kv as pb;
use kv_proto::kv::kv_service_server::KvService;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::driver::{ClientOp, ClientReply, ClientRequest};

pub struct KvApi {
    requests: mpsc::Sender<ClientRequest>,
}

impl KvApi {
    pub fn new(requests: mpsc::Sender<ClientRequest>) -> Self {
        Self { requests }
    }

    /// A full queue is shed as `resource_exhausted` rather than awaited, for
    /// the same reason the Raft inbox sheds (M5): blocking here would let a
    /// slow driver pin every inbound connection.
    async fn call(&self, op: ClientOp) -> Result<ClientReply, Status> {
        let (reply, wait) = oneshot::channel();
        self.requests
            .try_send(ClientRequest { op, reply })
            .map_err(|_| Status::resource_exhausted("client request queue full"))?;
        wait.await.map_err(|_| Status::unavailable("node is shutting down"))
    }
}

/// Node ids start at 1 throughout (`NodeConfig`), so 0 doubles as "this node
/// knows of no leader either" — the client then scans instead of following a
/// hint to nowhere.
fn not_leader(hint: Option<u64>) -> pb::NotLeader {
    pb::NotLeader { leader_hint: hint.unwrap_or(0) }
}

#[tonic::async_trait]
impl KvService for KvApi {
    /// **Deliberately stale (M6).** This reads local state, so a deposed
    /// leader that has not yet learned it lost will serve its old value. The
    /// gate for this milestone is "get k on any node returns it", and M7
    /// replaces this with ReadIndex — §M7 requires writing that test against
    /// *this* implementation first and watching it fail. Do not mistake it for
    /// a finished path.
    async fn get(
        &self,
        request: Request<pb::GetRequest>,
    ) -> Result<Response<pb::GetResponse>, Status> {
        match self.call(ClientOp::Get { key: request.into_inner().key }).await? {
            ClientReply::Value(value) => {
                Ok(Response::new(pb::GetResponse { value, not_leader: None }))
            }
            other => Err(Status::internal(format!("driver answered a get with {other:?}"))),
        }
    }

    async fn put(
        &self,
        request: Request<pb::PutRequest>,
    ) -> Result<Response<pb::PutResponse>, Status> {
        let req = request.into_inner();
        match self.call(ClientOp::Put { key: req.key, value: req.value }).await? {
            ClientReply::Applied => Ok(Response::new(pb::PutResponse { not_leader: None })),
            ClientReply::NotLeader { hint } => {
                Ok(Response::new(pb::PutResponse { not_leader: Some(not_leader(hint)) }))
            }
            other => Err(Status::internal(format!("driver answered a put with {other:?}"))),
        }
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteRequest>,
    ) -> Result<Response<pb::DeleteResponse>, Status> {
        match self.call(ClientOp::Delete { key: request.into_inner().key }).await? {
            ClientReply::Applied => Ok(Response::new(pb::DeleteResponse { not_leader: None })),
            ClientReply::NotLeader { hint } => {
                Ok(Response::new(pb::DeleteResponse { not_leader: Some(not_leader(hint)) }))
            }
            other => Err(Status::internal(format!("driver answered a delete with {other:?}"))),
        }
    }

    /// Compare-and-swap needs the session table to stay idempotent under
    /// retry, which is M7. Refused rather than half-implemented: a `Cas` that
    /// double-applies on retry is worse than one that is absent.
    async fn cas(
        &self,
        _request: Request<pb::CasRequest>,
    ) -> Result<Response<pb::CasResponse>, Status> {
        Err(Status::unimplemented("Cas arrives at M7, with the session table"))
    }
}
