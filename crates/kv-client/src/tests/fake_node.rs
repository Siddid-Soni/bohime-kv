//! A cluster of fake nodes, for testing what the client's retry loop does
//! with an answer.
//!
//! Real `Driver`s are `kv-node`'s business; what is under test here is the
//! client's reaction to a *reply*, so the server side only has to be able to
//! produce one on demand. Each node serves one `Answer` and counts both the
//! requests it received and the **TCP connections it accepted** — the second
//! is the observable that matters for backpressure, because redialling a node
//! that was merely slow is the cost this harness exists to catch.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use kv_proto::admin::admin_service_server::{AdminService, AdminServiceServer};
use kv_proto::admin::{
    AddNodeRequest, AddNodeResponse, ClusterStatusRequest, ClusterStatusResponse,
    GetShardMapRequest, GetShardMapResponse, RebalanceRequest, RebalanceResponse,
    RemoveNodeRequest, RemoveNodeResponse, ShardReplicas, VerifyShardRequest, VerifyShardResponse,
};
use kv_proto::kv::kv_service_server::{KvService, KvServiceServer};
use kv_proto::kv::{
    CasRequest, CasResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse, NotLeader,
    PutRequest, PutResponse,
};
use tonic::{Request, Response, Status};

/// What one fake node does with a request.
#[derive(Clone, Copy)]
pub(crate) enum Answer {
    /// "not me, ask `hint`" — the ordinary redirect a follower gives.
    Hint(u64),
    /// "not me, and I do not know who" — no hint, which is what a node in the
    /// middle of an election says.
    Blind,
    Value,
    /// Healthy, reachable, and **slower than the client's deadline** for its
    /// first `calls` requests; answers normally after that. This is a
    /// saturated leader, not a broken one, and the distinction is the whole
    /// point: the connection is fine and so is the routing.
    Slow {
        calls: usize,
        delay: Duration,
    },
    /// `ResourceExhausted` for its first `calls` requests. What
    /// `kv_service.rs` sheds when the driver's request queue is full.
    Busy {
        calls: usize,
    },
    /// `Unavailable`, forever. A node that is going away — the one case where
    /// dropping the channel is the right answer.
    Dead,
}

pub(crate) struct Node {
    pub(crate) requests: Arc<AtomicUsize>,
    /// TCP connections accepted. One per `Client`, unless something made the
    /// client redial.
    pub(crate) accepts: Arc<AtomicUsize>,
}

struct FakeNode {
    answer: Answer,
    /// Every node serves the same map, so the client routes rather than
    /// falling back to "try everyone".
    replicas: Vec<u64>,
    requests: Arc<AtomicUsize>,
}

impl FakeNode {
    /// The reply for one request, and the `n`th request for the counted
    /// answers. Shared by `get` and `put` so the write path is exercised by
    /// the same fixtures as the read path.
    #[allow(clippy::result_large_err, reason = "the tonic signature this stands in for")]
    async fn decide(&self) -> Result<Option<NotLeader>, Status> {
        let n = self.requests.fetch_add(1, Ordering::Relaxed);
        match self.answer {
            Answer::Value => Ok(None),
            Answer::Hint(to) => Ok(Some(NotLeader { leader_hint: to })),
            Answer::Blind => Ok(Some(NotLeader { leader_hint: 0 })),
            Answer::Slow { calls, delay } => {
                if n < calls {
                    tokio::time::sleep(delay).await;
                }
                Ok(None)
            }
            Answer::Busy { calls } => {
                if n < calls {
                    Err(Status::resource_exhausted("client request queue full"))
                } else {
                    Ok(None)
                }
            }
            Answer::Dead => Err(Status::unavailable("node is going away")),
        }
    }
}

#[tonic::async_trait]
impl KvService for FakeNode {
    async fn get(&self, _: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let not_leader = self.decide().await?;
        Ok(Response::new(match not_leader {
            None => GetResponse { value: Some(b"v".to_vec()), ..Default::default() },
            Some(nl) => GetResponse { not_leader: Some(nl), ..Default::default() },
        }))
    }

    async fn put(&self, _: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let not_leader = self.decide().await?;
        Ok(Response::new(PutResponse { not_leader, ..Default::default() }))
    }

    async fn delete(&self, _: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let not_leader = self.decide().await?;
        Ok(Response::new(DeleteResponse { not_leader, ..Default::default() }))
    }

    async fn cas(&self, _: Request<CasRequest>) -> Result<Response<CasResponse>, Status> {
        let not_leader = self.decide().await?;
        Ok(Response::new(CasResponse { swapped: true, not_leader, ..Default::default() }))
    }
}

#[tonic::async_trait]
impl AdminService for FakeNode {
    async fn get_shard_map(
        &self,
        _: Request<GetShardMapRequest>,
    ) -> Result<Response<GetShardMapResponse>, Status> {
        Ok(Response::new(GetShardMapResponse {
            version: 1,
            num_shards: 1,
            replication_factor: self.replicas.len() as u32,
            nodes: self.replicas.clone(),
            shards: vec![ShardReplicas { replicas: self.replicas.clone() }],
            ..Default::default()
        }))
    }

    async fn cluster_status(
        &self,
        _: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        Ok(Response::new(ClusterStatusResponse::default()))
    }

    async fn add_node(
        &self,
        _: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn remove_node(
        &self,
        _: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn rebalance(
        &self,
        _: Request<RebalanceRequest>,
    ) -> Result<Response<RebalanceResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn verify_shard(
        &self,
        _: Request<VerifyShardRequest>,
    ) -> Result<Response<VerifyShardResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

/// Serves `answers[i]` as node `i + 1`, and hands back the endpoint list a
/// `Client` is built from plus each node's counters.
///
/// The listener is drained by our own accept loop rather than handed to tonic
/// directly, which is the only way to count connections: `serve_with_incoming`
/// takes a stream, so we feed it one and tally on the way past.
pub(crate) async fn cluster(answers: &[Answer]) -> (Vec<(u64, String)>, Vec<Node>) {
    let replicas: Vec<u64> = (1..=answers.len() as u64).collect();
    let mut endpoints = Vec::new();
    let mut nodes = Vec::new();
    for (i, &answer) in answers.iter().enumerate() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a free port");
        let addr: SocketAddr = listener.local_addr().expect("a bound address");
        let requests = Arc::new(AtomicUsize::new(0));
        let accepts = Arc::new(AtomicUsize::new(0));
        nodes.push(Node { requests: Arc::clone(&requests), accepts: Arc::clone(&accepts) });

        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn({
            let accepts = Arc::clone(&accepts);
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    accepts.fetch_add(1, Ordering::Relaxed);
                    if tx.send(Ok::<_, std::io::Error>(stream)).await.is_err() {
                        break;
                    }
                }
            }
        });

        let kv = FakeNode { answer, replicas: replicas.clone(), requests: Arc::clone(&requests) };
        let admin = FakeNode { answer, replicas: replicas.clone(), requests };
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(KvServiceServer::new(kv))
                .add_service(AdminServiceServer::new(admin))
                .serve_with_incoming(tokio_stream::wrappers::ReceiverStream::new(rx))
                .await;
        });
        endpoints.push((i as u64 + 1, format!("http://{addr}")));
    }
    (endpoints, nodes)
}
