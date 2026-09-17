//! The cluster-administration gRPC service (M9.2).
//!
//! Same shape as `kv_service`: every handler turns its request into an
//! `AdminRequest`, hands it to the driver and waits on a `oneshot`. Membership
//! changes are log entries, so they go through the one task that owns the Raft
//! node rather than reaching into it from a handler.

// Every fallible call here returns `tonic::Status`, which is ~176 bytes and so
// trips `result_large_err`. Boxing it would mean unwrapping at every tonic
// boundary for no benefit; the rest of the shell carries the same allow.
#![allow(clippy::result_large_err)]

use kv_proto::admin as pb;
use kv_proto::admin::admin_service_server::AdminService;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status};

use crate::driver::{AdminOp, AdminReply, AdminRequest, ClientOp, ClientReply, ClientRequest};
use crate::meta::PublishedMap;
use crate::shard_map::SHARD_MAP_KEY;
use crate::transport::group;

pub struct AdminApi {
    /// The **meta** group's admin channel. Since M11 the meta group is the
    /// cluster's membership authority: there is no single data group to admit
    /// a node to, and a node admitted to the meta group replicates the shard
    /// map, which is what lets it route at all. Shards follow when M12's
    /// migration driver moves them.
    requests: mpsc::Sender<AdminRequest>,
    /// The **meta** group's request channel, for a linearizable map read. A
    /// map read is an ordinary `Get` through ReadIndex; it only looks unusual
    /// because the key is a reserved one no client may name.
    meta_requests: mpsc::Sender<ClientRequest>,
    /// The **shard** driver's admin channel, for what this node is carrying.
    /// A different driver from `requests`: the meta group owns membership and
    /// placement, the shard driver owns the data.
    shard_admin: mpsc::Sender<AdminRequest>,
    /// What this node's reconciler last published. The cheap read.
    published: PublishedMap,
}

impl AdminApi {
    pub fn new(
        requests: mpsc::Sender<AdminRequest>,
        meta_requests: mpsc::Sender<ClientRequest>,
        shard_admin: mpsc::Sender<AdminRequest>,
        published: PublishedMap,
    ) -> Self {
        Self { requests, meta_requests, shard_admin, published }
    }

    /// What this node is carrying, shard by shard.
    ///
    /// A failure here degrades the answer rather than failing it: an operator
    /// reaches for `ClusterStatus` when something is already wrong, and
    /// refusing the whole reply because one driver is wedged removes the tool
    /// mid-diagnosis.
    async fn shard_statuses(&self) -> Vec<crate::driver::ShardStatus> {
        let (reply, wait) = oneshot::channel();
        let request = AdminRequest { group: group::UNSET, op: AdminOp::ShardStatuses, reply };
        if self.shard_admin.try_send(request).is_err() {
            return Vec::new();
        }
        match wait.await {
            Ok(AdminReply::ShardStatuses(statuses)) => statuses,
            _ => Vec::new(),
        }
    }

    /// Reads the map through the meta group's ReadIndex.
    async fn read_map_linearizably(&self) -> Result<ClientReply, Status> {
        let (reply, wait) = oneshot::channel();
        self.meta_requests
            .try_send(ClientRequest {
                group: group::META,
                op: ClientOp::Get { key: SHARD_MAP_KEY.to_vec() },
                reply,
            })
            .map_err(|_| Status::resource_exhausted("meta request queue full"))?;
        wait.await.map_err(|_| Status::unavailable("node is shutting down"))
    }

    async fn call(&self, op: AdminOp) -> Result<AdminReply, Status> {
        let (reply, wait) = oneshot::channel();
        self.requests
            .try_send(AdminRequest { group: group::META, op, reply })
            .map_err(|_| Status::resource_exhausted("admin request queue full"))?;
        wait.await.map_err(|_| Status::unavailable("node is shutting down"))
    }
}

fn not_leader(hint: Option<u64>) -> pb::NotLeader {
    pb::NotLeader { leader_hint: hint.unwrap_or(0) }
}

/// A refusal is a `failed_precondition`, not a redirect: removing the last
/// voter or adding a node twice is wrong wherever it is sent, so an admin tool
/// must stop rather than try the next node.
fn rejected(reason: String) -> Status {
    Status::failed_precondition(reason)
}

#[tonic::async_trait]
impl AdminService for AdminApi {
    /// Admits a node as a **learner**. It replicates but does not vote until
    /// the leader promotes it, which happens on its own once it has caught up
    /// — so a success here means "admitted", not "voting".
    async fn add_node(
        &self,
        request: Request<pb::AddNodeRequest>,
    ) -> Result<Response<pb::AddNodeResponse>, Status> {
        let pb::AddNodeRequest { node_id, address } = request.into_inner();
        if address.is_empty() {
            return Err(Status::invalid_argument("a node needs an address to be reachable"));
        }
        match self.call(AdminOp::AddNode { id: node_id, address }).await? {
            AdminReply::Accepted { index } => {
                Ok(Response::new(pb::AddNodeResponse { not_leader: None, index }))
            }
            AdminReply::NotLeader { hint } => Ok(Response::new(pb::AddNodeResponse {
                not_leader: Some(not_leader(hint)),
                index: 0,
            })),
            AdminReply::Rejected { reason } => Err(rejected(reason)),
            AdminReply::Status(_) | AdminReply::ShardMap(_) | AdminReply::ShardStatuses(_) => {
                Err(Status::internal("driver answered the wrong request"))
            }
        }
    }

    async fn remove_node(
        &self,
        request: Request<pb::RemoveNodeRequest>,
    ) -> Result<Response<pb::RemoveNodeResponse>, Status> {
        let pb::RemoveNodeRequest { node_id } = request.into_inner();
        match self.call(AdminOp::RemoveNode { id: node_id }).await? {
            AdminReply::Accepted { index } => {
                Ok(Response::new(pb::RemoveNodeResponse { not_leader: None, index }))
            }
            AdminReply::NotLeader { hint } => Ok(Response::new(pb::RemoveNodeResponse {
                not_leader: Some(not_leader(hint)),
                index: 0,
            })),
            AdminReply::Rejected { reason } => Err(rejected(reason)),
            AdminReply::Status(_) | AdminReply::ShardMap(_) | AdminReply::ShardStatuses(_) => {
                Err(Status::internal("driver answered the wrong request"))
            }
        }
    }

    /// Answerable by any node, leader or not: an operator asking "what does
    /// this replica think the cluster is" is exactly how a split view gets
    /// diagnosed, so refusing on a follower would remove the tool that finds
    /// the problem.
    async fn cluster_status(
        &self,
        _request: Request<pb::ClusterStatusRequest>,
    ) -> Result<Response<pb::ClusterStatusResponse>, Status> {
        let AdminReply::Status(status) = self.call(AdminOp::Status).await? else {
            return Err(Status::internal("driver answered the wrong request"));
        };
        let members = status
            .voters
            .iter()
            .map(|id| (*id, true))
            .chain(status.learners.iter().map(|id| (*id, false)))
            .map(|(node_id, voter)| pb::Member {
                node_id,
                address: status.endpoints.get(&node_id).cloned().unwrap_or_default(),
                voter,
            })
            .collect();
        Ok(Response::new(pb::ClusterStatusResponse {
            shards: self
                .shard_statuses()
                .await
                .into_iter()
                .map(|s| pb::ShardStatus {
                    shard_id: s.shard as u32,
                    leader_id: s.leader.unwrap_or(0),
                    replicas: s.replicas,
                    term: s.term,
                    leading: s.leading,
                    applied_index: s.applied_index,
                })
                .collect(),
            node_id: status.id,
            leader_id: status.leader.unwrap_or(0),
            term: status.term,
            members,
            commit_index: status.commit_index,
            applied_index: status.applied_index,
            log_first_index: status.log_first_index,
            log_last_index: status.log_last_index,
        }))
    }

    /// The placement map, either as this replica has it or as the meta group
    /// can confirm it.
    ///
    /// The two are genuinely different questions. The published copy costs
    /// nothing and any replica can answer it, which is what makes it usable on
    /// the request path. The linearizable one costs a quorum round trip and
    /// only the meta leader can answer it — and a deposed leader cannot, which
    /// is the point.
    async fn get_shard_map(
        &self,
        request: Request<pb::GetShardMapRequest>,
    ) -> Result<Response<pb::GetShardMapResponse>, Status> {
        let map = if request.into_inner().linearizable {
            match self.read_map_linearizably().await? {
                ClientReply::Value(Some(bytes)) => Some(
                    kv_ring::ShardMap::decode(&bytes)
                        .map_err(|e| Status::internal(format!("stored shard map: {e}")))?,
                ),
                // The meta group has committed no map yet.
                ClientReply::Value(None) => None,
                ClientReply::NotLeader { hint } => {
                    return Ok(Response::new(pb::GetShardMapResponse {
                        not_leader: Some(not_leader(hint)),
                        ..Default::default()
                    }));
                }
                other => {
                    return Err(Status::internal(format!("meta driver answered with {other:?}")));
                }
            }
        } else {
            self.published.load_full().map(|map| (*map).clone())
        };

        let Some(map) = map else {
            // No map yet: a cluster is only in this state between starting and
            // its meta group's first election. Version 0 says so without
            // inventing a placement.
            return Ok(Response::new(pb::GetShardMapResponse::default()));
        };

        Ok(Response::new(pb::GetShardMapResponse {
            not_leader: None,
            version: map.version,
            num_shards: map.num_shards as u32,
            replication_factor: map.replication_factor as u32,
            nodes: map.nodes.iter().copied().collect(),
            shards: map
                .shards
                .iter()
                .map(|replicas| pb::ShardReplicas { replicas: replicas.clone() })
                .collect(),
        }))
    }

    /// M12. Rebalancing needs the ring (M10) and Merkle verification, neither
    /// of which exists yet; answering anything but "unimplemented" would be a
    /// lie an operator could act on.
    async fn rebalance(
        &self,
        _request: Request<pb::RebalanceRequest>,
    ) -> Result<Response<pb::RebalanceResponse>, Status> {
        Err(Status::unimplemented("rebalancing arrives with M12"))
    }

    /// M12, for the same reason as `rebalance`.
    async fn verify_shard(
        &self,
        _request: Request<pb::VerifyShardRequest>,
    ) -> Result<Response<pb::VerifyShardResponse>, Status> {
        Err(Status::unimplemented("shard verification arrives with M12"))
    }
}
