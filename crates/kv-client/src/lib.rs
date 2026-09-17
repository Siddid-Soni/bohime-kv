//! The smart client (M6, shard-aware since M11.7): find the shard, find its
//! leader, follow hints, retry.
//!
//! Only the leader of the key's *shard* accepts a write, and the client knows
//! neither at the start — so every write is a small search. Two hints narrow
//! it: `NotHosted { replicas }` says which nodes hold the shard, and
//! `NotLeader { leader_hint }` says which of them leads. Both are ordinary
//! answers rather than errors, so following one costs no reconnection.
//!
//! The leader cache is **per shard**. One cache would be worse than none once
//! a node hosts many groups: a failover on shard 3 would throw away what the
//! client knew about the other 255.
//!
//! The retry budget is bounded on purpose. A cluster with no quorum has no
//! leader to find, and a client that waits forever for one is
//! indistinguishable from a hung client.

pub mod cli;

use std::collections::BTreeMap;
use std::time::Duration;

use kv_proto::admin::admin_service_client::AdminServiceClient;
use kv_proto::admin::{ClusterStatusRequest, GetShardMapRequest};
use kv_proto::kv::kv_service_client::KvServiceClient;
use kv_proto::kv::{CasRequest, ClientContext, DeleteRequest, GetRequest, NotHosted, PutRequest};
use tonic::transport::Channel;

pub type NodeId = u64;
pub type ShardId = u16;

/// The placement a client routes by: the shard map, reduced to what routing
/// needs.
///
/// Not a `kv_ring::ShardMap`, because the wire form carries no
/// `vnodes_per_node` — the ring is the *server's* business, and a client that
/// reconstructed one could compute a placement the cluster never agreed to.
/// Key→shard is shared with the server through `kv_ring::shard_for`, which is
/// the one part that must agree exactly.
#[derive(Debug, Clone)]
struct Placement {
    version: u64,
    num_shards: u16,
    /// `shards[i]` is shard `i`'s replica set, in ring order.
    shards: Vec<Vec<NodeId>>,
}

impl Placement {
    fn shard_for(&self, key: &[u8]) -> ShardId {
        kv_ring::shard_for(key, self.num_shards)
    }

    fn replicas(&self, shard: ShardId) -> &[NodeId] {
        self.shards.get(shard as usize).map(Vec::as_slice).unwrap_or_default()
    }
}

/// Rounds of the whole candidate list before giving up.
///
/// Raised from 40 at M11.7, and the reason is the cold-start path rather than
/// impatience about failures. A single-group cluster became writable as soon
/// as one election finished, in a few hundred milliseconds. A sharded one has
/// to elect a meta leader, publish a placement, found the shard groups that
/// placement gives it, and *then* elect within the shard the key belongs to. A
/// client that gave up inside two seconds reported a cluster that was starting
/// normally as unreachable — which is the wrong answer, and the one an
/// operator sees on their first write after bringing a cluster up.
const RETRY_ROUNDS: usize = 120;
const RETRY_PAUSE: Duration = Duration::from_millis(50);
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// One variant, because the client's contract is "retry until a node accepts
/// this, or give up". Having tried every node, a single peer's `tonic::Status`
/// is not something a caller can act on — but it is what they want when
/// debugging, so the last one rides along as text. Keeping it out of the enum
/// as a `Status` also keeps `ClientError` small: it is the error half of every
/// `get`, and a 176-byte variant would be paid on the success path too.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no reachable node could serve the request{}", .last.as_ref().map(|e| format!("; last error: {e}")).unwrap_or_default())]
    NoReachableNode { last: Option<String> },
}

/// A write, held as data so the retry loop can reissue it against a different
/// node without the caller re-supplying it.
enum Write {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Cas { key: Vec<u8>, expected: Option<Vec<u8>>, new_value: Vec<u8> },
}

pub struct Client {
    endpoints: BTreeMap<NodeId, String>,
    /// Connected channels, kept so a retry does not redial. tonic's `Channel`
    /// is cheap to clone and reconnects on its own.
    channels: BTreeMap<NodeId, Channel>,
    /// The last node that accepted anything. Also what M6's end-to-end gate
    /// uses to decide which process to `kill -9`.
    leader: Option<NodeId>,
    /// The placement to route by, learned from any node and refreshed when one
    /// says the client's copy is wrong. `None` until then, in which case the
    /// client scans — which is also what it does against a cluster whose meta
    /// group has not elected yet.
    placement: Option<Placement>,
    /// Who last accepted a request for each shard. Per shard, not per cluster:
    /// see the module header.
    leaders: BTreeMap<ShardId, NodeId>,
    /// This client's identity for the session table (§1.8). Random rather
    /// than assigned, because there is no registration step yet; a collision
    /// would need two clients to draw the same u64.
    client_id: u64,
    /// Monotonic per request. The same value is reused for every retry of one
    /// request — that is the entire mechanism: a retry that picked a fresh
    /// number would be a new request and would apply a second time.
    sequence: u64,
}

impl Client {
    pub fn new(endpoints: Vec<(NodeId, String)>) -> Self {
        Self {
            endpoints: endpoints.into_iter().collect(),
            channels: BTreeMap::new(),
            leader: None,
            placement: None,
            leaders: BTreeMap::new(),
            client_id: rand::random(),
            sequence: 0,
        }
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The order to try nodes in for `key`: the shard's cached leader first,
    /// then the shard's other replicas, then everyone else as a fallback.
    ///
    /// The fallback matters: a client with no placement, or one whose
    /// placement is wrong about this shard, still has to be able to reach
    /// *somebody* who can tell it so.
    fn candidates(&self, key: &[u8]) -> Vec<NodeId> {
        let mut order: Vec<NodeId> = Vec::new();
        let shard = self.placement.as_ref().map(|p| p.shard_for(key));

        if let Some(shard) = shard
            && let Some(&leader) = self.leaders.get(&shard)
            && self.endpoints.contains_key(&leader)
        {
            order.push(leader);
        }
        if let (Some(shard), Some(placement)) = (shard, self.placement.as_ref()) {
            for &id in placement.replicas(shard) {
                if !order.contains(&id) && self.endpoints.contains_key(&id) {
                    order.push(id);
                }
            }
        }
        if let Some(leader) = self.leader
            && !order.contains(&leader)
            && shard.is_none()
        {
            order.push(leader);
        }
        for &id in self.endpoints.keys() {
            if !order.contains(&id) {
                order.push(id);
            }
        }
        order
    }

    /// The shard `key` belongs to, once the client has a placement.
    fn shard_of(&self, key: &[u8]) -> Option<ShardId> {
        self.placement.as_ref().map(|p| p.shard_for(key))
    }

    /// Notes that `id` accepted a request for `key`.
    fn accepted(&mut self, id: NodeId, key: &[u8]) {
        self.leader = Some(id);
        if let Some(shard) = self.shard_of(key) {
            self.leaders.insert(shard, id);
        }
    }

    /// Notes that `id` refused a request for `key`, so nothing it said about
    /// leadership holds any more.
    fn refused(&mut self, key: &[u8]) {
        self.leader = None;
        if let Some(shard) = self.shard_of(key) {
            self.leaders.remove(&shard);
        }
    }

    /// Fetches the placement from whichever node answers first.
    ///
    /// The **published** copy, not the linearizable one: routing by a map one
    /// version old costs a redirect, and paying a quorum round trip per
    /// refresh to avoid that redirect is a worse trade. Addresses come along
    /// from `ClusterStatus`, because the map names node ids and a client
    /// admitted to no `--endpoints` list cannot dial an id.
    async fn refresh_placement(&mut self) {
        let ids: Vec<NodeId> = self.endpoints.keys().copied().collect();
        for id in ids {
            let Some(channel) = self.channel(id).await else { continue };
            let mut admin = AdminServiceClient::new(channel);

            let Ok(resp) = admin.get_shard_map(GetShardMapRequest { linearizable: false }).await
            else {
                self.forget(id);
                continue;
            };
            let resp = resp.into_inner();
            // Version 0 is "no map yet": a cluster is only in that state
            // between starting and its meta group's first election.
            if resp.version == 0 || resp.num_shards == 0 {
                continue;
            }
            let fresh = self.placement.as_ref().is_none_or(|p| resp.version > p.version);
            if fresh {
                self.placement = Some(Placement {
                    version: resp.version,
                    num_shards: resp.num_shards as u16,
                    shards: resp.shards.into_iter().map(|s| s.replicas).collect(),
                });
                // A placement change means every cached leader is a guess
                // about a group that may no longer exist here.
                self.leaders.clear();
            }

            // Addresses for any node the map names that argv did not.
            if let Ok(status) = admin.cluster_status(ClusterStatusRequest {}).await {
                for member in status.into_inner().members {
                    if !member.address.is_empty() {
                        self.endpoints.entry(member.node_id).or_insert(member.address);
                    }
                }
            }
            return;
        }
    }

    /// Reacts to a `NotHosted`: the shard is somewhere else, and the answer
    /// says where. Returns the node to try next, if any.
    fn redirect(&mut self, key: &[u8], not_hosted: NotHosted) -> Option<NodeId> {
        self.refused(key);
        // The node's own map may be the stale one, in which case its replica
        // list is worth no more than ours — but a redirect that goes nowhere
        // costs one attempt, and staying put costs the whole budget.
        let stale = self
            .placement
            .as_ref()
            .is_none_or(|p| not_hosted.map_version > p.version || not_hosted.map_version == 0);
        if stale {
            self.placement = None;
        }
        not_hosted.replicas.into_iter().find(|id| self.endpoints.contains_key(id))
    }

    async fn channel(&mut self, id: NodeId) -> Option<Channel> {
        if let Some(c) = self.channels.get(&id) {
            return Some(c.clone());
        }
        let endpoint = self.endpoints.get(&id)?;
        let channel = Channel::from_shared(endpoint.clone())
            .ok()?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect()
            .await
            .ok()?;
        self.channels.insert(id, channel.clone());
        Some(channel)
    }

    /// Drops a channel that failed, so the next attempt redials rather than
    /// reusing a socket to a process that is gone.
    fn forget(&mut self, id: NodeId) {
        self.channels.remove(&id);
        if self.leader == Some(id) {
            self.leader = None;
        }
    }

    /// Reads go to the leader and follow `NotLeader` hints, exactly as writes
    /// do. Before M7 any node answered a read from local state; that is what
    /// made the read non-linearizable, so the cheaper path is gone on purpose.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        let mut last = None;
        let mut hinted: Option<NodeId> = None;

        for _ in 0..RETRY_ROUNDS {
            if self.placement.is_none() {
                self.refresh_placement().await;
            }
            let order = match hinted.take() {
                Some(id) if self.endpoints.contains_key(&id) => vec![id],
                _ => self.candidates(key),
            };

            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                match KvServiceClient::new(channel).get(GetRequest { key: key.to_vec() }).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        // Checked before `not_leader`: a node that does not
                        // hold the shard has no opinion about who leads it.
                        if let Some(not_hosted) = resp.not_hosted {
                            hinted = self.redirect(key, not_hosted);
                            break;
                        }
                        match resp.not_leader {
                            None => {
                                self.accepted(id, key);
                                return Ok(resp.value);
                            }
                            Some(not_leader) => {
                                self.refused(key);
                                if not_leader.leader_hint != 0
                                    && self.endpoints.contains_key(&not_leader.leader_hint)
                                {
                                    hinted = Some(not_leader.leader_hint);
                                    break;
                                }
                            }
                        }
                    }
                    Err(status) => {
                        last = Some(status.to_string());
                        self.forget(id);
                    }
                }
            }
            tokio::time::sleep(RETRY_PAUSE).await;
        }
        Err(ClientError::NoReachableNode { last })
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        self.write(Write::Put { key: key.to_vec(), value: value.to_vec() }).await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        self.write(Write::Delete { key: key.to_vec() }).await?;
        Ok(())
    }

    /// Compare-and-swap. `expected: None` means "only if absent". Returns
    /// whether the swap took effect.
    ///
    /// This is the operation the session table exists for. Every retry inside
    /// carries the same context, so a reply lost after the entry committed
    /// yields the original answer rather than a second evaluation — which
    /// would see the value it already swapped in and wrongly report `false`.
    pub async fn cas(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<bool, ClientError> {
        self.write(Write::Cas {
            key: key.to_vec(),
            expected: expected.map(|e| e.to_vec()),
            new_value: new_value.to_vec(),
        })
        .await
    }

    /// One write, retried until a node accepts it or the budget runs out.
    ///
    /// Returns the `swapped` flag for a `Cas`; `true` for the others, which
    /// have no such answer.
    async fn write(&mut self, op: Write) -> Result<bool, ClientError> {
        let mut hinted: Option<NodeId> = None;
        let mut last = None;
        let key: Vec<u8> = match &op {
            Write::Put { key, .. } | Write::Delete { key } | Write::Cas { key, .. } => key.clone(),
        };

        // One context for the whole request, reused by every attempt below.
        // Allocating it per attempt would make each retry a new request to the
        // state machine, which is exactly the double-apply the session table
        // exists to prevent.
        self.sequence += 1;
        let ctx = Some(ClientContext { client_id: self.client_id, sequence_number: self.sequence });

        for _ in 0..RETRY_ROUNDS {
            if self.placement.is_none() {
                self.refresh_placement().await;
            }
            // Follow a hint straight to the named node; otherwise route.
            let order = match hinted.take() {
                Some(id) if self.endpoints.contains_key(&id) => vec![id],
                _ => self.candidates(&key),
            };

            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                let mut client = KvServiceClient::new(channel);

                let outcome = match &op {
                    Write::Put { key, value } => client
                        .put(PutRequest { ctx, key: key.clone(), value: value.clone() })
                        .await
                        .map(|r| {
                            let r = r.into_inner();
                            (r.not_hosted, r.not_leader, true)
                        }),
                    Write::Delete { key } => {
                        client.delete(DeleteRequest { ctx, key: key.clone() }).await.map(|r| {
                            let r = r.into_inner();
                            (r.not_hosted, r.not_leader, true)
                        })
                    }
                    Write::Cas { key, expected, new_value } => client
                        .cas(CasRequest {
                            ctx,
                            key: key.clone(),
                            expected: expected.clone(),
                            new_value: new_value.clone(),
                        })
                        .await
                        .map(|r| {
                            let r = r.into_inner();
                            (r.not_hosted, r.not_leader, r.swapped)
                        }),
                };

                match outcome {
                    // Wrong node for this shard. Checked first: a node that
                    // does not hold the shard has no opinion about who leads
                    // it, so its `not_leader` would be noise.
                    Ok((Some(not_hosted), _, _)) => {
                        hinted = self.redirect(&key, not_hosted);
                        break;
                    }
                    Ok((None, None, swapped)) => {
                        self.accepted(id, &key);
                        return Ok(swapped);
                    }
                    // Refused as a non-leader. A hint of 0 means that node
                    // knows of no leader either, so fall back to routing
                    // rather than chasing a hint to nowhere.
                    Ok((None, Some(not_leader), _)) => {
                        self.refused(&key);
                        if not_leader.leader_hint != 0
                            && self.endpoints.contains_key(&not_leader.leader_hint)
                        {
                            hinted = Some(not_leader.leader_hint);
                            break;
                        }
                    }
                    Err(status) => {
                        last = Some(status.to_string());
                        self.forget(id);
                    }
                }
            }
            tokio::time::sleep(RETRY_PAUSE).await;
        }
        Err(ClientError::NoReachableNode { last })
    }
}

#[cfg(test)]
mod tests;
