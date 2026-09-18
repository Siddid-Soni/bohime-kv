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

use std::collections::{BTreeMap, BTreeSet};
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
/// What a round costs when it learned nothing.
///
/// Every node this client could reach refused and none of them named a
/// leader: there is nothing to act on, so the next round is a guess and
/// pacing it is the only thing that helps. A round that ends holding a
/// **hint** is the opposite — a node just answered and said where to go —
/// and it does not pay this. Sleeping on a hint turned every leader-cache
/// miss into 50 ms, and a fresh client has one of those per shard it
/// touches; see `tests::redirect`.
const RETRY_PAUSE: Duration = Duration::from_millis(50);
/// How many times `RETRY_PAUSE` may double while rounds keep coming back with
/// nothing to act on.
///
/// A node that sheds is a node that is already doing more than it can. Asking
/// it again on the same fixed interval is the client's contribution to the
/// overload, and with `RETRY_ROUNDS` of them it is a large one. Three
/// doublings caps a round at 400 ms, which is long enough to be out of the
/// way and short enough that a cluster recovering from a failover is noticed
/// promptly.
const BACKOFF_DOUBLINGS: u32 = 3;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
/// The default for `Client::with_request_timeout`.
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
    /// How long one attempt may take before the client stops waiting on it.
    ///
    /// Enforced here rather than on the `Endpoint`, and that is deliberate:
    /// `Endpoint::timeout` surfaces as `Code::Unknown` with the message
    /// "transport error", indistinguishable from a connection that actually
    /// broke. The client has to tell those apart — one of them means the node
    /// is busy and the other means it is gone — so it owns the clock.
    request_timeout: Duration,
}

/// The pacing state of one request's retry loop.
///
/// Separate from `Client` because it is per *request*, not per client: a hint
/// followed once is spent for this request and fresh for the next one.
#[derive(Default)]
struct Pacing {
    /// Hints already taken up during this request. A hint chased twice is a
    /// loop, not progress — two nodes pointing at each other is what a client
    /// sees for a moment after a failover.
    followed: BTreeSet<NodeId>,
    /// Consecutive rounds in which a node said it was too busy. Drives the
    /// backoff, and resets the moment anything useful arrives.
    ///
    /// Only *backpressure* counts here. A round in which nothing was reachable
    /// at all is the other failure, and it keeps the flat `RETRY_PAUSE`: a
    /// cluster that is down does not get less down if the client waits
    /// longer, and backing off there would only slow down saying so.
    busy_rounds: u32,
}

/// Why an attempt produced no answer, and what the client should keep.
///
/// The client used to have one arm for every `tonic::Status`, and it dropped
/// the channel **and** the per-shard leader cache. Under load that put every
/// client back into cold start — redial, re-search, arrive at the same
/// saturated node — so offered load rose exactly when the cluster could least
/// absorb it. See `tests::backpressure`.
enum Refusal {
    /// Reachable, and could not serve *this attempt*: it shed the request, or
    /// it did not answer inside the deadline. The connection is good and the
    /// node is very likely still the leader. Keep both; wait longer.
    Busy(String),
    /// The connection is no good. Drop it, so the next attempt redials rather
    /// than reusing a socket to a process that is gone.
    Gone(String),
}

impl Refusal {
    fn message(&self) -> &str {
        match self {
            Refusal::Busy(m) | Refusal::Gone(m) => m,
        }
    }
}

/// One attempt, bounded by the client's own clock.
///
/// The deadline is enforced here rather than by `Endpoint::timeout` because
/// tower's elapsed error reaches us as `Code::Unknown` with the message
/// "transport error" — the same thing a genuinely broken connection produces.
/// Telling a slow node from a dead one is the entire point, so the client
/// cannot afford to have those collapsed into one status by the layer below.
async fn attempt<T, F>(deadline: Duration, call: F) -> Result<T, Refusal>
where
    F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match tokio::time::timeout(deadline, call).await {
        Err(_) => Err(Refusal::Busy(format!("no answer within {deadline:?}"))),
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(match status.code() {
            // Backpressure, not failure: `kv_service.rs` sheds a full request
            // queue rather than awaiting it, precisely so that the gRPC layer
            // cannot pin unbounded memory behind a busy driver. A client that
            // reads that as "this node is broken" turns the one mechanism
            // protecting the server into the thing overwhelming it.
            tonic::Code::ResourceExhausted => Refusal::Busy(status.to_string()),
            _ => Refusal::Gone(status.to_string()),
        }),
    }
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
            request_timeout: REQUEST_TIMEOUT,
        }
    }

    /// How long one attempt may take. The default is `REQUEST_TIMEOUT`.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Whether a channel to `id` is still cached. Test-only: what it observes
    /// is that a failure was classified as fatal to the connection.
    #[cfg(test)]
    pub(crate) fn holds_channel(&self, id: NodeId) -> bool {
        self.channels.contains_key(&id)
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

    /// Paces the retry loop between rounds, and returns having done so.
    ///
    /// A hint is progress: some node answered and named the next one to ask,
    /// so the round found something out and the next one starts immediately.
    /// A hint we have **already followed for this request** is not progress —
    /// two nodes pointing at each other is what a client sees for a moment
    /// after a failover, and chasing that at loopback speed would burn the
    /// whole retry budget in the time one pause takes. So each node is worth
    /// one free hop per request, and after that the loop paces itself again.
    async fn pace(&self, hinted: Option<NodeId>, busy: bool, pacing: &mut Pacing) {
        // A hint this request has not taken up yet is progress: a node just
        // answered and said where to go. Sleeping before acting on it turned
        // every leader-cache miss into a `RETRY_PAUSE`, and a fresh client has
        // one of those per shard it touches. See `tests::redirect`.
        if let Some(id) = hinted
            && pacing.followed.insert(id)
        {
            pacing.busy_rounds = 0;
            return;
        }
        if !busy {
            pacing.busy_rounds = 0;
            tokio::time::sleep(RETRY_PAUSE).await;
            return;
        }
        let pause = RETRY_PAUSE * (1 << pacing.busy_rounds.min(BACKOFF_DOUBLINGS));
        pacing.busy_rounds += 1;
        tokio::time::sleep(pause).await;
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
        let mut pacing = Pacing::default();
        let deadline = self.request_timeout;

        for _ in 0..RETRY_ROUNDS {
            if self.placement.is_none() {
                self.refresh_placement().await;
            }
            let order = match hinted.take() {
                Some(id) if self.endpoints.contains_key(&id) => vec![id],
                _ => self.candidates(key),
            };

            let mut busy = false;
            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                let mut node = KvServiceClient::new(channel);
                let call = node.get(GetRequest { key: key.to_vec() });
                match attempt(deadline, call).await {
                    Ok(resp) => {
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
                    Err(refusal) => {
                        last = Some(refusal.message().to_string());
                        // Only a connection we cannot use is worth throwing
                        // away. A busy node keeps its channel and its place in
                        // the leader cache; the backoff in `pace` is what
                        // makes the next attempt cheaper for it, not a redial.
                        match refusal {
                            Refusal::Busy(_) => busy = true,
                            Refusal::Gone(_) => self.forget(id),
                        }
                    }
                }
            }
            self.pace(hinted, busy, &mut pacing).await;
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
        let mut pacing = Pacing::default();
        let deadline = self.request_timeout;

        for _ in 0..RETRY_ROUNDS {
            if self.placement.is_none() {
                self.refresh_placement().await;
            }
            // Follow a hint straight to the named node; otherwise route.
            let order = match hinted.take() {
                Some(id) if self.endpoints.contains_key(&id) => vec![id],
                _ => self.candidates(&key),
            };

            let mut busy = false;
            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                let mut client = KvServiceClient::new(channel);

                let outcome = match &op {
                    Write::Put { key, value } => {
                        let call =
                            client.put(PutRequest { ctx, key: key.clone(), value: value.clone() });
                        attempt(deadline, call).await.map(|r| (r.not_hosted, r.not_leader, true))
                    }
                    Write::Delete { key } => {
                        let call = client.delete(DeleteRequest { ctx, key: key.clone() });
                        attempt(deadline, call).await.map(|r| (r.not_hosted, r.not_leader, true))
                    }
                    Write::Cas { key, expected, new_value } => {
                        let call = client.cas(CasRequest {
                            ctx,
                            key: key.clone(),
                            expected: expected.clone(),
                            new_value: new_value.clone(),
                        });
                        attempt(deadline, call)
                            .await
                            .map(|r| (r.not_hosted, r.not_leader, r.swapped))
                    }
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
                    Err(refusal) => {
                        last = Some(refusal.message().to_string());
                        // See the same arm in `get`: a shed write means the
                        // driver is behind, not that the node is gone, and a
                        // write is the expensive half to re-search for.
                        match refusal {
                            Refusal::Busy(_) => busy = true,
                            Refusal::Gone(_) => self.forget(id),
                        }
                    }
                }
            }
            self.pace(hinted, busy, &mut pacing).await;
        }
        Err(ClientError::NoReachableNode { last })
    }
}

#[cfg(test)]
mod tests;
