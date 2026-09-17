//! The smart client (M6): find the leader, follow its hints, retry.
//!
//! Only the leader accepts a write, and the client does not know who that is —
//! so every write is a small search. `NotLeader { leader_hint }` turns it from
//! a scan into a single redirect in the common case, and the cached leader
//! makes the *next* write start in the right place.
//!
//! The retry budget is bounded on purpose. A cluster with no quorum has no
//! leader to find, and a client that waits forever for one is
//! indistinguishable from a hung client.

pub mod cli;

use std::collections::BTreeMap;
use std::time::Duration;

use kv_proto::kv::kv_service_client::KvServiceClient;
use kv_proto::kv::{CasRequest, ClientContext, DeleteRequest, GetRequest, PutRequest};
use tonic::transport::Channel;

pub type NodeId = u64;

const RETRY_ROUNDS: usize = 40;
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
    /// The last node that accepted a write. Also what M6's end-to-end gate
    /// uses to decide which process to `kill -9`.
    leader: Option<NodeId>,
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
            client_id: rand::random(),
            sequence: 0,
        }
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The order to try nodes in: the cached leader first, then everyone else.
    fn candidates(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.endpoints.keys().copied().collect();
        if let Some(leader) = self.leader {
            ids.retain(|id| *id != leader);
            ids.insert(0, leader);
        }
        ids
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
            let order = match hinted.take() {
                Some(id) if self.endpoints.contains_key(&id) => vec![id],
                _ => self.candidates(),
            };

            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                match KvServiceClient::new(channel).get(GetRequest { key: key.to_vec() }).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        match resp.not_leader {
                            None => {
                                self.leader = Some(id);
                                return Ok(resp.value);
                            }
                            Some(not_leader) => {
                                self.leader = None;
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

        // One context for the whole request, reused by every attempt below.
        // Allocating it per attempt would make each retry a new request to the
        // state machine, which is exactly the double-apply the session table
        // exists to prevent.
        self.sequence += 1;
        let ctx = Some(ClientContext { client_id: self.client_id, sequence_number: self.sequence });

        for _ in 0..RETRY_ROUNDS {
            // Follow a hint straight to the named node; otherwise scan.
            let order = match hinted.take() {
                Some(id) if self.endpoints.contains_key(&id) => vec![id],
                _ => self.candidates(),
            };

            for id in order {
                let Some(channel) = self.channel(id).await else { continue };
                let mut client = KvServiceClient::new(channel);

                let outcome = match &op {
                    Write::Put { key, value } => client
                        .put(PutRequest { ctx, key: key.clone(), value: value.clone() })
                        .await
                        .map(|r| (r.into_inner().not_leader, true)),
                    Write::Delete { key } => client
                        .delete(DeleteRequest { ctx, key: key.clone() })
                        .await
                        .map(|r| (r.into_inner().not_leader, true)),
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
                            (r.not_leader, r.swapped)
                        }),
                };

                match outcome {
                    Ok((None, swapped)) => {
                        self.leader = Some(id);
                        return Ok(swapped);
                    }
                    // Refused as a non-leader. A hint of 0 means that node
                    // knows of no leader either, so fall back to scanning
                    // rather than chasing a hint to nowhere.
                    Ok((Some(not_leader), _)) => {
                        self.leader = None;
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
