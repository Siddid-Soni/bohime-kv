//! Outbound side of the transport (M5, task 3).
//!
//! `PeerClient::connect` never blocks on the peer being up — a node must come
//! up and start campaigning whether or not its peers exist yet, so connecting
//! is a background concern.
//!
//! The queue is bounded and `try_send` sheds on full. Dropping a Raft message
//! is always safe: the protocol assumes a lossy network and the sender retries
//! on its next heartbeat, and M4 proved the core survives 20% loss. Blocking
//! the driver loop on one slow peer is what is *not* safe — it would stall
//! every other peer and the node's own ticking. Never make this unbounded.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kv_proto::raft as pb;
use kv_proto::raft::raft_service_client::RaftServiceClient;
use kv_raft::{Message, NodeId};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use super::convert::{self, snapshot_chunks};
use super::group::GroupId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    #[error("outbound queue for this peer is full; message shed")]
    Full,
    #[error("peer client has shut down")]
    Closed,
}

#[derive(Debug, Clone, Copy)]
pub struct PeerConfig {
    pub queue_depth: usize,
    /// Per-RPC deadline. Without one, a peer that accepts a connection and
    /// then never answers holds the sender task forever and the queue backs
    /// up behind it.
    pub request_timeout: Duration,
    /// Deadline for one snapshot stream (M8). Snapshots carry the whole state
    /// and legitimately take longer than a heartbeat-sized deadline; bounding
    /// them separately keeps a huge state image from looking like a dead peer
    /// while keeping a dead peer from pinning the transfer forever.
    pub snapshot_timeout: Duration,
    /// Messages coalesced into one `Batch` RPC (M11.3).
    ///
    /// A ceiling rather than a target: the loop sends whatever is already
    /// queued, so a quiet link still sends one message per RPC and a busy one
    /// amortizes the round trip across the whole drain. Bounded because the
    /// batch is one gRPC message and AppendEntries carries log entries.
    pub max_batch: usize,
    /// Snapshot streams this peer may have open at once (M11.2).
    ///
    /// One flag per peer was right while a node hosted two groups; with a
    /// group per shard it would let shard 7's multi-second transfer block
    /// shard 8's indefinitely. A cap rather than no limit because every
    /// concurrent stream is a whole state image on the wire.
    pub max_snapshots_inflight: usize,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            queue_depth: 64,
            request_timeout: Duration::from_millis(500),
            snapshot_timeout: Duration::from_secs(30),
            max_batch: 256,
            max_snapshots_inflight: 2,
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_secs(5),
        }
    }
}

/// A connection to one peer: a bounded queue plus a task that drains it.
///
/// One per peer, carrying **every** group's traffic (M11.2). The group travels
/// with each message rather than with the link, because a node hosting a Raft
/// group per shard would otherwise open one connection, one queue and one
/// reconnect loop per shard per peer.
pub struct PeerClient {
    tx: mpsc::Sender<(GroupId, Message)>,
    /// Connection attempts made. Exposed because "it backs off rather than
    /// spinning" is not observable without counting — a busy loop and a
    /// correctly backing-off client look identical from the outside. The
    /// sender task keeps its own `Arc`; this copy exists only so M5's gate can
    /// read it, hence `cfg(test)`.
    #[cfg(test)]
    attempts: Arc<AtomicU64>,
    #[cfg(test)]
    config: PeerConfig,
}

impl PeerClient {
    /// Starts the sender task. Returns immediately whether or not `addr` is up.
    ///
    /// Replies from this peer are pushed to `replies` tagged with `peer`, since
    /// a response carries no sender id of its own — we know who answered
    /// because we know who we called.
    pub fn connect(
        peer: NodeId,
        addr: String,
        config: PeerConfig,
        replies: mpsc::Sender<(GroupId, NodeId, Message)>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_depth);
        let attempts = Arc::new(AtomicU64::new(0));
        tokio::spawn(run(peer, addr, config, rx, replies, Arc::clone(&attempts)));
        Self {
            tx,
            #[cfg(test)]
            attempts,
            #[cfg(test)]
            config,
        }
    }

    /// Queues `msg` for `group`, shedding it if the queue is full. Never
    /// blocks.
    pub fn try_send(&self, group: GroupId, msg: Message) -> Result<(), SendError> {
        self.tx.try_send((group, msg)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SendError::Full,
            mpsc::error::TrySendError::Closed(_) => SendError::Closed,
        })
    }

    /// Messages waiting to be sent. Never exceeds `queue_depth`.
    #[cfg(test)]
    pub fn queued(&self) -> usize {
        self.config.queue_depth - self.tx.capacity()
    }

    #[cfg(test)]
    pub fn connect_attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }
}

/// Drains the queue, reconnecting as needed.
async fn run(
    peer: NodeId,
    addr: String,
    config: PeerConfig,
    mut rx: mpsc::Receiver<(GroupId, Message)>,
    replies: mpsc::Sender<(GroupId, NodeId, Message)>,
    attempts: Arc<AtomicU64>,
) {
    let mut client: Option<RaftServiceClient<Channel>> = None;
    let mut backoff = config.backoff_initial;
    // A snapshot stream holds its RPC open for seconds, and heartbeats must
    // keep flowing to this peer meanwhile — a follower whose election timer
    // fires mid-transfer campaigns on a stale log, forces a term jump, and
    // deposes the leader that was rescuing it. So the transfer runs beside the
    // loop, not inside it.
    //
    // Keyed by group, and capped: one transfer per group at a time (a second
    // would send the same image twice), and only so many across all groups at
    // once (each is a whole state image on the wire). Anything shed here is
    // re-driven by the next heartbeat.
    let snapshots: Arc<Mutex<BTreeSet<GroupId>>> = Arc::new(Mutex::new(BTreeSet::new()));

    while let Some(first) = rx.recv().await {
        // Whatever else is already queued rides along. `try_recv` rather than
        // a timer: waiting to fill a batch would add latency to the quiet
        // case, and a busy link has its next messages queued already.
        let mut batch = vec![first];
        while batch.len() < config.max_batch {
            match rx.try_recv() {
                Ok(next) => batch.push(next),
                Err(_) => break,
            }
        }

        if client.is_none() {
            attempts.fetch_add(1, Ordering::Relaxed);
            match dial(&addr, config.request_timeout).await {
                Ok(c) => {
                    client = Some(c);
                    backoff = config.backoff_initial;
                }
                Err(_) => {
                    // Shed this batch and wait before trying again. Sleeping
                    // here is what turns a dead peer into a slow trickle of
                    // attempts instead of a spin: the queue keeps filling and
                    // shedding meanwhile, which is the intended behaviour.
                    tokio::time::sleep(jittered(backoff, peer, &attempts)).await;
                    backoff = (backoff * 2).min(config.backoff_max);
                    continue;
                }
            }
        }

        // Snapshots leave the batch: each is a whole state image and travels
        // on its own streaming RPC, beside this loop rather than inside it.
        let mut envelopes = Vec::with_capacity(batch.len());
        for (group, msg) in batch {
            if !matches!(msg, Message::InstallSnapshot { .. }) {
                envelopes.push(convert::envelope(group, msg));
                continue;
            }
            let admitted = {
                let mut inflight = snapshots.lock().expect("snapshot set is never poisoned");
                inflight.len() < config.max_snapshots_inflight && inflight.insert(group)
            };
            if !admitted {
                continue;
            }
            let Some(c) = client.clone() else {
                snapshots.lock().expect("snapshot set is never poisoned").remove(&group);
                continue;
            };
            let replies = replies.clone();
            let inflight = Arc::clone(&snapshots);
            tokio::spawn(async move {
                if let Ok(Some(reply)) = send_snapshot(c, msg, group, config.snapshot_timeout).await
                {
                    let _ = replies.try_send((group, peer, reply));
                }
                inflight.lock().expect("snapshot set is never poisoned").remove(&group);
            });
        }

        if envelopes.is_empty() {
            continue;
        }

        let Some(c) = client.as_mut() else { continue };
        match send_batch(c, envelopes, config.request_timeout).await {
            Ok(answers) => {
                for (group, reply) in answers {
                    // A full reply queue is shed like any other message. The
                    // group rides along: a response carries no group of its
                    // own, so the only record of which group asked is here.
                    let _ = replies.try_send((group, peer, reply));
                }
            }
            Err(_) => {
                // Drop the connection so the next batch redials.
                client = None;
            }
        }
    }
}

async fn dial(
    addr: &str,
    timeout: Duration,
) -> Result<RaftServiceClient<Channel>, tonic::transport::Error> {
    let endpoint =
        Endpoint::from_shared(addr.to_string())?.connect_timeout(timeout).timeout(timeout);
    Ok(RaftServiceClient::new(endpoint.connect().await?))
}

/// Backoff with jitter, so peers that all lost the same server do not retry in
/// lockstep. Derived from the peer id and attempt count rather than a random
/// source: this crate has no seeded RNG and a thread_rng here would be one
/// more thing to rule out when a test is flaky.
fn jittered(base: Duration, peer: NodeId, attempts: &AtomicU64) -> Duration {
    let n = attempts.load(Ordering::Relaxed);
    let spread = base.as_millis() as u64 / 4;
    let offset = if spread == 0 { 0 } else { (peer.wrapping_mul(31).wrapping_add(n)) % spread };
    base + Duration::from_millis(offset)
}

/// Sends one batch and returns whatever came back, each reply tagged with the
/// group that asked.
///
/// Fewer replies than envelopes is ordinary: the far side drops an envelope
/// for a group it does not host or whose inbox is full, exactly as a lossy
/// network drops a message, and the next heartbeat re-drives it.
async fn send_batch(
    client: &mut RaftServiceClient<Channel>,
    envelopes: Vec<pb::RaftEnvelope>,
    timeout: Duration,
) -> Result<Vec<(GroupId, Message)>, tonic::Status> {
    let mut req = tonic::Request::new(pb::BatchRequest { msgs: envelopes });
    req.set_timeout(timeout);
    let answers = client.batch(req).await?.into_inner().msgs;
    Ok(answers.into_iter().filter_map(|e| convert::unwrap_envelope(e).ok()).collect())
}

/// Streams one snapshot as chunks and resolves the install response. Runs
/// beside the send loop (see `run`): holding the loop for a multi-second
/// transfer would stall this peer's heartbeats past its election timeout.
async fn send_snapshot(
    mut client: RaftServiceClient<Channel>,
    msg: Message,
    group: GroupId,
    timeout: Duration,
) -> Result<Option<Message>, tonic::Status> {
    // Every chunk carries the group, so each one is self-describing and the
    // server can route the transfer from the first chunk it sees.
    let chunks: Vec<_> = snapshot_chunks(&msg)
        .into_iter()
        .map(|mut chunk| {
            chunk.group = group;
            chunk
        })
        .collect();
    let mut req = tonic::Request::new(tokio_stream::iter(chunks));
    req.set_timeout(timeout);
    let resp = client.install_snapshot(req).await?.into_inner();
    Ok(Message::try_from(convert::Outbound::InstallSnapshotResp(resp)).ok())
}
