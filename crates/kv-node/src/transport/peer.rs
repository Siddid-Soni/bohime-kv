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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kv_proto::raft::raft_service_client::RaftServiceClient;
use kv_raft::{Message, NodeId};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use super::convert::Outbound;

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
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            queue_depth: 64,
            request_timeout: Duration::from_millis(500),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_secs(5),
        }
    }
}

/// A connection to one peer: a bounded queue plus a task that drains it.
pub struct PeerClient {
    tx: mpsc::Sender<Message>,
    /// Connection attempts made. Exposed because "it backs off rather than
    /// spinning" is not observable without counting — a busy loop and a
    /// correctly backing-off client look identical from the outside.
    attempts: Arc<AtomicU64>,
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
        replies: mpsc::Sender<(NodeId, Message)>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_depth);
        let attempts = Arc::new(AtomicU64::new(0));
        tokio::spawn(run(peer, addr, config, rx, replies, Arc::clone(&attempts)));
        Self { tx, attempts, config }
    }

    /// Queues `msg`, shedding it if the queue is full. Never blocks.
    pub fn try_send(&self, msg: Message) -> Result<(), SendError> {
        self.tx.try_send(msg).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SendError::Full,
            mpsc::error::TrySendError::Closed(_) => SendError::Closed,
        })
    }

    /// Messages waiting to be sent. Never exceeds `queue_depth`.
    pub fn queued(&self) -> usize {
        self.config.queue_depth - self.tx.capacity()
    }

    pub fn connect_attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }
}

/// Drains the queue, reconnecting as needed.
async fn run(
    peer: NodeId,
    addr: String,
    config: PeerConfig,
    mut rx: mpsc::Receiver<Message>,
    replies: mpsc::Sender<(NodeId, Message)>,
    attempts: Arc<AtomicU64>,
) {
    let mut client: Option<RaftServiceClient<Channel>> = None;
    let mut backoff = config.backoff_initial;

    while let Some(msg) = rx.recv().await {
        if client.is_none() {
            attempts.fetch_add(1, Ordering::Relaxed);
            match dial(&addr, config.request_timeout).await {
                Ok(c) => {
                    client = Some(c);
                    backoff = config.backoff_initial;
                }
                Err(_) => {
                    // Shed this message and wait before trying again. Sleeping
                    // here is what turns a dead peer into a slow trickle of
                    // attempts instead of a spin: the queue keeps filling and
                    // shedding meanwhile, which is the intended behaviour.
                    tokio::time::sleep(jittered(backoff, peer, &attempts)).await;
                    backoff = (backoff * 2).min(config.backoff_max);
                    continue;
                }
            }
        }

        let Some(c) = client.as_mut() else { continue };
        match send_one(c, msg, config.request_timeout).await {
            Ok(Some(reply)) => {
                // A full reply queue is shed like any other message.
                let _ = replies.try_send((peer, reply));
            }
            Ok(None) => {}
            Err(_) => {
                // Drop the connection so the next message redials.
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

async fn send_one(
    client: &mut RaftServiceClient<Channel>,
    msg: Message,
    timeout: Duration,
) -> Result<Option<Message>, tonic::Status> {
    let reply = match Outbound::from(msg) {
        Outbound::RequestVote(r) => {
            let mut req = tonic::Request::new(r);
            req.set_timeout(timeout);
            Some(Outbound::RequestVoteResp(client.request_vote(req).await?.into_inner()))
        }
        Outbound::AppendEntries(r) => {
            let mut req = tonic::Request::new(r);
            req.set_timeout(timeout);
            Some(Outbound::AppendEntriesResp(client.append_entries(req).await?.into_inner()))
        }
        // Responses are returned by the server as RPC responses, never sent as
        // requests, so reaching here means the driver addressed a reply to a
        // peer instead of answering an inbound RPC.
        Outbound::RequestVoteResp(_)
        | Outbound::AppendEntriesResp(_)
        | Outbound::InstallSnapshotResp(_) => None,
        Outbound::InstallSnapshot(_) => None, // M8
    };
    Ok(reply.and_then(|out| Message::try_from(out).ok()))
}
