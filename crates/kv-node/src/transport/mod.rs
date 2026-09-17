//! gRPC transport for the Raft core (M5): the impure shell that carries
//! `kv_raft::Message` values between processes.
//!
// Every fallible call here returns `tonic::Status`, which is ~176 bytes and so
// trips `result_large_err`. Boxing it would mean unwrapping at every tonic
// boundary for no benefit; kv-proto carries the same allow for its generated
// code. Scoped to the transport, where tonic actually lives.
#![allow(clippy::result_large_err)]

pub mod convert;
pub mod group;
pub mod peer;
pub mod server;

use kv_raft::{Message, NodeId};
use tokio::sync::mpsc;

use self::group::GroupId;
use self::peer::{PeerClient, PeerConfig, SendError};

/// How the driver reaches one peer.
///
/// `PeerClient` is the real implementation. The trait exists so tests can
/// substitute an in-memory link and run a whole cluster inside one process
/// under a partition — something loopback sockets cannot express without root,
/// and something M7's gate and M9's membership tests both need.
///
/// One dyn call per outbound message, measured against a network round trip.
/// A generic parameter would buy nothing and infect every signature that
/// touches a `Driver`.
pub trait PeerLink: Send + Sync + 'static {
    /// Queues `msg` for `group`, shedding it if the queue is full. Never
    /// blocks.
    ///
    /// The group travels with the message rather than with the link (M11.2):
    /// one link per peer carries every group this process hosts, so a node
    /// with a Raft group per shard opens one connection per peer instead of
    /// one per shard per peer.
    fn try_send(&self, group: GroupId, msg: Message) -> Result<(), SendError>;
}

impl PeerLink for PeerClient {
    fn try_send(&self, group: GroupId, msg: Message) -> Result<(), SendError> {
        PeerClient::try_send(self, group, msg)
    }
}

/// How the driver *opens* a link to a peer it did not start with.
///
/// Until M9 the peer set was fixed at boot, so `main` could build every link
/// up front. Membership changes make the set a moving target: a committed
/// conf entry names a node this process has never dialled, and the driver
/// has to reach it without a restart. The driver is the only thing that
/// knows the current membership, so it is the thing that has to be able to
/// connect — through this, so that the in-process test cluster can hand it a
/// channel instead of a socket.
pub trait PeerFactory: Send + 'static {
    /// Opens a link to `id` at `address`.
    ///
    /// Never blocks and never fails: `PeerClient` connects lazily and retries
    /// with backoff, so a member that is not up yet is an ordinary state
    /// rather than an error. A node added to the config before its process
    /// starts is exactly the normal case.
    fn connect(&self, id: NodeId, address: &str) -> Box<dyn PeerLink>;
}

/// The real factory: one `PeerClient` per member, all reporting into the
/// driver's single reply channel.
pub struct GrpcPeers {
    config: PeerConfig,
    replies: mpsc::Sender<(GroupId, NodeId, Message)>,
}

impl GrpcPeers {
    pub fn new(config: PeerConfig, replies: mpsc::Sender<(GroupId, NodeId, Message)>) -> Self {
        Self { config, replies }
    }
}

impl PeerFactory for GrpcPeers {
    fn connect(&self, id: NodeId, address: &str) -> Box<dyn PeerLink> {
        Box::new(PeerClient::connect(id, address.to_string(), self.config, self.replies.clone()))
    }
}
