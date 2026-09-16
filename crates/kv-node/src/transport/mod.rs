//! gRPC transport for the Raft core (M5): the impure shell that carries
//! `kv_raft::Message` values between processes.
//!
// Every fallible call here returns `tonic::Status`, which is ~176 bytes and so
// trips `result_large_err`. Boxing it would mean unwrapping at every tonic
// boundary for no benefit; kv-proto carries the same allow for its generated
// code. Scoped to the transport, where tonic actually lives.
#![allow(clippy::result_large_err)]

pub mod convert;
pub mod peer;
pub mod server;

use kv_raft::Message;

use self::peer::{PeerClient, SendError};

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
    /// Queues `msg`, shedding it if the queue is full. Never blocks.
    fn try_send(&self, msg: Message) -> Result<(), SendError>;
}

impl PeerLink for PeerClient {
    fn try_send(&self, msg: Message) -> Result<(), SendError> {
        PeerClient::try_send(self, msg)
    }
}
