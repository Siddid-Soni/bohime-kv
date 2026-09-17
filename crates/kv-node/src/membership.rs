//! The node side of membership (M9.2): the peer address book.
//!
//! `kv-raft` decides *who* is in the cluster and keeps that as bare ids —
//! reaching a member is the application's problem, which is exactly what the
//! opaque `ConfChange::context` is for. This module is where that context
//! lands: a `node id -> gRPC endpoint` map the driver reconciles its peer
//! connections against.
//!
//! **It lives in the state machine, not in memory.** Two reasons, and the
//! second is the one that decides it:
//!
//! 1. A restart must rebuild dial state from the log rather than from argv,
//!    or a node that was added at runtime disappears on the next boot.
//! 2. A snapshot *replaces* the log prefix the conf entries lived in, so
//!    replaying the log is not enough on its own. Putting the book in the
//!    state machine means the snapshot carries it for free — the same reason
//!    M7 put the session table there.
//!
//! The keys sit in the reserved `\x00` space the session table already
//! occupies, so the service-boundary check that stops a client forging a
//! session entry also stops it redirecting a peer at a machine of its choice.

use std::collections::BTreeMap;

use kv_raft::NodeId;
use kv_raft::membership::{ConfChange, ConfOp};
use kv_storage::Engine;

/// `PREFIX + node_id.to_be_bytes()`. Fixed-width suffix, so no id can be
/// mistaken for a neighbouring key space.
const PEER_PREFIX: &[u8] = b"\x00peer/";

fn peer_key(id: NodeId) -> Vec<u8> {
    let mut key = PEER_PREFIX.to_vec();
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn peer_id(key: &[u8]) -> Option<NodeId> {
    if key.len() != PEER_PREFIX.len() + 8 || !key.starts_with(PEER_PREFIX) {
        return None;
    }
    let suffix: [u8; 8] = key[PEER_PREFIX.len()..].try_into().expect("checked length");
    Some(NodeId::from_be_bytes(suffix))
}

/// Records where `id` can be reached. A plain overwrite: a node that leaves
/// and rejoins may well come back at a different address.
pub fn record_endpoint(engine: &mut Engine, id: NodeId, address: &str) -> std::io::Result<()> {
    engine.put(&peer_key(id), address.as_bytes())
}

/// Drops `id` from the book. Called when a conf change removes it, so the
/// driver stops dialling a node the cluster no longer contains.
pub fn forget_endpoint(engine: &mut Engine, id: NodeId) -> std::io::Result<()> {
    engine.delete(&peer_key(id))
}

/// The whole book. Scans the keydir, which is in memory — no segment read
/// happens for a key that is not an endpoint.
pub fn endpoints(engine: &Engine) -> std::io::Result<BTreeMap<NodeId, String>> {
    let mut book = BTreeMap::new();
    for (key, value) in engine.scan()? {
        let Some(id) = peer_id(&key) else {
            continue;
        };
        // A non-UTF-8 endpoint cannot have come from the admin path, which
        // takes a `String`. Skipping beats failing the whole drain over one
        // unreachable peer.
        match String::from_utf8(value) {
            Ok(address) => {
                book.insert(id, address);
            }
            Err(e) => tracing::warn!(peer = id, error = %e, "unreadable peer endpoint, ignoring"),
        }
    }
    Ok(book)
}

/// Marks a conf change context that carries a whole address book rather than
/// one address. One byte, because the alternative — guessing from the shape of
/// the bytes — would misread a plain address that happened to parse.
const BOOK_MAGIC: u8 = b'M';

/// Packs an address book into a conf change's `context`.
///
/// The entry that admits a node carries **the whole cluster's addresses**, not
/// just the new node's. That is what makes it self-contained: the joining node
/// learns where every other member lives from the one entry that concerns it,
/// with no flags to keep in sync and no second round trip. The founding
/// members' addresses never appear in a conf entry of their own — they came
/// from argv — so without this a node admitted later could receive from the
/// leader but never dial anyone, which is a node that can never campaign.
pub fn encode_book(book: &BTreeMap<NodeId, String>) -> Vec<u8> {
    let mut out = vec![BOOK_MAGIC];
    out.extend_from_slice(&bincode::serialize(book).expect("an address book always serializes"));
    out
}

/// Reads a conf change's `context` as addresses.
///
/// Falls back to "the whole context is one address for the node being
/// changed", which is what a hand-written or foreign conf entry looks like —
/// `kv-raft` treats the field as opaque and will happily carry either.
fn decode_book(context: &[u8], node: NodeId) -> BTreeMap<NodeId, String> {
    if let Some(body) = context.strip_prefix(&[BOOK_MAGIC])
        && let Ok(book) = bincode::deserialize::<BTreeMap<NodeId, String>>(body)
    {
        return book;
    }
    match std::str::from_utf8(context) {
        Ok(address) => BTreeMap::from([(node, address.to_string())]),
        Err(e) => {
            tracing::warn!(peer = node, error = %e, "conf change carries an unreadable endpoint");
            BTreeMap::new()
        }
    }
}

/// Applies one committed conf change's *application* half.
///
/// The core already changed the quorum when the entry was appended; this is
/// everything outside the core that the entry means, which today is the
/// address book alone. Idempotent, because the driver re-applies the live log
/// tail after a restart.
///
/// Returns what it learned, so the driver can mirror it in memory without
/// re-scanning the engine.
pub fn apply_conf(engine: &mut Engine, change: &ConfChange) -> std::io::Result<ConfEffect> {
    match change.op {
        // A promotion changes a node's standing, not its address, and carries
        // no context. An empty one leaves the book alone rather than
        // overwriting an entry with nothing.
        ConfOp::AddLearner | ConfOp::Promote => {
            if change.context.is_empty() {
                return Ok(ConfEffect::Learned(BTreeMap::new()));
            }
            let book = decode_book(&change.context, change.node);
            for (id, address) in &book {
                record_endpoint(engine, *id, address)?;
            }
            Ok(ConfEffect::Learned(book))
        }
        ConfOp::RemoveLearner | ConfOp::RemoveVoter => {
            forget_endpoint(engine, change.node)?;
            Ok(ConfEffect::Forgot(change.node))
        }
    }
}

/// What applying a conf change did to the address book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfEffect {
    Learned(BTreeMap<NodeId, String>),
    Forgot(NodeId),
}
