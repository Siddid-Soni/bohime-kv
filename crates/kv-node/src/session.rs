//! Exactly-once client semantics (M7, §1.8).
//!
//! A client sends `Cas`, the leader commits it, then dies before replying. The
//! client retries. Without protection the operation applies twice — harmless
//! for `Put`, wrong for `Cas`: the second evaluation sees the value it already
//! swapped in and answers `swapped: false`, which is not a wasted write but a
//! **wrong answer**.
//!
//! Fix: every client has an id, every request a monotonic sequence number, and
//! the state machine remembers the last one it answered per client along with
//! the answer it gave. A duplicate returns the cached answer without
//! re-applying.
//!
//! **The table lives inside the state machine**, in the same `Engine` as user
//! data, because the only situation it exists for is a leader failover — a
//! table held in driver memory dies with exactly the node whose death made it
//! necessary. Being in the log's state machine means every replica has it.
//!
//! Atomicity: applying a mutation is two `Engine::put`s (the value, then the
//! session entry) and Bitcask has no transaction across them. A crash in
//! between is nonetheless safe, because a restart replays the log from the
//! beginning and redoes both — which is the same property that lets M6 get
//! away with not persisting `last_applied`, and which M8's snapshots will have
//! to preserve.

use kv_storage::Engine;
use serde::{Deserialize, Serialize};

/// Reserved key space. User keys may not begin with `\x00` — `is_reserved`
/// is enforced at the service boundary so a client cannot forge a session
/// entry or corrupt one by accident.
pub const RESERVED_PREFIX: u8 = 0x00;

/// Identifies one request from one client, so a retry is recognisable as the
/// same request rather than a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestCtx {
    pub client_id: u64,
    pub sequence: u64,
}

/// What a mutation answered, cached so a retry gets the same answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResponse {
    Applied,
    /// Whether the compare-and-swap took effect.
    Swapped(bool),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEntry {
    last_sequence: u64,
    last_response: CommandResponse,
}

/// Whether `key` is in the reserved space the session table occupies.
pub fn is_reserved(key: &[u8]) -> bool {
    key.first() == Some(&RESERVED_PREFIX)
}

fn session_key(client_id: u64) -> Vec<u8> {
    let mut key = vec![RESERVED_PREFIX];
    key.extend_from_slice(b"session/");
    key.extend_from_slice(&client_id.to_be_bytes());
    key
}

/// The cached answer, if this request has already been applied.
///
/// `<=` rather than `==`: a client whose sequence numbers are monotonic will
/// never resend an older one, but treating an older one as fresh would
/// re-apply it, and the table only remembers the newest.
pub fn cached(engine: &mut Engine, ctx: &RequestCtx) -> std::io::Result<Option<CommandResponse>> {
    let Some(bytes) = engine.get(&session_key(ctx.client_id))? else {
        return Ok(None);
    };
    let entry: SessionEntry = bincode::deserialize(&bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if ctx.sequence <= entry.last_sequence { Ok(Some(entry.last_response)) } else { Ok(None) }
}

/// Records the answer given, so a retry of this request returns it again.
pub fn record(
    engine: &mut Engine,
    ctx: &RequestCtx,
    response: &CommandResponse,
) -> std::io::Result<()> {
    let entry = SessionEntry { last_sequence: ctx.sequence, last_response: response.clone() };
    let encoded = bincode::serialize(&entry)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    engine.put(&session_key(ctx.client_id), &encoded)
}
