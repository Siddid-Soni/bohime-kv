//! Exactly-once client semantics (M7, §1.8), bounded at M8.
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
//!
//! **Bounded (M8).** The table held one entry per client ever seen —
//! unbounded growth for a long-lived cluster. It now holds at most
//! `MAX_CLIENTS`, evicting the least-recently-touched client. Recency is the
//! applied log index, never wall-clock: expiry must be identical on every
//! replica, and only the log order is. An evicted client's retry is treated as
//! new and re-applies — safe for `Put`/`Delete`, a documented duplicate risk
//! for `Cas` — so the cap must exceed any realistic concurrent-client count.

use kv_raft::LogIndex;
use kv_storage::Engine;
use serde::{Deserialize, Serialize};

/// Reserved key space. User keys may not begin with `\x00` — `is_reserved`
/// is enforced at the service boundary so a client cannot forge a session
/// entry or corrupt one by accident.
pub const RESERVED_PREFIX: u8 = 0x00;

/// At most this many clients are remembered. Past it, the idlest client goes.
/// Must exceed any realistic concurrent-client count: eviction turns a retry
/// into a re-application, which `Cas` answers differently the second time.
pub const MAX_CLIENTS: usize = 10_000;

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
    /// Applied log index of the last record. `touched` sorts the eviction,
    /// and log order is the same on every replica — wall-clock would not be.
    touched: LogIndex,
}

/// The pre-M8 encoding, without `touched`. Entries written before the bound
/// existed still decode — with unknown age, so they are eviction-first rather
/// than lost.
#[derive(Debug, Deserialize)]
struct OldSessionEntry {
    last_sequence: u64,
    last_response: CommandResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct SessionMeta {
    count: u64,
}

/// `PREFIX + client_id.to_be_bytes()`. The meta key is `PREFIX + "meta"` —
/// distinguishable because a client suffix is always exactly 8 bytes.
const SESSION_PREFIX: &[u8] = b"\x00session/";

/// Whether `key` is in the reserved space the session table occupies.
pub fn is_reserved(key: &[u8]) -> bool {
    key.first() == Some(&RESERVED_PREFIX)
}

fn session_key(client_id: u64) -> Vec<u8> {
    let mut key = SESSION_PREFIX.to_vec();
    key.extend_from_slice(&client_id.to_be_bytes());
    key
}

fn meta_key() -> Vec<u8> {
    let mut key = SESSION_PREFIX.to_vec();
    key.extend_from_slice(b"meta");
    key
}

fn is_session_key(key: &[u8]) -> bool {
    key.len() == SESSION_PREFIX.len() + 8 && key.starts_with(SESSION_PREFIX)
}

fn decode_entry(bytes: &[u8]) -> Result<SessionEntry, bincode::Error> {
    match bincode::deserialize::<SessionEntry>(bytes) {
        Ok(entry) => Ok(entry),
        Err(_) => {
            let old: OldSessionEntry = bincode::deserialize(bytes)?;
            Ok(SessionEntry {
                last_sequence: old.last_sequence,
                last_response: old.last_response,
                touched: 0,
            })
        }
    }
}

fn io_invalid(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
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
    let entry = decode_entry(&bytes).map_err(io_invalid)?;
    if ctx.sequence <= entry.last_sequence { Ok(Some(entry.last_response)) } else { Ok(None) }
}

/// Records the answer given, so a retry of this request returns it again.
pub fn record(
    engine: &mut Engine,
    ctx: &RequestCtx,
    response: &CommandResponse,
    applied: LogIndex,
) -> std::io::Result<()> {
    record_bounded(engine, ctx, response, applied, MAX_CLIENTS)
}

/// `record` with an explicit cap, so tests can force an eviction without
/// registering ten thousand clients.
pub(crate) fn record_bounded(
    engine: &mut Engine,
    ctx: &RequestCtx,
    response: &CommandResponse,
    applied: LogIndex,
    cap: usize,
) -> std::io::Result<()> {
    let key = session_key(ctx.client_id);
    let is_new = engine.get(&key)?.is_none();
    let entry = SessionEntry {
        last_sequence: ctx.sequence,
        last_response: response.clone(),
        touched: applied,
    };
    let encoded = bincode::serialize(&entry).map_err(io_invalid)?;
    engine.put(&key, &encoded)?;
    if !is_new {
        return Ok(());
    }

    let mut meta = match engine.get(&meta_key())? {
        Some(bytes) => {
            let mut meta: SessionMeta = bincode::deserialize(&bytes).map_err(io_invalid)?;
            meta.count += 1;
            meta
        }
        // Pre-cap engines have session entries but no count. The scan runs
        // after the new entry was written, so it already includes this client
        // and must not be incremented again — that off-by-one evicts one
        // client early, every time, on every replica identically.
        None => {
            let counted = engine.scan()?.iter().filter(|(k, _)| is_session_key(k)).count() as u64;
            SessionMeta { count: counted }
        }
    };
    if meta.count > cap as u64 {
        evict_idlest(engine)?;
        meta.count -= 1;
    }
    let encoded = bincode::serialize(&meta).map_err(io_invalid)?;
    engine.put(&meta_key(), &encoded)
}

/// Removes the least-recently-touched client. Ties break on client id, and
/// both fields are log-derived, so every replica evicts the same client for
/// the same log — a wall-clock age would diverge them.
fn evict_idlest(engine: &mut Engine) -> std::io::Result<()> {
    let mut idlest: Option<(LogIndex, u64)> = None;
    for (key, value) in engine.scan()? {
        if !is_session_key(&key) {
            continue;
        }
        let entry = decode_entry(&value).map_err(io_invalid)?;
        let client =
            u64::from_be_bytes(key[SESSION_PREFIX.len()..].try_into().expect("checked len"));
        let candidate = (entry.touched, client);
        if idlest.is_none_or(|best| candidate < best) {
            idlest = Some(candidate);
        }
    }
    let (_, client) = idlest.expect("eviction runs over the cap, so at least one client exists");
    engine.delete(&session_key(client))
}
