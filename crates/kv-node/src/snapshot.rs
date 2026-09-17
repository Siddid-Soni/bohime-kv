//! The portable snapshot image (M8, §1.6 as decided: a serialized scan, not a
//! segment file).
//!
//! A snapshot is the state machine's live `(key, value)` pairs — user keys and
//! session entries alike, since both live in the same `Engine` — plus the log
//! boundary they replace. Sorted by key on encode, so any two replicas
//! snapshotting the same state produce the same bytes; checksummed, so a
//! truncated stream can never restore silently.
//!
//! Layout, all big-endian: `magic u32 | index u64 | term u64 | count u64 |
//! (klen u32 | vlen u32 | key | value)* | crc32 u32`. The CRC covers everything
//! before it.

use kv_raft::{LogIndex, Term};

const MAGIC: u32 = 0x4248_4D31; // "BHM1"
const HEADER_LEN: usize = 4 + 8 + 8 + 8;
const CRC_LEN: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotData {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SnapshotError {
    #[error("snapshot is truncated")]
    Truncated,
    #[error("snapshot has a bad magic: not our encoding")]
    BadMagic,
    #[error("snapshot checksum mismatch: truncated or tampered")]
    CrcMismatch,
}

/// Serializes one state image. Sorts by key so the encoding is deterministic
/// in the state, not in whatever order the keydir happened to iterate.
pub fn encode(
    last_included_index: LogIndex,
    last_included_term: Term,
    pairs: &[(Vec<u8>, Vec<u8>)],
) -> Vec<u8> {
    let mut sorted: Vec<&(Vec<u8>, Vec<u8>)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC.to_be_bytes());
    out.extend_from_slice(&last_included_index.to_be_bytes());
    out.extend_from_slice(&last_included_term.to_be_bytes());
    out.extend_from_slice(&(sorted.len() as u64).to_be_bytes());
    for (key, value) in sorted {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(value);
    }
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    out
}

pub fn decode(data: &[u8]) -> Result<SnapshotData, SnapshotError> {
    if data.len() < HEADER_LEN + CRC_LEN {
        return Err(SnapshotError::Truncated);
    }
    let (body, crc_bytes) = data.split_at(data.len() - CRC_LEN);
    let expected = u32::from_be_bytes(crc_bytes.try_into().expect("split_at CRC_LEN"));
    if crc32fast::hash(body) != expected {
        return Err(SnapshotError::CrcMismatch);
    }
    if u32::from_be_bytes(body[0..4].try_into().expect("magic range")) != MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let last_included_index = u64::from_be_bytes(body[4..12].try_into().expect("index range"));
    let last_included_term = u64::from_be_bytes(body[12..20].try_into().expect("term range"));
    let count = u64::from_be_bytes(body[20..28].try_into().expect("count range"));

    let mut pairs = Vec::new();
    let mut rest = &body[HEADER_LEN..];
    for _ in 0..count {
        if rest.len() < 8 {
            return Err(SnapshotError::Truncated);
        }
        let klen = u32::from_be_bytes(rest[0..4].try_into().expect("klen")) as usize;
        let vlen = u32::from_be_bytes(rest[4..8].try_into().expect("vlen")) as usize;
        rest = &rest[8..];
        if rest.len() < klen + vlen {
            return Err(SnapshotError::Truncated);
        }
        // `split_at` twice rather than slicing twice: the second bound is
        // relative to the remainder, and getting that wrong invents overlap.
        let (key, after_key) = rest.split_at(klen);
        let (value, after_value) = after_key.split_at(vlen);
        pairs.push((key.to_vec(), value.to_vec()));
        rest = after_value;
    }
    if !rest.is_empty() {
        return Err(SnapshotError::Truncated);
    }
    Ok(SnapshotData { last_included_index, last_included_term, pairs })
}
