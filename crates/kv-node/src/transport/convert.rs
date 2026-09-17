//! `kv_raft::Message` <-> prost types, in one file (M5, task 1).
//!
//! Keeping every conversion here is what stops proto types leaking into
//! `kv-raft`, which must stay free of tonic and prost for M4's simulator to
//! keep working.
//!
//! The conversions are total in one direction and fallible in the other: every
//! `Message` has a proto form, but a proto message arriving off the wire may
//! not name a variant we understand, so the inbound direction is `TryFrom`.

use kv_proto::raft as pb;
use kv_raft::{ClusterConfig, Entry, Message};

use super::group;

/// A proto message that does not correspond to any `kv_raft::Message`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConvertError {
    #[error("InstallSnapshot chunk at offset {offset} is not a whole snapshot")]
    ChunkedSnapshot { offset: u64 },
    #[error("InstallSnapshot stream is empty; no snapshot was sent")]
    EmptySnapshot,
    #[error("InstallSnapshot chunk at offset {got} breaks contiguity, expected {expected}")]
    OffsetGap { expected: u64, got: u64 },
    #[error("InstallSnapshot chunk carries a different snapshot header")]
    HeaderMismatch,
    #[error("InstallSnapshot stream ended without a terminal chunk")]
    TruncatedSnapshot,
    #[error("a batch envelope carries no message")]
    EmptyEnvelope,
}

/// One chunk's payload ceiling. Small enough to multiplex with heartbeats on
/// the shared channel, large enough that chunk framing is noise next to it.
pub const SNAPSHOT_CHUNK_SIZE: usize = 64 * 1024;

/// Splits a snapshot message into transmittable chunks. Always at least one —
/// an empty image still needs a terminal chunk, or nothing arrives at all.
/// Every chunk repeats the membership header, so each one is self-describing
/// and the assembly check stays a uniformity comparison.
pub fn snapshot_chunks(msg: &Message) -> Vec<pb::InstallSnapshotChunk> {
    let Message::InstallSnapshot {
        term,
        leader_id,
        last_included_index,
        last_included_term,
        data,
        config,
    } = msg
    else {
        panic!("snapshot_chunks takes an InstallSnapshot");
    };
    let voters: Vec<u64> = config.voters.iter().copied().collect();
    let learners: Vec<u64> = config.learners.iter().copied().collect();
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    for piece in data.chunks(SNAPSHOT_CHUNK_SIZE.max(1)) {
        chunks.push(pb::InstallSnapshotChunk {
            group: group::UNSET,
            term: *term,
            leader_id: *leader_id,
            last_included_index: *last_included_index,
            last_included_term: *last_included_term,
            offset,
            data: piece.to_vec(),
            done: false,
            voters: voters.clone(),
            learners: learners.clone(),
        });
        offset += piece.len() as u64;
    }
    if chunks.is_empty() {
        chunks.push(pb::InstallSnapshotChunk {
            group: group::UNSET,
            term: *term,
            leader_id: *leader_id,
            last_included_index: *last_included_index,
            last_included_term: *last_included_term,
            offset: 0,
            data: Vec::new(),
            done: true,
            voters,
            learners,
        });
    } else {
        chunks.last_mut().expect("non-empty").done = true;
    }
    chunks
}

/// Reassembles a chunk stream into the message it was split from. Every
/// structural defect is an error rather than a best effort: a gapped, mixed,
/// or unterminated stream must never decode into a truncated state machine.
pub fn assemble_snapshot(chunks: &[pb::InstallSnapshotChunk]) -> Result<Message, ConvertError> {
    let Some(first) = chunks.first() else {
        return Err(ConvertError::EmptySnapshot);
    };
    if first.offset != 0 {
        return Err(ConvertError::OffsetGap { expected: 0, got: first.offset });
    }
    let mut data = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        if chunk.term != first.term
            || chunk.leader_id != first.leader_id
            || chunk.last_included_index != first.last_included_index
            || chunk.last_included_term != first.last_included_term
            || chunk.voters != first.voters
            || chunk.learners != first.learners
        {
            return Err(ConvertError::HeaderMismatch);
        }
        if chunk.offset != data.len() as u64 {
            return Err(ConvertError::OffsetGap { expected: data.len() as u64, got: chunk.offset });
        }
        let last = i + 1 == chunks.len();
        if chunk.done != last {
            return Err(ConvertError::TruncatedSnapshot);
        }
        data.extend_from_slice(&chunk.data);
    }
    Ok(Message::InstallSnapshot {
        term: first.term,
        leader_id: first.leader_id,
        last_included_index: first.last_included_index,
        last_included_term: first.last_included_term,
        data,
        config: ClusterConfig {
            voters: first.voters.iter().copied().collect(),
            learners: first.learners.iter().copied().collect(),
        },
    })
}

// Free functions rather than `From` impls: both types are foreign to this
// crate, so the orphan rule forbids the trait impls.
fn entry_to_pb(e: Entry) -> pb::LogEntry {
    pb::LogEntry { term: e.term, index: e.index, command: e.command }
}

fn entry_from_pb(e: pb::LogEntry) -> Entry {
    Entry { term: e.term, index: e.index, command: e.command }
}

/// The request half of the service: the three messages a peer initiates with.
///
/// Split from responses because tonic models each RPC as request -> response,
/// so a `Message` is never converted to "some proto type" in the abstract —
/// it is always converted for a specific RPC.
#[derive(Debug, Clone, PartialEq)]
pub enum Outbound {
    RequestVote(pb::RequestVoteRequest),
    AppendEntries(pb::AppendEntriesRequest),
    InstallSnapshot(pb::InstallSnapshotChunk),
    TimeoutNow(pb::TimeoutNowRequest),
    RequestVoteResp(pb::RequestVoteResponse),
    AppendEntriesResp(pb::AppendEntriesResponse),
    InstallSnapshotResp(pb::InstallSnapshotResponse),
    TimeoutNowResp(pb::TimeoutNowResponse),
}

impl From<Message> for Outbound {
    fn from(msg: Message) -> Self {
        match msg {
            Message::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                Outbound::RequestVote(pb::RequestVoteRequest {
                    group: group::UNSET,
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                })
            }
            Message::RequestVoteResp { term, vote_granted } => {
                Outbound::RequestVoteResp(pb::RequestVoteResponse { term, vote_granted })
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            } => Outbound::AppendEntries(pb::AppendEntriesRequest {
                group: group::UNSET,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries: entries.into_iter().map(entry_to_pb).collect(),
                leader_commit,
                read_round,
            }),
            Message::AppendEntriesResp {
                term,
                success,
                match_index,
                conflict_term,
                conflict_index,
                read_round,
            } => Outbound::AppendEntriesResp(pb::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_term,
                conflict_index,
                read_round,
            }),
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
                config,
            } => Outbound::InstallSnapshot(pb::InstallSnapshotChunk {
                group: group::UNSET,
                term,
                leader_id,
                last_included_index,
                last_included_term,
                // One chunk carrying the whole snapshot. The chunked path
                // (`snapshot_chunks`) is what the peer actually sends; this
                // single-chunk form exists so the message round-trip property
                // still covers the variant, and the inbound direction rejects
                // anything but a whole snapshot rather than silently treating
                // a fragment as complete.
                offset: 0,
                data,
                done: true,
                voters: config.voters.iter().copied().collect(),
                learners: config.learners.iter().copied().collect(),
            }),
            Message::TimeoutNow { term, leader_id } => {
                Outbound::TimeoutNow(pb::TimeoutNowRequest { group: group::UNSET, term, leader_id })
            }
            Message::InstallSnapshotResp { term, success } => {
                Outbound::InstallSnapshotResp(pb::InstallSnapshotResponse { term, success })
            }
            Message::TimeoutNowResp { term } => {
                Outbound::TimeoutNowResp(pb::TimeoutNowResponse { term })
            }
        }
    }
}

impl TryFrom<Outbound> for Message {
    type Error = ConvertError;

    fn try_from(value: Outbound) -> Result<Self, Self::Error> {
        Ok(match value {
            Outbound::RequestVote(r) => Message::RequestVote {
                term: r.term,
                candidate_id: r.candidate_id,
                last_log_index: r.last_log_index,
                last_log_term: r.last_log_term,
            },
            Outbound::RequestVoteResp(r) => {
                Message::RequestVoteResp { term: r.term, vote_granted: r.vote_granted }
            }
            Outbound::AppendEntries(r) => Message::AppendEntries {
                term: r.term,
                leader_id: r.leader_id,
                prev_log_index: r.prev_log_index,
                prev_log_term: r.prev_log_term,
                entries: r.entries.into_iter().map(entry_from_pb).collect(),
                leader_commit: r.leader_commit,
                read_round: r.read_round,
            },
            Outbound::AppendEntriesResp(r) => Message::AppendEntriesResp {
                term: r.term,
                success: r.success,
                match_index: r.match_index,
                conflict_term: r.conflict_term,
                conflict_index: r.conflict_index,
                read_round: r.read_round,
            },
            Outbound::InstallSnapshot(r) => {
                if r.offset != 0 || !r.done {
                    return Err(ConvertError::ChunkedSnapshot { offset: r.offset });
                }
                Message::InstallSnapshot {
                    term: r.term,
                    leader_id: r.leader_id,
                    last_included_index: r.last_included_index,
                    last_included_term: r.last_included_term,
                    data: r.data,
                    config: ClusterConfig {
                        voters: r.voters.into_iter().collect(),
                        learners: r.learners.into_iter().collect(),
                    },
                }
            }
            Outbound::InstallSnapshotResp(r) => {
                Message::InstallSnapshotResp { term: r.term, success: r.success }
            }
            Outbound::TimeoutNow(r) => Message::TimeoutNow { term: r.term, leader_id: r.leader_id },
            Outbound::TimeoutNowResp(r) => Message::TimeoutNowResp { term: r.term },
        })
    }
}

/// Wraps one message for one group, for the batched peer RPC (M11.3).
///
/// The group is set on the envelope *and* on the inner request where the
/// inner type has the field, so a batched request is byte-identical to the
/// unary one the four single-message RPCs carry. The server reads the
/// envelope's copy — it is the only one a *response* can have.
pub fn envelope(group: group::GroupId, msg: Message) -> pb::RaftEnvelope {
    use pb::raft_envelope::Msg;
    let msg = match Outbound::from(msg) {
        Outbound::RequestVote(mut r) => {
            r.group = group;
            Some(Msg::Vote(r))
        }
        Outbound::AppendEntries(mut r) => {
            r.group = group;
            Some(Msg::Append(r))
        }
        Outbound::TimeoutNow(mut r) => {
            r.group = group;
            Some(Msg::TimeoutNow(r))
        }
        Outbound::RequestVoteResp(r) => Some(Msg::VoteResp(r)),
        Outbound::AppendEntriesResp(r) => Some(Msg::AppendResp(r)),
        Outbound::TimeoutNowResp(r) => Some(Msg::TimeoutNowResp(r)),
        // Snapshots travel on their own streaming RPC: chunking needs the
        // message whole, and one image would swamp a batch sized for
        // heartbeats. A caller that tries is a bug rather than a peer error,
        // and an envelope with no message is what the far side refuses.
        Outbound::InstallSnapshot(_) | Outbound::InstallSnapshotResp(_) => None,
    };
    pb::RaftEnvelope { group, msg }
}

/// Unwraps one batched envelope.
pub fn unwrap_envelope(
    envelope: pb::RaftEnvelope,
) -> Result<(group::GroupId, Message), ConvertError> {
    use pb::raft_envelope::Msg;
    let group = envelope.group;
    let out = match envelope.msg.ok_or(ConvertError::EmptyEnvelope)? {
        Msg::Vote(r) => Outbound::RequestVote(r),
        Msg::VoteResp(r) => Outbound::RequestVoteResp(r),
        Msg::Append(r) => Outbound::AppendEntries(r),
        Msg::AppendResp(r) => Outbound::AppendEntriesResp(r),
        Msg::TimeoutNow(r) => Outbound::TimeoutNow(r),
        Msg::TimeoutNowResp(r) => Outbound::TimeoutNowResp(r),
    };
    Ok((group, Message::try_from(out)?))
}

/// Who sent a *request*, as the message itself names them.
///
/// A response has no sender field — the caller knows who answered because it
/// knows who it called — so this is `None` for one, which is also how the
/// server tells a request batch from a response wrongly sent as one.
pub fn sender_of(msg: &Message) -> Option<kv_raft::NodeId> {
    match msg {
        Message::RequestVote { candidate_id, .. } => Some(*candidate_id),
        Message::AppendEntries { leader_id, .. }
        | Message::InstallSnapshot { leader_id, .. }
        | Message::TimeoutNow { leader_id, .. } => Some(*leader_id),
        Message::RequestVoteResp { .. }
        | Message::AppendEntriesResp { .. }
        | Message::InstallSnapshotResp { .. }
        | Message::TimeoutNowResp { .. } => None,
    }
}
