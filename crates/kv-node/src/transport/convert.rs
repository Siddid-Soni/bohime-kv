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
use kv_raft::{Entry, Message};

/// A proto message that does not correspond to any `kv_raft::Message`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConvertError {
    #[error(
        "InstallSnapshot chunk at offset {offset} is not a whole snapshot; chunked transfer arrives at M8"
    )]
    ChunkedSnapshot { offset: u64 },
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
    RequestVoteResp(pb::RequestVoteResponse),
    AppendEntriesResp(pb::AppendEntriesResponse),
    InstallSnapshotResp(pb::InstallSnapshotResponse),
}

impl From<Message> for Outbound {
    fn from(msg: Message) -> Self {
        match msg {
            Message::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                Outbound::RequestVote(pb::RequestVoteRequest {
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
            } => Outbound::InstallSnapshot(pb::InstallSnapshotChunk {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                // One chunk carrying the whole snapshot. Real chunking is M8;
                // until then `done` is always true and `offset` always 0, and
                // the inbound direction rejects anything else rather than
                // silently treating a fragment as a complete snapshot.
                offset: 0,
                data,
                done: true,
            }),
            Message::InstallSnapshotResp { term, success } => {
                Outbound::InstallSnapshotResp(pb::InstallSnapshotResponse { term, success })
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
                }
            }
            Outbound::InstallSnapshotResp(r) => {
                Message::InstallSnapshotResp { term: r.term, success: r.success }
            }
        })
    }
}
