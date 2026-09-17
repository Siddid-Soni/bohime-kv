//! Every `Message` variant must survive a round trip through its proto form.
//!
//! A field silently dropped here surfaces later as a Raft bug — a vote that
//! is never granted, a conflict hint that never converges — and that is the
//! worst possible place to debug it. So this is generated, not hand-picked:
//! hand-written cases test the fields someone remembered.

use kv_raft::{Entry, Message};
use proptest::prelude::*;

use crate::transport::convert::{ConvertError, Outbound};

fn entry_strategy() -> impl Strategy<Value = Entry> {
    (any::<u64>(), any::<u64>(), proptest::collection::vec(any::<u8>(), 0..32))
        .prop_map(|(term, index, command)| Entry { term, index, command })
}

fn message_strategy() -> impl Strategy<Value = Message> {
    prop_oneof![
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
            |(term, candidate_id, last_log_index, last_log_term)| Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            }
        ),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, vote_granted)| Message::RequestVoteResp { term, vote_granted }),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            proptest::collection::vec(entry_strategy(), 0..6),
            any::<u64>(),
            proptest::option::of(any::<u64>()),
        )
            .prop_map(
                |(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_round,
                )| {
                    Message::AppendEntries {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        read_round,
                    }
                }
            ),
        (
            any::<u64>(),
            any::<bool>(),
            any::<u64>(),
            proptest::option::of(any::<u64>()),
            proptest::option::of(any::<u64>()),
            proptest::option::of(any::<u64>()),
        )
            .prop_map(
                |(term, success, match_index, conflict_term, conflict_index, read_round)| {
                    Message::AppendEntriesResp {
                        term,
                        success,
                        match_index,
                        conflict_term,
                        conflict_index,
                        read_round,
                    }
                }
            ),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            proptest::collection::vec(any::<u8>(), 0..64),
        )
            .prop_map(
                |(term, leader_id, last_included_index, last_included_term, data)| {
                    Message::InstallSnapshot {
                        term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        data,
                        config: kv_raft::ClusterConfig::voting([leader_id]),
                    }
                }
            ),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, success)| Message::InstallSnapshotResp { term, success }),
        (any::<u64>(), any::<u64>())
            .prop_map(|(term, leader_id)| Message::TimeoutNow { term, leader_id }),
        (any::<u64>(),).prop_map(|(term,)| Message::TimeoutNowResp { term }),
    ]
}

proptest! {
    #[test]
    fn every_message_survives_a_round_trip_through_proto(msg in message_strategy()) {
        let wire: Outbound = msg.clone().into();
        let back = Message::try_from(wire).expect("a message we produced must convert back");
        prop_assert_eq!(back, msg);
    }
}

/// `read_round` is `Option<u64>` for the same reason the conflict hints are:
/// round 0 is a real round, so a sentinel 0 would make "round 0" and "no read
/// outstanding" the same value — and a leader would then count an ack from a
/// heartbeat that carried no read at all as confirmation of one.
#[test]
fn an_absent_read_round_stays_absent() {
    let none = Message::AppendEntriesResp {
        term: 4,
        success: true,
        match_index: 2,
        conflict_term: None,
        conflict_index: None,
        read_round: None,
    };
    let round_zero = Message::AppendEntriesResp {
        term: 4,
        success: true,
        match_index: 2,
        conflict_term: None,
        conflict_index: None,
        read_round: Some(0),
    };

    let back_none = Message::try_from(Outbound::from(none.clone())).unwrap();
    let back_zero = Message::try_from(Outbound::from(round_zero.clone())).unwrap();
    assert_eq!(back_none, none);
    assert_eq!(back_zero, round_zero);
    assert_ne!(back_none, back_zero, "round 0 and no round must not collapse");
}

#[test]
fn absent_conflict_hints_stay_absent() {
    // The regression this guards: encoding Option<u64> as a sentinel 0 makes
    // `Some(0)` and `None` indistinguishable, and the leader then backtracks
    // to the wrong place forever.
    let msg = Message::AppendEntriesResp {
        term: 4,
        success: false,
        match_index: 0,
        conflict_term: None,
        conflict_index: None,
        read_round: None,
    };
    let back = Message::try_from(Outbound::from(msg.clone())).unwrap();
    assert_eq!(back, msg);

    let with_zero = Message::AppendEntriesResp {
        term: 4,
        success: false,
        match_index: 0,
        conflict_term: Some(0),
        conflict_index: Some(0),
        read_round: None,
    };
    let back_zero = Message::try_from(Outbound::from(with_zero.clone())).unwrap();
    assert_eq!(back_zero, with_zero);
    assert_ne!(back, back_zero, "Some(0) and None must not collapse onto each other");
}

#[test]
fn a_partial_snapshot_chunk_is_rejected_not_silently_accepted() {
    // A fragment on its own is still meaningless: chunks are only valid as a
    // stream the server assembles. Mistaking one for a whole snapshot would
    // install a truncated state machine and look like corruption.
    let chunk = kv_proto::raft::InstallSnapshotChunk {
        group: crate::transport::group::DATA,
        term: 3,
        leader_id: 1,
        last_included_index: 50,
        last_included_term: 2,
        offset: 4096,
        data: vec![7; 16],
        done: false,
        voters: vec![1, 2],
        learners: vec![],
    };
    assert_eq!(
        Message::try_from(Outbound::InstallSnapshot(chunk)),
        Err(ConvertError::ChunkedSnapshot { offset: 4096 })
    );
}

#[test]
fn a_snapshot_chunks_and_reassembles_over_many_chunks() {
    use crate::transport::convert::{SNAPSHOT_CHUNK_SIZE, assemble_snapshot, snapshot_chunks};

    let msg = Message::InstallSnapshot {
        term: 3,
        leader_id: 1,
        last_included_index: 50,
        last_included_term: 2,
        data: (0..(SNAPSHOT_CHUNK_SIZE * 2 + 100)).map(|i| (i % 251) as u8).collect(),
        config: kv_raft::ClusterConfig::voting([1, 2]),
    };
    let chunks = snapshot_chunks(&msg);
    assert!(chunks.len() >= 3, "a 2x-plus payload must split, got {}", chunks.len());
    assert!(chunks.iter().all(|c| c.data.len() <= SNAPSHOT_CHUNK_SIZE));
    assert_eq!(chunks.first().unwrap().offset, 0);
    assert!(chunks.last().unwrap().done);
    assert!(chunks[..chunks.len() - 1].iter().all(|c| !c.done));

    let back = assemble_snapshot(&chunks).expect("chunks we produced must assemble");
    assert_eq!(back, msg);
}

#[test]
fn an_empty_snapshot_still_travels_as_one_chunk() {
    use crate::transport::convert::{assemble_snapshot, snapshot_chunks};

    let msg = Message::InstallSnapshot {
        term: 1,
        leader_id: 1,
        last_included_index: 2,
        last_included_term: 1,
        data: Vec::new(),
        config: kv_raft::ClusterConfig::voting([1, 2]),
    };
    let chunks = snapshot_chunks(&msg);
    assert_eq!(chunks.len(), 1, "zero chunks would deliver nothing at all");
    assert_eq!(assemble_snapshot(&chunks).unwrap(), msg);
}

#[test]
fn assembly_rejects_a_gapped_mismatched_or_unterminated_stream() {
    use crate::transport::convert::{assemble_snapshot, snapshot_chunks};

    let msg = Message::InstallSnapshot {
        term: 3,
        leader_id: 1,
        last_included_index: 50,
        last_included_term: 2,
        data: vec![7; 300_000],
        config: kv_raft::ClusterConfig::voting([1, 2]),
    };
    let chunks = snapshot_chunks(&msg);

    let mut gapped = chunks.clone();
    gapped.remove(1);
    assert!(assemble_snapshot(&gapped).is_err(), "a dropped chunk must not assemble");

    let mut mismatched = chunks.clone();
    mismatched[1].last_included_index = 51;
    assert!(assemble_snapshot(&mismatched).is_err(), "a foreign chunk must not assemble");

    let mut unterminated = chunks.clone();
    unterminated.last_mut().unwrap().done = false;
    assert!(assemble_snapshot(&unterminated).is_err(), "a stream with no end must not assemble");

    assert!(assemble_snapshot(&[]).is_err(), "an empty stream carries no snapshot");
}
