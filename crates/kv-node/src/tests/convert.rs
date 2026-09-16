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
        )
            .prop_map(
                |(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)| {
                    Message::AppendEntries {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    }
                }
            ),
        (
            any::<u64>(),
            any::<bool>(),
            any::<u64>(),
            proptest::option::of(any::<u64>()),
            proptest::option::of(any::<u64>()),
        )
            .prop_map(|(term, success, match_index, conflict_term, conflict_index)| {
                Message::AppendEntriesResp {
                    term,
                    success,
                    match_index,
                    conflict_term,
                    conflict_index,
                }
            }),
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
                    }
                }
            ),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, success)| Message::InstallSnapshotResp { term, success }),
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
    };
    let back = Message::try_from(Outbound::from(msg.clone())).unwrap();
    assert_eq!(back, msg);

    let with_zero = Message::AppendEntriesResp {
        term: 4,
        success: false,
        match_index: 0,
        conflict_term: Some(0),
        conflict_index: Some(0),
    };
    let back_zero = Message::try_from(Outbound::from(with_zero.clone())).unwrap();
    assert_eq!(back_zero, with_zero);
    assert_ne!(back, back_zero, "Some(0) and None must not collapse onto each other");
}

#[test]
fn a_partial_snapshot_chunk_is_rejected_not_silently_accepted() {
    // Until M8 does real chunking, a fragment must be an error rather than be
    // mistaken for a whole snapshot — which would install a truncated state
    // machine and look like corruption.
    let chunk = kv_proto::raft::InstallSnapshotChunk {
        term: 3,
        leader_id: 1,
        last_included_index: 50,
        last_included_term: 2,
        offset: 4096,
        data: vec![7; 16],
        done: false,
    };
    assert_eq!(
        Message::try_from(Outbound::InstallSnapshot(chunk)),
        Err(ConvertError::ChunkedSnapshot { offset: 4096 })
    );
}
