//! ReadIndex (M7): serving a linearizable read without writing to the log.
//!
//! The leader records its commit index, confirms with a heartbeat quorum that
//! it still leads, and only then may the caller read. The confirmation is the
//! whole point — without it a deposed leader that has not yet heard about the
//! election serves whatever it last applied.

use crate::message::{Config, Message, ReadIndexError, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;
use crate::types::{LogIndex, NodeId};

fn config(id: NodeId) -> Config {
    Config { id, peers: vec![2, 3], election_timeout: 10, heartbeat_interval: 2, seed: id }
}

/// A node that has won an election. Its no-op is in the log at index 1 but is
/// not yet committed, because no peer has acked it.
fn fresh_leader() -> RaftNode<MemStorage> {
    let mut node = RaftNode::new(config(1), MemStorage::default());
    for _ in 0..25 {
        node.tick();
    }
    assert_eq!(node.role(), Role::Candidate);
    node.step(2, Message::RequestVoteResp { term: node.current_term(), vote_granted: true });
    assert_eq!(node.role(), Role::Leader);
    node
}

/// Acks `match_index` from `peer`, echoing `round`.
fn ack(node: &mut RaftNode<MemStorage>, peer: NodeId, match_index: LogIndex, round: Option<u64>) {
    node.step(
        peer,
        Message::AppendEntriesResp {
            term: node.current_term(),
            success: true,
            match_index,
            conflict_term: None,
            conflict_index: None,
            read_round: round,
        },
    );
}

/// The round the leader stamped on its most recent outbound AppendEntries.
fn round_in_flight(node: &mut RaftNode<MemStorage>) -> Option<u64> {
    node.ready().messages.iter().rev().find_map(|(_, msg)| match msg {
        Message::AppendEntries { read_round, .. } => Some(*read_round),
        _ => None,
    })?
}

/// A leader whose own-term no-op has committed, so it is allowed to serve
/// reads at all.
fn caught_up_leader() -> RaftNode<MemStorage> {
    let mut node = fresh_leader();
    ack(&mut node, 2, 1, None);
    assert_eq!(node.commit_index(), 1, "the no-op should commit on a quorum ack");
    let _ = node.ready();
    node
}

#[test]
fn a_follower_refuses_a_read() {
    let mut node = RaftNode::new(config(1), MemStorage::default());
    assert_eq!(node.read_index(7), Err(ReadIndexError::NotLeader));
}

/// Until a leader has committed an entry in its *own* term, its commit index
/// may not reflect everything it is required to hold — the figure-8 case. The
/// no-op appended on election is what resolves this, and until it commits the
/// leader must not serve reads.
#[test]
fn a_leader_that_has_not_committed_in_its_own_term_refuses() {
    let mut node = fresh_leader();
    assert_eq!(node.commit_index(), 0);
    assert_eq!(node.read_index(7), Err(ReadIndexError::NoQuorumInTerm));
}

#[test]
fn a_quorum_acking_the_round_confirms_the_read() {
    let mut node = caught_up_leader();

    assert_eq!(node.read_index(7), Ok(()));
    let round = round_in_flight(&mut node).expect("the read broadcasts a stamped heartbeat");

    // Nothing is confirmed on the strength of the leader's own vote alone.
    assert!(node.ready().read_states.is_empty());

    // One peer ack makes two of three: a quorum.
    ack(&mut node, 2, 1, Some(round));
    let ready = node.ready();
    assert_eq!(ready.read_states.len(), 1);
    assert_eq!(ready.read_states[0].token, 7);
    assert_eq!(ready.read_states[0].index, 1);
}

/// The reason `read_round` exists. An ack already in flight when the read
/// arrived proves the node led at some *earlier* instant; it could have been
/// deposed in between. Counting it is a stale read that shows up only under a
/// partition — exactly the bug M7 exists to remove.
#[test]
fn an_ack_from_before_the_read_does_not_confirm_it() {
    let mut node = caught_up_leader();

    assert_eq!(node.read_index(7), Ok(()));
    let round = round_in_flight(&mut node).expect("a stamped heartbeat");

    // An ack carrying no round at all: a heartbeat sent before any read.
    ack(&mut node, 2, 1, None);
    assert!(node.ready().read_states.is_empty(), "an unstamped ack must not confirm a read");

    // An ack stamped with an earlier round: an older read's heartbeat.
    let earlier = round.wrapping_sub(1);
    ack(&mut node, 3, 1, Some(earlier));
    assert!(
        node.ready().read_states.is_empty(),
        "an ack echoing an older round must not confirm a newer read"
    );

    // The current round does confirm it, so the test above is not passing
    // merely because confirmation is broken outright.
    ack(&mut node, 2, 1, Some(round));
    assert_eq!(node.ready().read_states.len(), 1);
}

/// The index is the commit index as of the *request*, not of the confirmation.
/// Reading at a later index would be serving a value the read did not ask for.
#[test]
fn the_read_records_the_commit_index_at_request_time() {
    let mut node = caught_up_leader();

    assert_eq!(node.read_index(7), Ok(()));
    let round = round_in_flight(&mut node).expect("a stamped heartbeat");

    // The log moves on while the read is in flight.
    node.propose(b"later".to_vec()).unwrap();
    let _ = node.ready();

    // One ack both commits index 2 and confirms the read.
    ack(&mut node, 2, 2, Some(round));
    let ready = node.ready();
    assert_eq!(node.commit_index(), 2, "the ack should also advance the commit index");
    assert_eq!(ready.read_states.len(), 1);
    assert_eq!(ready.read_states[0].index, 1, "the read was recorded before index 2 existed");
}

/// Losing leadership abandons every outstanding read. Confirming one
/// afterwards would be confirming leadership we no longer have.
#[test]
fn stepping_down_drops_outstanding_reads() {
    let mut node = caught_up_leader();
    assert_eq!(node.read_index(7), Ok(()));
    let round = round_in_flight(&mut node).expect("a stamped heartbeat");

    // A higher term deposes us.
    node.step(
        3,
        Message::RequestVote { term: 99, candidate_id: 3, last_log_index: 5, last_log_term: 99 },
    );
    assert_eq!(node.role(), Role::Follower);

    ack(&mut node, 2, 1, Some(round));
    assert!(node.ready().read_states.is_empty(), "a deposed leader confirms nothing");
}
