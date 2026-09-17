//! The leader hint (M6). `NotLeader { leader_hint }` is how a client finds
//! the leader, and the hint has to come from somewhere the core actually
//! knows: `voted_for` is not it — that is who we voted for, who often lost.

use crate::message::{Config, Message, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;

fn config(id: u64, peers: Vec<u64>) -> Config {
    Config {
        id,
        peers,
        election_timeout: 10,
        heartbeat_interval: 2,
        seed: id,
        initial_learner: false,
    }
}

fn append_from(leader: u64, term: u64) -> Message {
    Message::AppendEntries {
        term,
        leader_id: leader,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        read_round: None,
    }
}

#[test]
fn a_fresh_follower_knows_no_leader() {
    let node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    assert_eq!(node.leader_id(), None);
}

#[test]
fn accepting_append_entries_records_the_leader() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    node.step(2, append_from(2, 4));
    assert_eq!(node.leader_id(), Some(2));
}

#[test]
fn a_stale_leaders_append_is_not_believed() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    // Reach term 7 by hearing from the real leader.
    node.step(3, append_from(3, 7));
    // Node 2 still thinks it leads at term 4. Believing it would send every
    // client to a deposed leader.
    node.step(2, append_from(2, 4));
    assert_eq!(node.leader_id(), Some(3), "a lower-term append must not move the hint");
}

#[test]
fn winning_an_election_points_the_hint_at_ourselves() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    for _ in 0..25 {
        node.tick();
    }
    assert_eq!(node.role(), Role::Candidate);
    // A candidate has no leader to name.
    assert_eq!(node.leader_id(), None);

    node.step(2, Message::RequestVoteResp { term: node.current_term(), vote_granted: true });
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(node.leader_id(), Some(1));
}

#[test]
fn a_higher_term_clears_the_hint() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());
    node.step(2, append_from(2, 4));
    assert_eq!(node.leader_id(), Some(2));

    // A new election is underway at a higher term; the old leader is gone and
    // we must stop directing clients to it.
    node.step(
        3,
        Message::RequestVote { term: 9, candidate_id: 3, last_log_index: 0, last_log_term: 0 },
    );
    assert_eq!(node.leader_id(), None, "a higher term means the old leader is stale");
}
