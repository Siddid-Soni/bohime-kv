//! A group of one. Legitimate on its own (the M6 driver test runs one), and
//! the degenerate end of M9's shrink. Before M6, `try_advance_commit` was
//! reachable only from an AppendEntries response, which a group with no peers
//! never receives — so it elected itself and then committed nothing, forever.

use crate::message::{Config, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;

fn solo() -> RaftNode<MemStorage> {
    let config =
        Config { id: 1, peers: vec![], election_timeout: 10, heartbeat_interval: 2, seed: 1 };
    RaftNode::new(config, MemStorage::default())
}

#[test]
fn a_lone_node_elects_itself() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    assert_eq!(node.role(), Role::Leader);
}

#[test]
fn a_lone_leader_commits_its_own_no_op() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    // The no-op is index 1, and a quorum of one already holds it.
    assert_eq!(node.commit_index(), 1, "a group of one is its own majority");
}

#[test]
fn a_lone_leader_commits_and_applies_a_proposal() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    let _ = node.ready();

    let index = node.propose(b"set x 1".to_vec()).expect("the lone node leads");
    let ready = node.ready();

    assert_eq!(node.commit_index(), index);
    let applied: Vec<_> = ready.committed.iter().map(|e| e.command.clone()).collect();
    assert_eq!(applied, vec![b"set x 1".to_vec()], "it must reach the state machine");
}

/// A group of one is its own read quorum. The same shape of bug as the commit
/// rule had: confirmation only ran on receiving an AppendEntries ack, which a
/// node with no peers never gets, so every read hung forever.
#[test]
fn a_lone_leader_confirms_a_read_immediately() {
    let mut node = solo();
    for _ in 0..25 {
        node.tick();
    }
    let _ = node.ready();

    node.read_index(7).expect("a lone caught-up leader can serve a read");
    let ready = node.ready();
    assert_eq!(ready.read_states.len(), 1, "no peer will ever ack; it must confirm itself");
    assert_eq!(ready.read_states[0].token, 7);
    assert_eq!(ready.read_states[0].index, node.commit_index());
}
