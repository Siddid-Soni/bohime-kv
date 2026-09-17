use crate::message::{Action, Message, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;
use crate::tests::harness::{Cluster, config};

#[test]
fn candidate_with_majority_becomes_leader() {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);
    assert!([1, 2, 3].contains(&leader));
    assert_eq!(cluster.leaders().len(), 1);
}

#[test]
fn single_leader_is_stable_over_time() {
    let mut cluster = Cluster::of_three();
    cluster.run_until_leader(500);
    for _ in 0..200 {
        cluster.tick_all();
    }
    assert_eq!(cluster.leaders().len(), 1, "exactly one leader must persist");
}

#[test]
fn never_votes_twice_in_one_term() {
    let mut node = RaftNode::new(config(1, vec![2, 3], 7), MemStorage::default());

    let grant = node.step(
        2,
        Message::RequestVote { term: 1, candidate_id: 2, last_log_index: 0, last_log_term: 0 },
    );
    assert!(
        grant.iter().any(|a| matches!(
            a,
            Action::Send { msg: Message::RequestVoteResp { vote_granted: true, .. }, .. }
        )),
        "first candidate gets the vote"
    );

    let deny = node.step(
        3,
        Message::RequestVote { term: 1, candidate_id: 3, last_log_index: 0, last_log_term: 0 },
    );
    assert!(
        deny.iter().any(|a| matches!(
            a,
            Action::Send { msg: Message::RequestVoteResp { vote_granted: false, .. }, .. }
        )),
        "second candidate in the same term must be refused"
    );
}

#[test]
fn vote_is_persisted_before_the_grant_is_sent() {
    let mut node = RaftNode::new(config(1, vec![2, 3], 7), MemStorage::default());

    let actions = node.step(
        2,
        Message::RequestVote { term: 1, candidate_id: 2, last_log_index: 0, last_log_term: 0 },
    );

    let persist_pos = actions.iter().position(|a| matches!(a, Action::PersistHardState(_)));
    let send_pos = actions
        .iter()
        .position(|a| matches!(a, Action::Send { msg: Message::RequestVoteResp { .. }, .. }));
    match (persist_pos, send_pos) {
        (Some(p), Some(s)) => assert!(p < s, "disk before network: vote persisted first"),
        _ => panic!("expected both a persist and a vote response, got {actions:?}"),
    }
}

#[test]
fn higher_term_steps_down_a_leader() {
    let mut cluster = Cluster::of_three();
    let leader_id = cluster.run_until_leader(500);
    let leader = cluster.nodes.iter().find(|n| n.id() == leader_id).unwrap();
    let term = leader.current_term();
    // The higher term must arrive from a *member*: since M9, a message from a
    // node outside the config is dropped before the term rules run, so a made-up
    // sender id would assert nothing here. `stranger_messages_do_not_force_a_step_down`
    // covers that case on purpose.
    let usurper = cluster.nodes.iter().find(|n| n.id() != leader_id).unwrap().id();

    let node = cluster.nodes.iter_mut().find(|n| n.id() == leader_id).unwrap();
    node.step(
        usurper,
        Message::AppendEntries {
            term: term + 1,
            leader_id: usurper,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            read_round: None,
        },
    );
    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), term + 1);
}

#[test]
fn higher_term_steps_down_a_candidate() {
    let mut node = RaftNode::new(config(1, vec![2, 3], 7), MemStorage::default());
    for _ in 0..30 {
        node.tick();
        if node.role() == Role::Candidate {
            break;
        }
    }
    assert_eq!(node.role(), Role::Candidate);

    node.step(
        2,
        Message::RequestVote { term: 5, candidate_id: 2, last_log_index: 0, last_log_term: 0 },
    );
    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), 5);
}
