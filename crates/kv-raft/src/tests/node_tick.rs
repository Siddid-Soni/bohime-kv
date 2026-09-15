use super::*;
use crate::message::{Action, Config, Message, Role};
use crate::storage::MemStorage;

fn config(id: u64, peers: Vec<u64>) -> Config {
    Config { id, peers, election_timeout: 10, heartbeat_interval: 2, seed: 42 }
}

#[test]
fn follower_becomes_candidate_within_randomized_timeout_window() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());

    // Below the base timeout nothing may happen — randomization only extends.
    for _ in 0..10 {
        let actions = node.tick();
        assert!(actions.is_empty());
        assert_eq!(node.role(), Role::Follower);
    }

    // The timeout is uniform in [10, 20): by tick 20 an election must fire.
    let mut actions = vec![];
    for _ in 10..20 {
        actions = node.tick();
        if node.role() == Role::Candidate {
            break;
        }
        assert!(actions.is_empty(), "no actions before the election fires");
    }
    assert_eq!(node.role(), Role::Candidate, "candidate within [timeout, 2*timeout)");
    let requests: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { to, msg } => Some((*to, msg.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(requests.len(), 2, "candidate must solicit every peer");
    for (to, msg) in &requests {
        match msg {
            Message::RequestVote { term, candidate_id, .. } => {
                assert_eq!(*term, 1);
                assert_eq!(*candidate_id, 1);
            }
            other => panic!("expected RequestVote, got {other:?}"),
        }
        assert!(*to == 2 || *to == 3);
    }
    assert!(
        actions.iter().any(|a| matches!(a, Action::PersistHardState(_))),
        "term and self-vote must be persisted before soliciting"
    );
}

#[test]
fn heartbeat_suppresses_election_over_10k_ticks() {
    let mut node = RaftNode::new(config(1, vec![2, 3]), MemStorage::default());

    for _ in 0..10_000 {
        node.step(
            2,
            Message::AppendEntries {
                term: 0,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        );
        assert_eq!(node.role(), Role::Follower);
    }
}

#[test]
fn leader_never_starts_an_election() {
    let mut node = RaftNode::new(config(1, vec![]), MemStorage::default());

    for _ in 0..25 {
        node.tick();
        if node.role() == Role::Leader {
            break;
        }
    }
    assert_eq!(node.role(), Role::Leader, "single node must elect itself");

    // A leader counts heartbeats, not elections: no RequestVote may ever fire.
    for _ in 0..100 {
        let actions = node.tick();
        assert_eq!(node.role(), Role::Leader);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::Send { msg: Message::RequestVote { .. }, .. })),
            "leader must not solicit votes"
        );
    }
}

#[test]
fn randomized_timeouts_differ_across_seeds() {
    let mut timeouts = std::collections::HashSet::new();
    for seed in 0..20 {
        let cfg =
            Config { id: 1, peers: vec![2, 3], election_timeout: 100, heartbeat_interval: 5, seed };
        let mut node = RaftNode::new(cfg, MemStorage::default());
        let mut ticks = 0;
        while node.role() == Role::Follower {
            node.tick();
            ticks += 1;
        }
        timeouts.insert(ticks);
    }
    assert!(timeouts.len() > 1, "randomized timeouts must vary with seed");
}
