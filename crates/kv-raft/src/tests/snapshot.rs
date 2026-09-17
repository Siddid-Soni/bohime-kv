//! InstallSnapshot at the core (M8): the leader ships a snapshot when a
//! follower's `next_index` points inside the compacted prefix, and the
//! follower installs it into storage plus `Ready::snapshot` for the driver.

use crate::message::{Action, Message};
use crate::storage::RaftStorage;
use crate::tests::harness::Cluster;
use crate::types::Entry;

fn takes_snapshot(actions: &[Action]) -> bool {
    actions.iter().any(|a| matches!(a, Action::ApplySnapshot(_)))
}

/// The M8.1 trap at the send path: once the leader compacts past a follower's
/// `next_index`, `prev_log_term` no longer exists — sending `0` there makes
/// the follower reject forever. The leader must send a snapshot instead.
#[test]
fn leader_sends_snapshot_not_a_zero_term_appendentries_when_next_is_below_first() {
    let mut cluster = Cluster::of_n(&[1, 2]);
    let leader = cluster.run_until_leader(500);
    let peer = if leader == 1 { 2 } else { 1 };

    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    cluster.nodes[li].propose(b"x".to_vec()).unwrap();
    for _ in 0..50 {
        cluster.tick_all();
    }
    // No-op at 1, `x` at 2, both committed on both nodes.
    cluster.nodes[li].compact_prefix_for_tests(2);

    // The peer falls behind: its next pointer still aims at the prefix.
    cluster.nodes[li].set_next_index_for_tests(peer, 1);
    let mut to_peer = Vec::new();
    for _ in 0..3 {
        for action in cluster.nodes[li].tick() {
            if let Action::Send { to, msg } = action
                && to == peer
            {
                to_peer.push(msg);
            }
        }
    }

    let last = to_peer.last().expect("a heartbeat must go out");
    match last {
        Message::InstallSnapshot { last_included_index, last_included_term, .. } => {
            assert_eq!(*last_included_index, 2);
            assert_eq!(*last_included_term, 1);
        }
        other => panic!("expected InstallSnapshot, got {other:?}"),
    }
    assert!(
        !to_peer.iter().any(|m| matches!(
            m,
            Message::AppendEntries { prev_log_term: 0, prev_log_index: p, .. } if *p > 0
        )),
        "no AppendEntries with a fabricated zero term may go out"
    );
}

#[test]
fn follower_installs_snapshot_and_reports_it_in_ready() {
    use crate::membership::ClusterConfig;
    use crate::message::Config;
    use crate::node::RaftNode;
    use crate::storage::MemStorage;

    let mut follower = RaftNode::new(
        Config {
            id: 2,
            peers: vec![1],
            election_timeout: 10,
            heartbeat_interval: 2,
            seed: 7,
            initial_learner: false,
        },
        MemStorage::default(),
    );
    let term = 3;
    let actions = follower.step(
        1,
        Message::InstallSnapshot {
            term,
            leader_id: 1,
            last_included_index: 2,
            last_included_term: 1,
            data: vec![9; 4],
            config: ClusterConfig::voting([1, 2]),
        },
    );

    let resp = actions
        .iter()
        .find_map(|a| match a {
            Action::Send { msg: Message::InstallSnapshotResp { term, success }, .. } => {
                Some((*term, *success))
            }
            _ => None,
        })
        .expect("an InstallSnapshotResp must go out");
    assert_eq!(resp, (term, true));
    assert!(takes_snapshot(&actions));

    let snap = follower.storage().snapshot().unwrap().expect("stored");
    assert_eq!(snap.last_included_index, 2);
    assert_eq!(follower.storage().first_index().unwrap(), 3);

    let ready = follower.ready();
    let snap = ready.snapshot.expect("driver must restore the state machine");
    assert_eq!(snap.last_included_index, 2);
    assert!(ready.committed.is_empty(), "nothing below the snapshot is delivered as entries");
}

/// A snapshot at or below the follower's commit is already applied state.
/// Ack it so the leader advances, but install nothing.
#[test]
fn stale_snapshot_is_acked_without_installing() {
    use crate::membership::ClusterConfig;
    use crate::message::Config;
    use crate::node::RaftNode;
    use crate::storage::{MemStorage, RaftStorage};

    let mut follower = RaftNode::new(
        Config {
            id: 2,
            peers: vec![1],
            election_timeout: 10,
            heartbeat_interval: 2,
            seed: 7,
            initial_learner: false,
        },
        MemStorage::default(),
    );
    follower.step(
        1,
        Message::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: (1..=5).map(|i| Entry { term: 1, index: i, command: vec![i as u8] }).collect(),
            leader_commit: 5,
            read_round: None,
        },
    );
    assert_eq!(follower.commit_index(), 5);

    let actions = follower.step(
        1,
        Message::InstallSnapshot {
            term: 1,
            leader_id: 1,
            last_included_index: 3,
            last_included_term: 1,
            data: vec![1],
            config: ClusterConfig::voting([1, 2]),
        },
    );
    let success = actions
        .iter()
        .find_map(|a| match a {
            Action::Send { msg: Message::InstallSnapshotResp { success, .. }, .. } => {
                Some(*success)
            }
            _ => None,
        })
        .expect("a response must go out");
    assert!(success);
    assert!(!takes_snapshot(&actions));
    assert!(follower.storage().snapshot().unwrap().is_none());
}

/// A fully-compacted node still campaigns on its snapshot boundary, not as an
/// empty log — otherwise the election restriction lets a stale node win.
#[test]
fn fully_compacted_node_campaigns_on_its_snapshot_boundary() {
    use crate::membership::ClusterConfig;
    use crate::storage::{MemStorage, RaftStorage};
    use crate::types::Snapshot;

    let mut s = MemStorage::default();
    s.append(&[
        Entry { term: 1, index: 1, command: vec![1] },
        Entry { term: 2, index: 2, command: vec![2] },
    ])
    .unwrap();
    s.save_snapshot(&Snapshot {
        last_included_index: 2,
        last_included_term: 2,
        data: vec![],
        config: ClusterConfig::voting([1, 2]),
    })
    .unwrap();
    s.truncate_prefix(2).unwrap();

    assert_eq!(crate::log::last_log(&s), (2, 2));
}

/// A leader that has not compacted sends a prefix the follower already
/// snapshotted. The covered tail is accepted, not rejected, and nothing below
/// the snapshot is re-applied.
#[test]
fn appendentries_with_prev_below_the_snapshot_is_accepted() {
    use crate::message::Config;
    use crate::node::RaftNode;
    use crate::storage::MemStorage;

    let mut follower = RaftNode::new(
        Config {
            id: 2,
            peers: vec![1],
            election_timeout: 10,
            heartbeat_interval: 2,
            seed: 7,
            initial_learner: false,
        },
        MemStorage::default(),
    );
    follower.step(
        1,
        Message::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                Entry { term: 1, index: 1, command: vec![1] },
                Entry { term: 1, index: 2, command: vec![2] },
                Entry { term: 2, index: 3, command: vec![3] },
            ],
            leader_commit: 3,
            read_round: None,
        },
    );
    follower.compact_prefix_for_tests(2);

    let actions = follower.step(
        1,
        Message::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                Entry { term: 1, index: 2, command: vec![2] },
                Entry { term: 2, index: 3, command: vec![3] },
            ],
            leader_commit: 3,
            read_round: None,
        },
    );
    let (success, matched) = actions
        .iter()
        .find_map(|a| match a {
            Action::Send {
                msg: Message::AppendEntriesResp { success, match_index, .. }, ..
            } => Some((*success, *match_index)),
            _ => None,
        })
        .expect("a response must go out");
    assert!(success);
    assert_eq!(matched, 3);
    assert_eq!(follower.log_entries().len(), 1, "only the live tail is visible");
}

/// Taking a snapshot is not receiving one: the state machine already has the
/// state (the image was scanned from it), so nothing is reported to the
/// driver — the prefix is simply dropped. Out-of-range snapshots are refused,
/// not clamped: beyond the commit is unapplied state, at/below the base is a
/// regression.
#[test]
fn taking_a_snapshot_drops_the_prefix_and_reports_nothing() {
    use crate::types::Snapshot;

    let mut cluster = Cluster::of_n(&[1, 2]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    cluster.nodes[li].propose(b"x".to_vec()).unwrap();
    for _ in 0..50 {
        cluster.tick_all();
    }

    let live = cluster.nodes[li].cluster_config().clone();
    let taken = cluster.nodes[li].take_snapshot(Snapshot {
        last_included_index: 2,
        last_included_term: 1,
        data: vec![1, 2, 3],
        config: live.clone(),
    });
    assert!(taken);
    assert_eq!(cluster.nodes[li].storage().first_index().unwrap(), 3);
    assert!(cluster.nodes[li].ready().snapshot.is_none());

    // At the base: a regression, refused.
    let retaken = cluster.nodes[li].take_snapshot(Snapshot {
        last_included_index: 2,
        last_included_term: 1,
        data: vec![9],
        config: live.clone(),
    });
    assert!(!retaken);
    assert_eq!(cluster.nodes[li].storage().snapshot().unwrap().expect("kept").data, vec![1, 2, 3]);

    // Beyond the commit: unapplied state, refused.
    let future = cluster.nodes[li].take_snapshot(Snapshot {
        last_included_index: 99,
        last_included_term: 1,
        data: vec![],
        config: live,
    });
    assert!(!future);
}

/// The M8 gate in miniature at core level: a node partitioned away while the
/// group commits past it, then healed, catches up via snapshot and rejoins
/// with the same log tail as the leader.
#[test]
fn partitioned_follower_catches_up_via_snapshot_and_rejoins() {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);
    let victim = cluster.nodes.iter().find(|n| n.id() != leader).unwrap().id();
    let vi = cluster.nodes.iter().position(|n| n.id() == victim).unwrap();
    cluster.nodes[vi].set_election_timeout_for_tests(1_000_000);
    cluster.isolate(victim);

    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    cluster.nodes[li].propose(b"a".to_vec()).unwrap();
    cluster.nodes[li].propose(b"b".to_vec()).unwrap();
    for _ in 0..50 {
        cluster.tick_all();
    }
    // No-op at 1, `a` at 2, `b` at 3 — committed without the victim.
    cluster.nodes[li].compact_prefix_for_tests(3);
    cluster.nodes[li].propose(b"c".to_vec()).unwrap();
    for _ in 0..10 {
        cluster.tick_all();
    }

    cluster.heal(victim);
    for _ in 0..50 {
        cluster.tick_all();
    }

    let nodes = &cluster.nodes;
    let li = nodes.iter().position(|n| n.id() == leader).unwrap();
    let vi = nodes.iter().position(|n| n.id() == victim).unwrap();
    assert_eq!(
        nodes[vi].storage().snapshot().unwrap().as_ref().map(|s| s.last_included_index),
        Some(3)
    );
    assert_eq!(nodes[vi].log_entries(), nodes[li].log_entries());
    assert!(!nodes[vi].log_entries().is_empty(), "the post-snapshot tail replicated");
}
