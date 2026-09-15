use crate::harness::{Cluster, config};
use crate::message::{Action, Message, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;
use crate::types::{Entry, LogIndex, Term};

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: vec![index as u8] }
}

/// A single leader with suppressed followers: followers never time out, so
/// the leader stays put while replication is driven manually.
fn stable_cluster() -> (Cluster, u64) {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);
    for node in cluster.nodes.iter_mut() {
        if node.id() != leader {
            node.set_election_timeout_for_tests(100_000);
        }
    }
    (cluster, leader)
}

/// Paper figure 7: six divergent follower logs, each must converge to the
/// leader's log through rejection and retry.
#[test]
fn divergent_follower_repair_converges() {
    let leader_log = vec![entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2), entry(5, 3)];
    let followers = vec![
        vec![entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2)],
        vec![entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2), entry(5, 3), entry(6, 3)],
        vec![entry(1, 1), entry(2, 1), entry(3, 2)],
        vec![entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 2)],
        vec![entry(1, 1), entry(2, 1), entry(3, 1)],
        vec![entry(1, 2), entry(2, 2)],
    ];

    for (case, follower_log) in followers.into_iter().enumerate() {
        let (mut cluster, leader) = stable_cluster();
        for node in cluster.nodes.iter_mut() {
            if node.id() == leader {
                node.replace_log_for_tests(leader_log.clone());
            } else {
                node.replace_log_for_tests(follower_log.clone());
            }
        }

        for _ in 0..500 {
            cluster.tick_all();
        }

        for node in &cluster.nodes {
            assert_eq!(node.log_entries(), leader_log, "figure-7 case {case} did not converge");
        }
    }
}

struct Pair {
    leader: RaftNode<MemStorage>,
    follower: RaftNode<MemStorage>,
    append_round_trips: u64,
}

impl Pair {
    /// Elect on empty logs, then install the divergent logs for the test.
    fn with_divergent_logs() -> Self {
        let mut leader = RaftNode::new(config(1, vec![2], 10_000), MemStorage::default());
        let mut follower = RaftNode::new(config(2, vec![1], 100_000), MemStorage::default());
        for _ in 0..20_000 {
            let mut solicited = false;
            for action in leader.tick() {
                if let Action::Send { to, msg } = action {
                    assert_eq!(to, 2);
                    for r in follower.step(1, msg) {
                        if let Action::Send { msg: m, .. } = r {
                            leader.step(2, m);
                        }
                    }
                    solicited = true;
                }
            }
            if solicited && leader.role() == Role::Leader {
                break;
            }
        }
        assert_eq!(leader.role(), Role::Leader);

        let mut long: Vec<Entry> = (1..=10).map(|i| entry(i, 1)).collect();
        long.push(entry(11, 2));
        long.push(entry(12, 2));
        leader.replace_log_for_tests(long);
        follower.replace_log_for_tests((1..=5).map(|i| entry(i, 1)).collect());
        // Start sending from the end so the first request is rejected and the
        // conflict hint does the work; naive decrement would take ~8 rounds.
        leader.set_next_index_for_tests(2, 13);
        Self { leader, follower, append_round_trips: 0 }
    }

    fn round(&mut self) {
        let mut sent_append = false;
        for action in self.leader.tick() {
            if let Action::Send { to, msg } = action {
                assert_eq!(to, 2);
                if matches!(msg, Message::AppendEntries { .. }) {
                    sent_append = true;
                }
                for r in self.follower.step(1, msg) {
                    if let Action::Send { msg: m, .. } = r {
                        self.leader.step(2, m);
                    }
                }
            }
        }
        if sent_append {
            self.append_round_trips += 1;
        }
    }
}

/// The conflict hint must converge in ≤2 round trips (reject + resend) on a
/// log where naive next_index decrement takes ~8.
#[test]
fn conflict_hint_converges_in_two_round_trips() {
    let mut pair = Pair::with_divergent_logs();
    for _ in 0..50 {
        pair.round();
        if pair.follower.log_entries() == pair.leader.log_entries() {
            break;
        }
    }
    assert_eq!(pair.follower.log_entries(), pair.leader.log_entries());
    assert!(
        pair.append_round_trips <= 2,
        "conflict hint must converge in ≤2 round trips, took {}",
        pair.append_round_trips
    );
}
