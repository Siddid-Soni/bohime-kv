//! M3.6 — the election restriction (§5.4.1) and the Leader Completeness
//! property it exists to buy.
//!
//! The rule itself lives in `election::should_grant_vote`; these tests assert
//! it holds at the cluster level, where a stale candidate actually campaigns
//! and actually loses.

use proptest::prelude::*;

use crate::message::Role;
use crate::tests::harness::Cluster;
use crate::types::{Entry, LogIndex, Term};

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: vec![index as u8] }
}

/// A cluster where only `campaigner` can time out, so exactly one node runs
/// for office and the result is unambiguous.
fn cluster_with_only(campaigner: u64) -> Cluster {
    let mut cluster = Cluster::of_three();
    for node in cluster.nodes.iter_mut() {
        if node.id() == campaigner {
            node.set_election_timeout_for_tests(2);
        } else {
            node.set_election_timeout_for_tests(100_000);
        }
    }
    cluster
}

/// Runs `cluster` for `ticks`, returning every node that ever held leadership.
///
/// Sampling every tick matters: a stale candidate that briefly won and was then
/// deposed would still be a Leader Completeness violation, and a check only at
/// the end would miss it.
fn leaders_seen(cluster: &mut Cluster, ticks: usize) -> Vec<u64> {
    let mut seen = Vec::new();
    for _ in 0..ticks {
        cluster.tick_all();
        for id in cluster.leaders() {
            if !seen.contains(&id) {
                seen.push(id);
            }
        }
    }
    seen
}

#[test]
fn stale_log_candidate_cannot_win_against_up_to_date_voters() {
    let mut cluster = cluster_with_only(1);
    let fresh = vec![entry(1, 1), entry(2, 1), entry(3, 2)];
    for node in cluster.nodes.iter_mut() {
        if node.id() == 1 {
            // Node 1 is missing index 3, so its last term is behind.
            node.replace_log_for_tests(vec![entry(1, 1), entry(2, 1)]);
        } else {
            node.replace_log_for_tests(fresh.clone());
        }
    }

    let seen = leaders_seen(&mut cluster, 200);

    assert!(!seen.contains(&1), "a stale log won an election: leaders seen {seen:?}");
    // Node 1 campaigning bumps the others' terms, which legitimately resets
    // their election timers — so an up-to-date node takes over. That it does is
    // what proves the votes were actually flowing.
    assert!(!seen.is_empty(), "no leader at all means no votes were exchanged");
}

/// The control for the test above: identical setup and tick budget, with one
/// variable changed — node 1's log is current. Without this, the test above
/// would also pass if node 1 simply never managed to campaign.
#[test]
fn the_same_candidate_wins_once_its_log_is_current() {
    let mut cluster = cluster_with_only(1);
    let fresh = vec![entry(1, 1), entry(2, 1), entry(3, 2)];
    for node in cluster.nodes.iter_mut() {
        node.replace_log_for_tests(fresh.clone());
    }

    let seen = leaders_seen(&mut cluster, 200);

    assert!(seen.contains(&1), "an up-to-date candidate must win: leaders seen {seen:?}");
}

#[derive(Debug, Clone)]
enum Op {
    /// Propose on whichever node currently leads; a no-op if there is none.
    Propose,
    Tick(u8),
    /// Force `node` to time out, driving a term bump and a leader change.
    ForceElection(u8),
    /// Drop `node`'s uncommitted tail, modelling a follower that was
    /// partitioned away and missed entries. Without divergence like this the
    /// cluster never reaches a state where the election restriction matters.
    FallBehind(u8, u8),
    /// Partition `node` away, so the rest of the cluster commits without it.
    /// This is the state that makes the election restriction matter: a node
    /// that missed committed entries and then campaigns.
    Isolate(u8),
    Heal(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => Just(Op::Propose),
        3 => (1u8..6).prop_map(Op::Tick),
        1 => (0u8..3).prop_map(Op::ForceElection),
        2 => (0u8..3, 0u8..6).prop_map(|(n, keep)| Op::FallBehind(n, keep)),
        2 => (0u8..3).prop_map(Op::Isolate),
        2 => (0u8..3).prop_map(Op::Heal),
    ]
}

proptest! {
    /// Leader Completeness: an entry committed in some term is present in the
    /// log of every leader of every later term. Checked after every operation,
    /// against every node that currently believes it leads.
    #[test]
    fn leader_completeness_holds_over_random_histories(
        ops in proptest::collection::vec(op_strategy(), 1..40),
    ) {
        let mut cluster = Cluster::of_three();
        // (entry, term it was committed in) — Leader Completeness only
        // constrains leaders of *higher* terms than the committing one.
        let mut committed: Vec<(Entry, Term)> = Vec::new();
        let mut next_cmd = 0u8;

        for op in &ops {
            match *op {
                Op::Propose => {
                    let leader = cluster.leaders().first().copied();
                    if let Some(id) = leader {
                        next_cmd = next_cmd.wrapping_add(1);
                        let node = cluster.nodes.iter_mut().find(|n| n.id() == id).unwrap();
                        let _ = node.propose(vec![next_cmd]);
                    }
                }
                Op::Tick(n) => {
                    for _ in 0..n {
                        cluster.tick_all();
                    }
                }
                Op::ForceElection(idx) => {
                    let node = &mut cluster.nodes[idx as usize % 3];
                    node.set_election_timeout_for_tests(1);
                    cluster.tick_all();
                }
                Op::Isolate(idx) => {
                    let id = cluster.nodes[idx as usize % 3].id();
                    cluster.isolate(id);
                }
                Op::Heal(idx) => {
                    let id = cluster.nodes[idx as usize % 3].id();
                    cluster.heal(id);
                }
                Op::FallBehind(idx, keep) => {
                    // Truncation may only remove an *uncommitted* suffix. The
                    // floor is the highest index committed anywhere in the
                    // cluster, not this node's local commit_index: that lags,
                    // and using it would let this op strip a committed entry
                    // from a majority, which no real Raft run can do. Doing so
                    // destroys the precondition of Leader Completeness and the
                    // property then fails for a state that cannot occur.
                    let floor = committed.iter().map(|(e, _)| e.index).max().unwrap_or(0) as usize;
                    let node = &mut cluster.nodes[idx as usize % 3];
                    let log = node.log_entries();
                    let keep = (keep as usize).max(floor).min(log.len());
                    node.replace_log_for_tests(log[..keep].to_vec());
                }
            }

            // Anything any node reports committed is committed, permanently.
            for node in cluster.nodes.iter_mut() {
                let at_term = node.current_term();
                for e in node.ready().committed {
                    if !committed.iter().any(|(c, _)| c.index == e.index) {
                        committed.push((e, at_term));
                    }
                }
            }

            for node in cluster.nodes.iter() {
                if node.role() != Role::Leader {
                    continue;
                }
                let log = node.log_entries();
                for (c, commit_term) in &committed {
                    // A leader of a term at or below the committing term is a
                    // deposed leader that has not learned about the entry yet;
                    // the property says nothing about it.
                    if node.current_term() <= *commit_term {
                        continue;
                    }
                    let found = log.iter().find(|e| e.index == c.index);
                    prop_assert_eq!(
                        found,
                        Some(c),
                        "leader {} of term {} is missing entry {:?} committed in term {}",
                        node.id(),
                        node.current_term(),
                        c,
                        commit_term
                    );
                }
            }
        }
    }
}
