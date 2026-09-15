//! M3.7 — each invariant gets a deliberately violating cluster state and must
//! reject it. A checker that has only ever seen healthy clusters is not known
//! to detect anything, so every test here asserts a specific `Violation`, not
//! merely that something was returned.

use proptest::prelude::*;

use crate::invariants::{NodeView, Violation, check_all, check_views};
use crate::message::Role;
use crate::tests::harness::Cluster;
use crate::types::{Entry, LogIndex, NodeId, Term};

fn entry(index: LogIndex, term: Term) -> Entry {
    Entry { term, index, command: vec![index as u8] }
}

/// A follower with `log` committed up to `commit_index`.
fn view(id: NodeId, term: Term, commit_index: LogIndex, log: Vec<Entry>) -> NodeView {
    NodeView { id, term, role: Role::Follower, commit_index, log }
}

fn leader(id: NodeId, term: Term, commit_index: LogIndex, log: Vec<Entry>) -> NodeView {
    NodeView { id, term, role: Role::Leader, commit_index, log }
}

#[test]
fn a_healthy_cluster_passes_every_invariant() {
    let log = vec![entry(1, 1), entry(2, 1), entry(3, 2)];
    let views = vec![
        leader(1, 2, 3, log.clone()),
        view(2, 2, 3, log.clone()),
        // Node 3 is simply behind, which is legal: a shorter prefix violates
        // nothing as long as what it has matches.
        view(3, 2, 2, log[..2].to_vec()),
    ];

    assert_eq!(check_views(&views), Ok(()));
}

#[test]
fn election_safety_catches_two_leaders_in_one_term() {
    let log = vec![entry(1, 4)];
    let views =
        vec![leader(1, 4, 1, log.clone()), leader(2, 4, 1, log.clone()), view(3, 4, 1, log)];

    assert_eq!(
        check_views(&views),
        Err(Violation::ElectionSafety { term: 4, leaders: vec![1, 2] })
    );
}

#[test]
fn election_safety_allows_leaders_in_different_terms() {
    // A deposed leader that has not yet learned it lost is not a violation.
    let views = vec![
        leader(1, 3, 1, vec![entry(1, 3)]),
        leader(2, 4, 1, vec![entry(1, 3)]),
        view(3, 4, 1, vec![entry(1, 3)]),
    ];

    assert_eq!(check_views(&views), Ok(()));
}

#[test]
fn log_matching_catches_equal_terms_over_differing_prefixes() {
    // Both nodes have term 5 at index 3, so by Log Matching everything before
    // index 3 must be identical — but index 2 differs.
    let a = vec![entry(1, 1), entry(2, 1), entry(3, 5)];
    let b = vec![entry(1, 1), entry(2, 4), entry(3, 5)];
    let views = vec![view(1, 5, 0, a), view(2, 5, 0, b), view(3, 5, 0, vec![])];

    assert_eq!(
        check_views(&views),
        Err(Violation::LogMatching { a: 1, b: 2, index: 3, term: 5, diverged_at: 2 })
    );
}

#[test]
fn log_matching_allows_divergent_suffixes_at_different_terms() {
    // Legal: the logs share a prefix and diverge at index 3 with *different*
    // terms, which is what an interrupted replication looks like.
    let a = vec![entry(1, 1), entry(2, 1), entry(3, 5)];
    let b = vec![entry(1, 1), entry(2, 1), entry(3, 6)];
    let views = vec![view(1, 6, 2, a), view(2, 6, 2, b), view(3, 6, 2, vec![])];

    assert_eq!(check_views(&views), Ok(()));
}

#[test]
fn leader_completeness_catches_a_leader_missing_a_committed_entry() {
    let committed = entry(2, 1);
    // Nodes 2 and 3 committed index 2 in term 1; node 1 leads term 3 without it.
    let views = vec![
        leader(1, 3, 1, vec![entry(1, 1)]),
        view(2, 3, 2, vec![entry(1, 1), committed.clone()]),
        view(3, 3, 2, vec![entry(1, 1), committed.clone()]),
    ];

    assert_eq!(
        check_views(&views),
        Err(Violation::LeaderCompleteness { leader: 1, leader_term: 3, expected: committed })
    );
}

#[test]
fn state_machine_safety_catches_divergent_committed_entries() {
    // Both nodes consider index 2 committed, with different entries — the
    // failure that loses an acknowledged write.
    let views = vec![
        view(1, 7, 2, vec![entry(1, 1), entry(2, 3)]),
        view(2, 7, 2, vec![entry(1, 1), entry(2, 4)]),
        view(3, 7, 1, vec![entry(1, 1)]),
    ];

    assert_eq!(
        check_views(&views),
        Err(Violation::StateMachineSafety {
            index: 2,
            a: 1,
            b: 2,
            a_entry: entry(2, 3),
            b_entry: entry(2, 4),
        })
    );
}

#[test]
fn check_all_accepts_a_real_cluster_at_every_step() {
    let mut cluster = Cluster::of_three();
    let leader_id = cluster.run_until_leader(500);

    for round in 0..40 {
        let node = cluster.nodes.iter_mut().find(|n| n.id() == leader_id);
        if let Some(node) = node {
            let _ = node.propose(vec![round as u8]);
        }
        cluster.tick_all();
        check_all(&cluster.nodes).expect("a correct run violates nothing");
    }
}

#[derive(Debug, Clone)]
enum Step {
    Propose,
    Tick(u8),
    ForceElection(u8),
    Isolate(u8),
    Heal(u8),
}

fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => Just(Step::Propose),
        3 => (1u8..6).prop_map(Step::Tick),
        1 => (0u8..3).prop_map(Step::ForceElection),
        2 => (0u8..3).prop_map(Step::Isolate),
        2 => (0u8..3).prop_map(Step::Heal),
    ]
}

proptest! {
    /// The M3 milestone gate: the suite must hold after *any* step, because M4
    /// calls it after every single one. Partitions and forced elections are in
    /// the mix deliberately — a checker only ever run against a quiescent,
    /// fully-connected cluster would not be known to avoid false positives on
    /// the legal-but-messy states a real run spends most of its time in.
    #[test]
    fn no_step_of_a_real_cluster_violates_any_invariant(
        steps in proptest::collection::vec(step_strategy(), 1..40),
    ) {
        let mut cluster = Cluster::of_three();
        let mut cmd = 0u8;

        for step in &steps {
            match *step {
                Step::Propose => {
                    if let Some(id) = cluster.leaders().first().copied() {
                        cmd = cmd.wrapping_add(1);
                        let node = cluster.nodes.iter_mut().find(|n| n.id() == id).unwrap();
                        let _ = node.propose(vec![cmd]);
                    }
                }
                Step::Tick(n) => {
                    for _ in 0..n {
                        cluster.tick_all();
                        prop_assert_eq!(check_all(&cluster.nodes), Ok(()));
                    }
                }
                Step::ForceElection(idx) => {
                    let node = &mut cluster.nodes[idx as usize % 3];
                    node.set_election_timeout_for_tests(1);
                    cluster.tick_all();
                }
                Step::Isolate(idx) => {
                    let id = cluster.nodes[idx as usize % 3].id();
                    cluster.isolate(id);
                }
                Step::Heal(idx) => {
                    let id = cluster.nodes[idx as usize % 3].id();
                    cluster.heal(id);
                }
            }
            prop_assert_eq!(check_all(&cluster.nodes), Ok(()));
        }
    }
}
