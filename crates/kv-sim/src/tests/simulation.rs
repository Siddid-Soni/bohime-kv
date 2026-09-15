//! The M4 gates. Every claim this milestone makes rests on the first test
//! here: if a seed does not reproduce, nothing else below is falsifiable.

use crate::cluster::{Cluster, SimConfig, TraceEvent};
use crate::network::NetworkConfig;

fn hostile(seed: u64) -> SimConfig {
    SimConfig::new(seed, 3).with_network(NetworkConfig {
        drop_percent: 20,
        duplicate_percent: 5,
        max_delay_ticks: 6,
    })
}

/// Runs one seed under load and faults, returning its full trace.
fn run_trace(seed: u64, ticks: u64) -> Vec<TraceEvent> {
    let mut cluster = Cluster::new(hostile(seed));
    for round in 0..ticks {
        // A deterministic fault schedule: the point of this test is that the
        // *network* is random-but-seeded, not that the schedule is.
        match round % 37 {
            11 => cluster.partition(&[&[1, 2], &[3]]),
            19 => cluster.heal(),
            29 => cluster.crash(2),
            33 => cluster.restart(2),
            _ => {}
        }
        cluster.propose(vec![(round % 251) as u8]);
        cluster.step().expect("invariants held");
    }
    cluster.trace().to_vec()
}

#[test]
fn a_seed_reproduces_byte_identically() {
    let first = run_trace(41, 400);
    let second = run_trace(41, 400);

    assert_eq!(first.len(), second.len(), "two runs of one seed produced different trace lengths");
    // Compare element-wise so a divergence names the step it happened at.
    for (i, (a, b)) in first.iter().zip(second.iter()).enumerate() {
        assert_eq!(a, b, "runs of seed 41 diverged at trace event {i}");
    }
    assert!(first.len() > 100, "a trace this short is not evidence of much: {}", first.len());
}

#[test]
fn different_seeds_produce_different_runs() {
    // Guards against the failure where everything "reproduces" because the
    // seed is not actually threaded through anything.
    assert_ne!(run_trace(41, 400), run_trace(42, 400));
}

#[test]
fn a_quiet_three_node_cluster_elects_exactly_one_leader() {
    for seed in 0..200 {
        let mut cluster = Cluster::new(SimConfig::new(seed, 3));
        let leader = cluster.run_until_leader(300).expect("invariants held");
        assert!(leader.is_some(), "seed {seed} elected no leader within 300 ticks");
    }
}

#[test]
fn a_lossy_three_node_cluster_still_elects_a_leader() {
    for seed in 0..200 {
        let mut cluster = Cluster::new(hostile(seed));
        let leader = cluster.run_until_leader(2000).expect("invariants held");
        assert!(leader.is_some(), "seed {seed} elected no leader under loss within 2000 ticks");
    }
}

#[test]
fn invariants_hold_under_loss_and_partitions() {
    for seed in 0..300 {
        let mut cluster = Cluster::new(hostile(seed));
        for round in 0..300u64 {
            match round % 23 {
                7 => cluster.partition(&[&[1, 2], &[3]]),
                13 => cluster.partition(&[&[1], &[2, 3]]),
                17 => cluster.heal(),
                _ => {}
            }
            cluster.propose(vec![round as u8]);
            if let Err(v) = cluster.step() {
                panic!("seed {seed} violated an invariant at tick {}: {v}", cluster.now());
            }
        }
    }
}

#[test]
fn a_crashed_node_comes_back_with_only_what_it_persisted() {
    let mut cluster = Cluster::new(SimConfig::new(5, 3));
    let leader = cluster.run_until_leader(300).expect("invariants held").expect("a leader");

    for i in 0..20u8 {
        cluster.propose(vec![i]);
        cluster.step().expect("invariants held");
    }
    cluster.run(50).expect("invariants held");
    let before = cluster.committed().len();
    assert!(before > 0, "nothing committed, so the test would prove nothing");

    let victim = if leader == 1 { 2 } else { 1 };
    cluster.crash(victim);
    assert!(cluster.down_nodes().contains(&victim));
    cluster.restart(victim);

    cluster.run(200).expect("invariants held after a restart");
    assert!(cluster.committed().len() >= before, "a restart lost committed entries");
}

#[test]
fn killing_the_leader_mid_replication_loses_no_committed_entry() {
    for seed in 0..100 {
        let mut cluster = Cluster::new(hostile(seed));
        let Some(leader) = cluster.run_until_leader(2000).expect("invariants held") else {
            continue;
        };

        for i in 0..10u8 {
            cluster.propose(vec![i]);
            cluster.step().expect("invariants held");
        }
        let committed_before = cluster.committed();

        // Kill it mid-flight: messages it already sent are still in the
        // network, which is the case that loses writes if commit is wrong.
        cluster.crash(leader);
        cluster.run(1500).expect("invariants held after the leader died");
        cluster.restart(leader);
        cluster.run(500).expect("invariants held after it came back");

        let after = cluster.committed();
        for entry in &committed_before {
            let found = after.iter().find(|e| e.index == entry.index);
            assert_eq!(
                found,
                Some(entry),
                "seed {seed}: entry {:?} was committed, then lost when leader {leader} died",
                entry
            );
        }
    }
}
