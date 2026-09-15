//! The seed sweeps (M4, task 6).
//!
//! The full gates are 10k and 100k seeds, which take minutes — too slow for
//! `cargo nextest run` to stay usable, so they are `#[ignore]`d and CI opts in
//! with `--run-ignored all`. The fast subsets below run by default and cover
//! the same code paths, so a regression is caught in seconds and only the
//! long tail needs the full sweep.

use crate::cluster::{Cluster, SimConfig};
use crate::nemesis::{Nemesis, NemesisConfig};
use crate::network::NetworkConfig;

fn hostile(seed: u64) -> SimConfig {
    SimConfig::new(seed, 3).with_network(NetworkConfig {
        drop_percent: 20,
        duplicate_percent: 5,
        max_delay_ticks: 6,
    })
}

/// Every seed in `seeds` must elect exactly one leader on a clean network.
fn election_sweep(seeds: impl Iterator<Item = u64>) {
    for seed in seeds {
        let mut cluster = Cluster::new(SimConfig::new(seed, 3));
        let leader = cluster.run_until_leader(400).expect("invariants held");
        assert!(leader.is_some(), "seed {seed}: no leader within 400 ticks");
    }
}

/// Every seed in `seeds` must hold every invariant under loss and a nemesis.
fn nemesis_sweep(seeds: impl Iterator<Item = u64>, ticks: u64) {
    for seed in seeds {
        let mut cluster = Cluster::new(hostile(seed));
        let mut nemesis = Nemesis::new(seed, NemesisConfig::default());
        for round in 0..ticks {
            nemesis.act(&mut cluster);
            cluster.propose(vec![(round % 251) as u8]);
            if let Err(v) = cluster.step() {
                panic!("seed {seed} violated an invariant at tick {}: {v}", cluster.now());
            }
        }
    }
}

#[test]
fn election_sweep_fast() {
    election_sweep(0..500);
}

#[test]
fn nemesis_sweep_fast() {
    nemesis_sweep(0..150, 250);
}

#[test]
#[ignore = "full gate: 10k seeds, minutes"]
fn election_sweep_full() {
    election_sweep(0..10_000);
}

#[test]
#[ignore = "full gate: 100k seeds, many minutes"]
fn nemesis_sweep_full() {
    nemesis_sweep(0..100_000, 250);
}
