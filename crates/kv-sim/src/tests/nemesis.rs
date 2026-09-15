use crate::cluster::{Cluster, SimConfig};
use crate::nemesis::{Nemesis, NemesisConfig};

#[test]
fn the_nemesis_actually_injects_faults() {
    // A nemesis that never fires would make every sweep below it vacuous, so
    // this asserts that faults genuinely happen over a normal-length run.
    let mut cluster = Cluster::new(SimConfig::new(3, 3));
    let mut nemesis = Nemesis::new(3, NemesisConfig::default());
    let mut ever_down = false;

    for _ in 0..600 {
        nemesis.act(&mut cluster);
        if !cluster.down_nodes().is_empty() {
            ever_down = true;
        }
        cluster.step().expect("invariants held");
    }

    assert!(ever_down, "the nemesis never crashed a node in 600 steps");
}

#[test]
fn the_nemesis_never_kills_the_last_node() {
    let mut cluster = Cluster::new(SimConfig::new(9, 3));
    let mut nemesis = Nemesis::new(9, NemesisConfig { fault_percent: 90, recover_percent: 0 });

    for _ in 0..500 {
        nemesis.act(&mut cluster);
        cluster.step().expect("invariants held");
        assert!(!cluster.live_nodes().is_empty(), "the nemesis emptied the cluster");
    }
}

#[test]
fn a_nemesis_run_is_reproducible() {
    let run = |seed| {
        let mut cluster = Cluster::new(SimConfig::new(seed, 3));
        let mut nemesis = Nemesis::new(seed, NemesisConfig::default());
        for round in 0..300u64 {
            nemesis.act(&mut cluster);
            cluster.propose(vec![round as u8]);
            cluster.step().expect("invariants held");
        }
        cluster.trace().to_vec()
    };
    assert_eq!(run(21), run(21), "a nemesis run must be a function of its seed");
    assert_ne!(run(21), run(22));
}
