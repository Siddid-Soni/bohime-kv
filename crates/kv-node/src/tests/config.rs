use std::time::Duration;

use crate::config::NodeConfig;

fn cfg(id: u64) -> NodeConfig {
    NodeConfig {
        id,
        listen: "127.0.0.1:7001".parse().unwrap(),
        peers: [(2, "http://127.0.0.1:7002".to_string())].into_iter().collect(),
        data_dir: std::path::PathBuf::from("/tmp/n"),
        tick: Duration::from_millis(20),
        election_timeout: 15,
        heartbeat_interval: 3,
        lease_reads: false,
        keydir: Default::default(),
        snapshot_threshold: 10_000,
        initial_learner: false,
        num_shards: 256,
        replication_factor: 3,
        vnodes_per_node: kv_ring::DEFAULT_VNODES,
        max_migrations: 4,
        log_fsync: crate::config::LogFsync::default().into(),
        state_fsync: crate::config::LogFsync::default().into(),
    }
}

#[test]
fn the_raft_log_and_the_state_machine_get_separate_directories() {
    let c = cfg(1);
    assert_ne!(c.shard_raft_dir(0), c.shard_state_dir(0), "one Bitcask directory cannot hold both");
    assert!(c.shard_raft_dir(0).starts_with(&c.data_dir));
    assert!(c.shard_state_dir(0).starts_with(&c.data_dir));
}

/// Identical seeds make every node draw the identical election timeout, so
/// they all campaign on the same tick, split the vote, and do it again. The
/// cluster then never elects anyone, and it looks like a network fault.
#[test]
fn nodes_get_different_election_seeds() {
    assert_ne!(cfg(1).raft_config().seed, cfg(2).raft_config().seed);
    assert_ne!(cfg(2).raft_config().seed, cfg(3).raft_config().seed);
}

#[test]
fn the_raft_config_carries_the_peers_but_not_ourselves() {
    let raft = cfg(1).raft_config();
    assert_eq!(raft.id, 1);
    assert_eq!(raft.peers, vec![2]);
    assert_eq!(raft.cluster_size(), 2);
}

/// Well below the election timeout, or a healthy leader is deposed by its own
/// followers between heartbeats.
#[test]
fn the_heartbeat_is_well_inside_the_election_timeout() {
    let c = cfg(1);
    assert!(c.heartbeat_interval * 3 <= c.election_timeout);
}

fn args(id: u64, peers: Vec<(u64, String)>) -> crate::config::Args {
    crate::config::Args {
        id,
        listen: "127.0.0.1:7001".parse().unwrap(),
        peers,
        data_dir: std::path::PathBuf::from("/tmp/n"),
        tick_ms: 20,
        election_timeout: 15,
        heartbeat_interval: 3,
        lease_reads: false,
        keydir: Default::default(),
        snapshot_threshold: 10_000,
        join: false,
        num_shards: 256,
        replication_factor: 1,
        vnodes_per_node: kv_ring::DEFAULT_VNODES,
        max_migrations: 4,
        log_fsync: Default::default(),
        state_fsync: Default::default(),
    }
}

/// Every process in a cluster should be launchable with the same `--peer`
/// flags, differing only in `--id` and `--listen`. So a node finding itself in
/// its own list filters itself out rather than refusing to start.
#[test]
fn a_node_filters_itself_out_of_a_uniform_cluster_list() {
    let uniform = vec![
        (1, "http://127.0.0.1:7001".to_string()),
        (2, "http://127.0.0.1:7002".to_string()),
        (3, "http://127.0.0.1:7003".to_string()),
    ];
    let config = args(2, uniform).into_config().unwrap();
    assert_eq!(config.peers.keys().copied().collect::<Vec<_>>(), vec![1, 3]);
    assert_eq!(config.raft_config().cluster_size(), 3);
}

/// A duplicate is still a real mistake: it would leave the node believing the
/// cluster is smaller than it is, and a quorum computed from a short list is
/// not a quorum.
#[test]
fn a_repeated_peer_is_an_error() {
    let repeated =
        vec![(2, "http://127.0.0.1:7002".to_string()), (2, "http://127.0.0.1:9999".to_string())];
    assert!(args(1, repeated).into_config().is_err());
}

/// `--max-migrations` is an operational knob with a conservative default:
/// moving many shards at once is how a rebalance becomes an outage, and the
/// cost lands on one receiver's snapshot ingest and its two Bitcask opens.
///
/// `--replication-factor 1` because `into_config` refuses a factor larger
/// than the cluster in `--peer`, and this is a one-node command line.
#[test]
fn max_migrations_defaults_to_four_and_is_configurable() {
    use clap::Parser;

    let base = ["kv-node", "--id", "1", "--data-dir", "/tmp/x", "--replication-factor", "1"];

    let config = crate::config::Args::try_parse_from(base).unwrap().into_config().unwrap();
    assert_eq!(config.max_migrations, 4);

    let mut with_flag = base.to_vec();
    with_flag.extend(["--max-migrations", "16"]);
    let config = crate::config::Args::try_parse_from(with_flag).unwrap().into_config().unwrap();
    assert_eq!(config.max_migrations, 16);
}
