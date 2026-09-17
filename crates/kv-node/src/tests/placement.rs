//! The on-disk shard layout and the founding marker (M11.1).

use std::collections::BTreeSet;

use crate::placement;
use crate::tests::support::config_in;

/// Four Bitcask instances became `2 * hosted_shards + 2`, and every one of
/// them must have a directory of its own: one keydir cannot hold two key
/// spaces, and shard 7's log uses the very same `e{index:020}` entry keys
/// shard 8's does.
#[test]
fn every_shard_gets_its_own_log_and_state_directory() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());

    let mut seen = BTreeSet::new();
    for shard in [0u16, 1, 7, 255, 65535] {
        assert!(seen.insert(config.shard_raft_dir(shard)), "shard {shard}'s log dir repeats");
        assert!(seen.insert(config.shard_state_dir(shard)), "shard {shard}'s state dir repeats");
    }
    for path in &seen {
        assert!(path.starts_with(&config.data_dir), "{path:?} escapes the data dir");
    }
    // Also apart from the meta group's two, which keep their M10 paths.
    assert!(!seen.contains(&config.meta_raft_dir()));
    assert!(!seen.contains(&config.meta_state_dir()));
}

/// A store written before M11 holds one log covering every shard's keys, and
/// splitting it needs a data move — M12's migration driver, not a startup
/// path. Starting anyway would present an empty database over a directory
/// holding the whole previous dataset.
#[test]
fn a_pre_m11_store_is_refused_rather_than_served_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());
    assert!(placement::check_layout(&config).is_ok(), "a fresh store is fine");

    std::fs::create_dir_all(config.legacy_raft_dir()).unwrap();
    let refusal = placement::check_layout(&config).expect_err("the old layout must be refused");
    let text = refusal.to_string();
    assert!(text.contains("raft"), "the refusal must name the directory: {text}");
}

/// Founding is a one-time act, and the marker is what makes it one. Without a
/// durable record, a node that restarted after the map moved a replica slot
/// would found a second configuration for a shard already running elsewhere.
#[test]
fn the_founding_marker_records_the_map_version_and_survives() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());

    assert_eq!(placement::founded_at(&config).unwrap(), None, "a fresh store has founded nothing");
    placement::record_founding(&config, 7).unwrap();
    assert_eq!(placement::founded_at(&config).unwrap(), Some(7));
    // Re-read through a fresh config over the same directory: this is what a
    // restart sees.
    assert_eq!(placement::founded_at(&config_in(dir.path())).unwrap(), Some(7));
}

/// After a restart the shards on disk are the truth, not the map: the map may
/// have moved on, and a group we already run is one we are still a member of
/// as far as Raft is concerned.
#[test]
fn the_shards_on_disk_are_what_a_restart_hosts() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());
    assert!(placement::hosted_on_disk(&config).unwrap().is_empty());

    for shard in [3u16, 0, 41] {
        std::fs::create_dir_all(config.shard_raft_dir(shard)).unwrap();
        std::fs::create_dir_all(config.shard_state_dir(shard)).unwrap();
    }
    assert_eq!(placement::hosted_on_disk(&config).unwrap(), BTreeSet::from([0, 3, 41]));
}

/// A directory that is not a shard number must not be read as one — a stray
/// file under `shards/` would otherwise become a Raft group with a garbage id.
#[test]
fn a_directory_that_is_not_a_shard_number_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_in(dir.path());
    std::fs::create_dir_all(config.shards_dir().join("lost+found")).unwrap();
    std::fs::create_dir_all(config.shard_raft_dir(9)).unwrap();

    assert_eq!(placement::hosted_on_disk(&config).unwrap(), BTreeSet::from([9]));
}

/// Per-shard Bitcask means two engines per hosted shard, each holding a
/// descriptor per segment — so a node replicating 154 of 256 shards needs
/// over a thousand descriptors before it serves one client, and the usual
/// soft default is 1024. The node raises its own limit rather than relying on
/// a launch script, so that starting it by hand works.
///
/// Safe to mutate the limit here: nextest runs each test in its own process.
#[test]
fn the_node_raises_its_own_open_file_limit() {
    // SAFETY: `getrlimit`/`setrlimit` over a struct we own; neither retains
    // the pointer.
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) }, 0);
    // Start from something small, so the raise is observable even on a box
    // whose soft limit already equals its hard one.
    let lowered = libc::rlimit { rlim_cur: 64.min(limit.rlim_max), rlim_max: limit.rlim_max };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) }, 0);

    let raised = crate::raise_file_limit();

    assert_eq!(raised, limit.rlim_max, "the soft limit was not raised to the hard one");
    assert!(raised >= lowered.rlim_cur);
}
