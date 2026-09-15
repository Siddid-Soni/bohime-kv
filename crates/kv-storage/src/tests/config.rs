use crate::config::{EngineConfig, FsyncPolicy};
use std::time::Duration;

#[test]
fn never_policy_never_syncs() {
    let policy = FsyncPolicy::Never;
    assert!(!policy.should_sync(0, Duration::ZERO));
    assert!(!policy.should_sync(1_000_000, Duration::from_secs(3600)));
}

#[test]
fn every_write_policy_syncs_whenever_anything_is_unsynced() {
    let policy = FsyncPolicy::EveryWrite;
    assert!(!policy.should_sync(0, Duration::ZERO), "nothing pending means nothing to sync");
    assert!(policy.should_sync(1, Duration::ZERO));
}

#[test]
fn group_commit_syncs_on_record_count() {
    let policy = FsyncPolicy::GroupCommit { max_records: 4, max_delay: Duration::from_secs(60) };
    assert!(!policy.should_sync(3, Duration::ZERO));
    assert!(policy.should_sync(4, Duration::ZERO));
}

#[test]
fn group_commit_syncs_on_elapsed_time() {
    let policy =
        FsyncPolicy::GroupCommit { max_records: 1000, max_delay: Duration::from_millis(10) };
    assert!(!policy.should_sync(1, Duration::from_millis(9)));
    assert!(policy.should_sync(1, Duration::from_millis(10)));
}

#[test]
fn group_commit_with_nothing_pending_never_syncs_on_time_alone() {
    let policy = FsyncPolicy::GroupCommit { max_records: 4, max_delay: Duration::from_millis(1) };
    assert!(!policy.should_sync(0, Duration::from_secs(60)));
}

#[test]
fn default_config_is_durable_not_fast() {
    assert_eq!(EngineConfig::default().fsync_policy, FsyncPolicy::EveryWrite);
    assert_eq!(EngineConfig::default().max_segment_size, 64 * 1024 * 1024);
}
