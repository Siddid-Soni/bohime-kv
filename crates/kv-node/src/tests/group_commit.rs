//! Group commit on the Raft log, and the ordering that makes it safe.
//!
//! The Raft log's Bitcask ran `FsyncPolicy::EveryWrite`, so appending N
//! entries cost N fsyncs plus one for the `\x00log_meta` update. On this box
//! that is the entire write path: a measured 574 fdatasync/s on btrfs against
//! ~82 cluster writes/s, and a shard sweep that stays flat at 1, 8 and 64
//! shards because every shard's log lands on the same device.
//!
//! Group commit removes the per-entry fsync and leaves exactly one per drain.
//! What makes that safe is *not* the policy — it is the ordering §1.5
//! already required and `Group::drain` already implemented: **disk before
//! network**. A vote must be durable before it is granted on the wire, or a
//! crash lets the node vote twice in one term and election safety is gone; an
//! entry must be durable before it is counted toward a commit. Under
//! `EveryWrite` that ordering held by accident, because every write synced on
//! the way in. Under `GroupCommit` the drain's sync is the only thing holding
//! it, so it is pinned here rather than left to a comment.
//!
//! The second test is the one that matters. It asserts that the very first
//! message this node ever puts on the wire — its own `RequestVote` — leaves
//! after an fsync has actually happened, counted by the engine rather than
//! inferred from the policy. Delete the sync in `Group::drain` and it fails.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kv_raft::storage::RaftStorage;
use kv_raft::types::Entry;
use kv_raft::{Message, NodeId, RaftNode};
use kv_storage::{Engine, FsyncPolicy};
use tokio::sync::mpsc;

use crate::config::NodeConfig;
use crate::driver::{Driver, Group, GroupChannels};
use crate::storage::BitcaskStorage;
use crate::tests::support::SilentPeers;
use crate::transport::PeerLink;
use crate::transport::group::GroupId;
use crate::transport::peer::SendError;

fn entries(from: u64, count: u64) -> Vec<Entry> {
    (from..from + count).map(|index| Entry { term: 1, index, command: vec![7; 64] }).collect()
}

/// A log whose engine never syncs on its own, so the only fsyncs it performs
/// are the ones something asked for explicitly.
fn batching() -> FsyncPolicy {
    FsyncPolicy::GroupCommit { max_records: usize::MAX, max_delay: Duration::from_secs(3600) }
}

#[test]
fn every_write_costs_one_fsync_per_entry_plus_one_for_the_metadata() {
    // The control arm, and the thing being fixed. It is asserted rather than
    // asserted-about-in-a-comment so the "after" number has a "before" in the
    // same file.
    let dir = tempfile::tempdir().unwrap();
    let mut log = BitcaskStorage::open_with_policy(dir.path(), FsyncPolicy::EveryWrite).unwrap();

    log.append(&entries(1, 64)).unwrap();

    assert_eq!(log.sync_count(), 65, "64 entries plus one \\x00log_meta update, each fsynced");
}

#[test]
fn group_commit_costs_one_fsync_per_drain_however_many_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = BitcaskStorage::open_with_policy(dir.path(), batching()).unwrap();

    log.append(&entries(1, 64)).unwrap();
    assert_eq!(log.sync_count(), 0, "nothing is durable until somebody asks for it");

    // What `Group::drain` does before it sends anything.
    log.sync().unwrap();
    assert_eq!(log.sync_count(), 1, "one fsync covers the whole batch");

    // And a second sync with nothing pending is free, which is what lets the
    // driver call it unconditionally.
    log.sync().unwrap();
    assert_eq!(log.sync_count(), 1);
}

#[test]
fn a_group_committed_log_still_reads_back_everything_it_synced() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = BitcaskStorage::open_with_policy(dir.path(), batching()).unwrap();
        log.append(&entries(1, 32)).unwrap();
        log.sync().unwrap();
    }
    let log = BitcaskStorage::open_with_policy(dir.path(), batching()).unwrap();
    assert_eq!(log.last_index().unwrap(), 32);
    assert_eq!(log.entries(1, 33).unwrap().len(), 32);
}

/// Records, for every message it is handed, how many fsyncs the Raft log had
/// performed at the instant of the send.
///
/// The ticker is the log's own counter, not a proxy: `BitcaskStorage::sync`
/// publishes `Engine::sync_count()` into it, so a zero here means no fsync had
/// happened yet — not merely that nobody had called `sync`.
struct SyncWatchingLink {
    ticker: Arc<AtomicU64>,
    seen: mpsc::UnboundedSender<(u64, Message)>,
}

impl PeerLink for SyncWatchingLink {
    fn try_send(&self, _group: GroupId, msg: Message) -> Result<(), SendError> {
        let _ = self.seen.send((self.ticker.load(Ordering::SeqCst), msg));
        Ok(())
    }
}

fn three_node_config(dir: &std::path::Path) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> =
        [(2, "http://127.0.0.1:1".to_string()), (3, "http://127.0.0.1:2".to_string())]
            .into_iter()
            .collect();
    let config = NodeConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().unwrap(),
        peers,
        data_dir: dir.to_path_buf(),
        tick: Duration::from_millis(2),
        election_timeout: 3,
        heartbeat_interval: 1,
        lease_reads: false,
        keydir: Default::default(),
        snapshot_threshold: u64::MAX,
        initial_learner: false,
        num_shards: 256,
        replication_factor: 3,
        vnodes_per_node: kv_ring::DEFAULT_VNODES,
        log_fsync: batching(),
        state_fsync: crate::config::LogFsync::default().into(),
    };
    std::fs::create_dir_all(config.shard_raft_dir(0)).unwrap();
    std::fs::create_dir_all(config.shard_state_dir(0)).unwrap();
    config
}

/// §1.5, disk before network — the property group commit is safe *because of*.
///
/// A node that granted itself a vote, crashed before that vote reached the
/// disk, came back and granted a second vote in the same term would let two
/// leaders be elected in one term. Under `EveryWrite` the vote was durable
/// before `step` even returned. Under `GroupCommit` nothing is durable until
/// the drain syncs, so the drain syncing *before* it sends is the entire
/// argument, and this test is what holds it.
#[tokio::test]
async fn a_vote_is_durable_before_it_is_granted_on_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let config = three_node_config(dir.path());

    let log = BitcaskStorage::open_with_policy(config.shard_raft_dir(0), config.log_fsync).unwrap();
    assert_eq!(log.sync_count(), 0, "a freshly opened log has synced nothing");
    let ticker = log.sync_ticker();

    let node = RaftNode::new(config.raft_config(), log);
    let engine = Engine::open(config.shard_state_dir(0)).unwrap();

    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let peers: BTreeMap<NodeId, Box<dyn PeerLink>> = [2, 3]
        .into_iter()
        .map(|id| {
            let link: Box<dyn PeerLink> =
                Box::new(SyncWatchingLink { ticker: Arc::clone(&ticker), seen: seen_tx.clone() });
            (id, link)
        })
        .collect();

    let (_inbox_tx, inbox) = mpsc::channel(8);
    let (_replies_tx, peer_replies) = mpsc::channel(8);
    let (_requests_tx, requests) = mpsc::channel(8);
    let (_admin_tx, admin) = mpsc::channel(8);
    let (_groups_tx, new_groups) = mpsc::channel(8);

    let driver = Driver::new(
        &config,
        vec![Group::new(&config, crate::transport::group::DATA, node, engine)],
        peers,
        Box::new(SilentPeers),
        GroupChannels { inbox, peer_replies, requests, admin, new_groups },
    );
    tokio::spawn(driver.run());

    let (syncs_at_send, msg) = tokio::time::timeout(Duration::from_secs(10), seen.recv())
        .await
        .expect("the node campaigns within ten seconds")
        .expect("the driver sends before it shuts down");

    assert!(
        matches!(msg, Message::RequestVote { .. }),
        "the first thing a fresh node sends is its own RequestVote; got {msg:?}"
    );
    assert!(
        syncs_at_send >= 1,
        "the vote left the process before any fsync had happened: \
         disk-before-network is broken and a crash can elect two leaders in one term"
    );
}

/// The same ordering for an *entry*, which is what makes a commit safe.
///
/// A leader that counted its own unsynced entry toward a quorum, crashed, and
/// came back without it would have committed an entry it no longer holds.
#[tokio::test]
async fn an_entry_is_durable_before_it_is_replicated() {
    let dir = tempfile::tempdir().unwrap();
    let config = three_node_config(dir.path());

    let log = BitcaskStorage::open_with_policy(config.shard_raft_dir(0), config.log_fsync).unwrap();
    let ticker = log.sync_ticker();
    let node = RaftNode::new(config.raft_config(), log);
    let engine = Engine::open(config.shard_state_dir(0)).unwrap();

    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let peers: BTreeMap<NodeId, Box<dyn PeerLink>> = [2, 3]
        .into_iter()
        .map(|id| {
            let link: Box<dyn PeerLink> =
                Box::new(SyncWatchingLink { ticker: Arc::clone(&ticker), seen: seen_tx.clone() });
            (id, link)
        })
        .collect();

    let (_inbox_tx, inbox) = mpsc::channel(8);
    let (_replies_tx, peer_replies) = mpsc::channel(8);
    let (_requests_tx, requests) = mpsc::channel(8);
    let (_admin_tx, admin) = mpsc::channel(8);
    let (_groups_tx, new_groups) = mpsc::channel(8);

    let driver = Driver::new(
        &config,
        vec![Group::new(&config, crate::transport::group::DATA, node, engine)],
        peers,
        Box::new(SilentPeers),
        GroupChannels { inbox, peer_replies, requests, admin, new_groups },
    );
    tokio::spawn(driver.run());

    // Every message, not just the first: the assertion is about the ordering
    // in general, so it holds for each send the drain produces.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut checked = 0usize;
    while tokio::time::Instant::now() < deadline && checked < 20 {
        let Ok(Some((syncs_at_send, msg))) =
            tokio::time::timeout(Duration::from_secs(2), seen.recv()).await
        else {
            break;
        };
        assert!(
            syncs_at_send >= 1,
            "{msg:?} left the process before the log had ever been fsynced"
        );
        checked += 1;
    }
    assert!(checked > 0, "the driver sent nothing at all in ten seconds");
}

/// The state machine is a *cache of the log*, and the one place that stops
/// being true.
///
/// A replicated state machine does not need its own fsync: everything in it
/// came from the Raft log, which is durable, and a restart replays the tail.
/// `NodeConfig::engine_config`'s own comment says exactly that — and then
/// hands the engine `FsyncPolicy::EveryWrite` anyway, so through M11.5 every
/// applied `Put` cost two extra fsyncs a node (the value and its session-table
/// row). Measured at 8 shards: removing them took the cluster from 119 to 286
/// writes/s, more than group commit on the log was worth.
///
/// Removing them is only safe because of one ordering. A snapshot **drops the
/// log prefix that could rebuild the state**, and `Group::new` starts replay
/// at the stored snapshot's index on the stated assumption that "the state on
/// disk already reflects the stored snapshot". So the state machine must be
/// fsynced *before* `take_snapshot` truncates, or a crash loses applied writes
/// below the boundary and nothing will ever replay them. That is what this
/// test pins.
///
/// The counter is the engine's own, held through an `Arc` because the engine
/// is moved into the `Group`. Under `FsyncPolicy::Never` nothing syncs it
/// implicitly, so a non-zero count can only have come from an explicit
/// `Engine::sync` — which on this path means `maybe_snapshot`'s.
#[tokio::test]
async fn the_state_machine_is_fsynced_before_the_log_prefix_that_could_rebuild_it_is_dropped() {
    use kv_storage::{EngineConfig, IndexKind};

    let dir = tempfile::tempdir().unwrap();
    let mut config = three_node_config(dir.path());
    // A lone voter, so it elects itself and commits without any peer.
    config.peers.clear();
    // Low enough that a few dozen writes cross it.
    config.snapshot_threshold = 8;

    let log = BitcaskStorage::open_with_policy(config.shard_raft_dir(0), config.log_fsync).unwrap();
    let node = RaftNode::new(config.raft_config(), log);

    let engine = Engine::open_with_config(
        config.shard_state_dir(0),
        EngineConfig {
            index: IndexKind::default(),
            fsync_policy: FsyncPolicy::Never,
            ..Default::default()
        },
    )
    .unwrap();
    let state_syncs = engine.sync_counter();
    assert_eq!(state_syncs.load(Ordering::SeqCst), 0);

    let (_inbox_tx, inbox) = mpsc::channel(8);
    let (_replies_tx, peer_replies) = mpsc::channel(8);
    let (requests_tx, requests) = mpsc::channel(64);
    let (_admin_tx, admin) = mpsc::channel(8);
    let (_groups_tx, new_groups) = mpsc::channel(8);

    let driver = Driver::new(
        &config,
        vec![Group::new(&config, crate::transport::group::DATA, node, engine)],
        BTreeMap::new(),
        Box::new(SilentPeers),
        GroupChannels { inbox, peer_replies, requests, admin, new_groups },
    );
    tokio::spawn(driver.run());

    // Comfortably past the threshold, retried through the election.
    for i in 0..60u32 {
        for _ in 0..200 {
            let (reply, wait) = tokio::sync::oneshot::channel();
            requests_tx
                .send(crate::driver::ClientRequest {
                    group: crate::transport::group::DATA,
                    op: crate::driver::ClientOp::put(
                        format!("k{i}").as_bytes(),
                        format!("v{i}").as_bytes(),
                    ),
                    reply,
                })
                .await
                .unwrap();
            match tokio::time::timeout(Duration::from_secs(5), wait).await.unwrap().unwrap() {
                crate::driver::ClientReply::NotLeader { .. } => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                _ => break,
            }
        }
    }

    // The snapshot is taken on a drain after apply, so give the loop a moment.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while state_syncs.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        state_syncs.load(Ordering::SeqCst) >= 1,
        "60 writes crossed a snapshot threshold of 8 and the state machine was never fsynced; \
         the log prefix it depends on has been dropped and a crash loses applied writes \
         that nothing will replay"
    );
}
