//! Test-only support shared across the node's test files.

use kv_raft::{Message, NodeId};

use crate::transport::group::GroupId;
use crate::transport::peer::SendError;
use crate::transport::{PeerFactory, PeerLink};

/// A link that accepts everything and delivers nothing — a peer that is
/// configured but unreachable, which for a driver is indistinguishable from
/// one that is simply down.
pub(crate) struct DeadLink;

impl PeerLink for DeadLink {
    fn try_send(&self, _group: GroupId, _msg: Message) -> Result<(), SendError> {
        Ok(())
    }
}

/// Hands out `DeadLink`s. For tests about one driver in isolation, where the
/// peers exist in the config only so the node does not elect itself.
pub(crate) struct SilentPeers;

impl PeerFactory for SilentPeers {
    fn connect(&self, _id: NodeId, _address: &str) -> Box<dyn PeerLink> {
        Box::new(DeadLink)
    }
}

/// A `NodeConfig` rooted at `dir`, with its Bitcask directories created.
///
/// Single-node and timing-neutral: for tests about configuration itself
/// rather than about a running cluster.
pub(crate) fn config_in(dir: &std::path::Path) -> crate::config::NodeConfig {
    let config = crate::config::NodeConfig {
        id: 1,
        listen: "127.0.0.1:0".parse().unwrap(),
        peers: std::collections::BTreeMap::new(),
        data_dir: dir.to_path_buf(),
        tick: std::time::Duration::from_millis(10),
        election_timeout: 10,
        heartbeat_interval: 2,
        lease_reads: false,
        keydir: kv_storage::IndexKind::default(),
        snapshot_threshold: u64::MAX,
        initial_learner: false,
        num_shards: 256,
        replication_factor: 1,
        vnodes_per_node: kv_ring::DEFAULT_VNODES,
        max_migrations: 4,
        log_fsync: crate::config::LogFsync::default().into(),
        state_fsync: crate::config::LogFsync::default().into(),
    };
    // The meta group's two only. A shard's directories are created when the
    // shard is founded, and the legacy pair must stay absent — their presence
    // is what `placement::check_layout` refuses.
    for dir in [config.meta_raft_dir(), config.meta_state_dir()] {
        std::fs::create_dir_all(dir).unwrap();
    }
    config
}

/// A published map with exactly one shard, owned by `me`.
///
/// For tests that drive `KvApi` against a single-group driver: with one shard
/// every key routes to shard 0, whose group is `group::DATA`, so the router
/// resolves every request to the one group the driver hosts. The map's
/// `num_shards` is the map's own business — the router reads it from there,
/// never from the flags.
pub(crate) fn published_one_shard(me: kv_raft::NodeId) -> crate::meta::PublishedMap {
    let map = kv_ring::ShardMap::build(1, [me], 1, 1, kv_ring::DEFAULT_VNODES)
        .expect("one node can hold one replica of one shard");
    let published = crate::meta::PublishedMap::default();
    published.store(Some(std::sync::Arc::new(map)));
    published
}
