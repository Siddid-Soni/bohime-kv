//! `kv-node`: the impure shell binding kv-storage + kv-raft to real
//! tokio/tonic I/O (M6).
//!
//! One listener carries all three services. There is no reason to make an
//! operator manage two ports for one process, and the Raft peers and the
//! clients reach the same driver either way.
//!
//! Since M11 the process hosts **many** Raft groups: one per shard it
//! replicates, plus the meta group whose state machine is the shard map and
//! whose membership is the cluster's. They are ordinary `RaftNode`s —
//! `kv-raft` used as a library N times, with no instance aware it has
//! siblings. What keeps them apart is a group id on every wire message and a
//! Bitcask directory pair each.
//!
//! **Two drivers, not one, and not one per shard.** Every shard group shares
//! one `Driver` — one tick loop, one thread's worth of ownership, which is the
//! design M11 exists to get right. The meta group gets its own, because it is
//! the routing authority for the whole node and a shard's snapshot scan must
//! not stall the map.

mod admin_service;
mod command;
mod config;
mod driver;
mod kv_service;
mod membership;
mod meta;
mod migrate;
mod placement;
mod read_engine;
mod read_view;
mod router;
mod session;
mod shard_map;
mod shards;
mod snapshot;
mod storage;
mod transport;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;
use kv_proto::admin::admin_service_server::AdminServiceServer;
use kv_proto::kv::kv_service_server::KvServiceServer;
use kv_proto::raft::raft_service_server::RaftServiceServer;
use kv_raft::{NodeId, RaftNode};
use kv_storage::Engine;
use tokio::sync::mpsc;

use crate::admin_service::AdminApi;
use crate::config::{Args, NodeConfig};
use crate::driver::{AdminRequest, ClientRequest, Driver, Group, GroupChannels};
use crate::kv_service::KvApi;
use crate::meta::{MetaReconciler, PublishedMap, RECONCILE_INTERVAL};
use crate::storage::BitcaskStorage;
use crate::transport::group::{self, GroupId};
use crate::transport::peer::PeerConfig;
use crate::transport::server::{GroupRegistry, Inbound, RaftServer};
use crate::transport::{GrpcPeers, PeerFactory, PeerLink};

/// The channels one running driver is reached on.
struct DriverHandles {
    inbox: mpsc::Sender<Inbound>,
    requests: mpsc::Sender<ClientRequest>,
    admin: mpsc::Sender<AdminRequest>,
    /// Groups founded after the driver started. Unused by the meta driver,
    /// whose one group is hosted from its first tick.
    new_groups: mpsc::Sender<Group>,
}

/// Starts a driver, with no groups yet.
///
/// Groups arrive through `new_groups`: the meta group's immediately below,
/// each shard's from [`shards::ShardSupervisor`] once the map says which ones
/// this node holds.
fn start_driver(config: &NodeConfig, groups: Vec<Group>) -> DriverHandles {
    let (inbox_tx, inbox) = mpsc::channel(1024);
    let (replies_tx, peer_replies) = mpsc::channel(1024);
    let (requests_tx, requests) = mpsc::channel(256);
    let (admin_tx, admin) = mpsc::channel(16);
    let (groups_tx, new_groups) = mpsc::channel(64);

    // `connect` never blocks on a peer being up — a node must campaign whether
    // or not its peers exist yet, which is also what lets a cluster be started
    // in any order.
    //
    // The same factory stays with the driver: since M9 the peer set moves at
    // runtime, and a node admitted by a conf change has to be dialled without
    // a restart. The group travels on each message (M11.2), so these links
    // carry every group this driver hosts — one connection per peer, not one
    // per group per peer.
    let factory = GrpcPeers::new(PeerConfig::default(), replies_tx);
    let peers: BTreeMap<NodeId, Box<dyn PeerLink>> =
        config.peers.iter().map(|(&id, addr)| (id, factory.connect(id, addr))).collect();

    let driver = Driver::new(
        config,
        groups,
        peers,
        Box::new(factory),
        GroupChannels { inbox, peer_replies, requests, admin, new_groups },
    );
    tokio::spawn(async move {
        if let Err(e) = driver.run().await {
            tracing::error!(error = %e, "driver stopped");
        }
    });

    DriverHandles { inbox: inbox_tx, requests: requests_tx, admin: admin_tx, new_groups: groups_tx }
}

/// Opens one group's log and state machine.
fn open_group(
    config: &NodeConfig,
    group: GroupId,
    raft_dir: &Path,
    state_dir: &Path,
) -> anyhow::Result<Group> {
    std::fs::create_dir_all(raft_dir)?;
    std::fs::create_dir_all(state_dir)?;
    let node = RaftNode::new(
        config.raft_config_for(group),
        BitcaskStorage::open_with_policy(raft_dir, config.log_fsync)?,
    );
    let engine = Engine::open_with_config(state_dir, config.engine_config())?;
    Ok(Group::new(config, group, node, engine))
}

/// Raises the open-file limit to whatever the kernel will allow.
///
/// Per-shard Bitcask means two engines per hosted shard, each holding a
/// descriptor per segment, so a node replicating 154 shards needs over a
/// thousand descriptors before it serves one client — and the usual soft
/// default is 1024. Every database does this; doing it here rather than in a
/// launch script means the node works when someone starts it by hand.
///
/// Reported rather than enforced: the shard count this node will actually
/// host is not known until the map arrives, and refusing to start on a
/// heuristic would be worse than saying what the limit is.
fn raise_file_limit() -> u64 {
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: `getrlimit`/`setrlimit` with a resource we own and a struct we
    // own; neither retains the pointer.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        tracing::warn!("cannot read the open-file limit");
        return 0;
    }
    if limit.rlim_cur < limit.rlim_max {
        let raised = libc::rlimit { rlim_cur: limit.rlim_max, rlim_max: limit.rlim_max };
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
            limit = raised;
        }
    }
    tracing::info!(open_files = limit.rlim_cur, "open-file limit");
    limit.rlim_cur
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Args::parse().into_config()?;
    placement::check_layout(&config)?;
    let _ = raise_file_limit();

    if config.replication_factor == 2 {
        // Legal, and occasionally what someone wants — it survives a lost
        // disk, which RF=1 does not. But quorum is 2 of 2, so *either* node
        // being down stops the group: it is strictly less available than a
        // single node while costing twice the hardware. Worth saying out loud
        // rather than discovering during an incident.
        tracing::warn!(
            "--replication-factor 2 tolerates no failures and is less available than 1; \
             it buys durability, not uptime"
        );
    }

    // The group table the `RaftService` routes by. The meta group goes in now;
    // shard groups as the supervisor founds them.
    let routing = GroupRegistry::default();

    let meta_group =
        open_group(&config, group::META, &config.meta_raft_dir(), &config.meta_state_dir())?;
    let meta = start_driver(&config, vec![meta_group]);
    routing.register(group::META, meta.inbox.clone());

    // No shard groups yet: which ones this node holds is the map's answer, and
    // the supervisor founds them once it has one.
    let data = start_driver(&config, Vec::new());

    tracing::info!(
        id = config.id,
        listen = %config.listen,
        peers = config.peers.len(),
        shards = config.num_shards,
        replication_factor = config.replication_factor,
        data_dir = %config.data_dir.display(),
        "kv-node starting"
    );

    // Every node runs a reconciler; only the meta leader's proposals land,
    // and the `Cas` inside settles a race between two that think they lead.
    let published: PublishedMap = PublishedMap::default();
    tokio::spawn(
        MetaReconciler::new(
            config.clone(),
            meta.admin.clone(),
            meta.requests.clone(),
            Arc::clone(&published),
            RECONCILE_INTERVAL,
        )
        .run(),
    );

    // And a supervisor, which founds this node's shards once placement says
    // which they are, and opens whatever is already on disk before that.
    tokio::spawn(
        shards::ShardSupervisor::new(
            config.clone(),
            Arc::clone(&published),
            routing.clone(),
            data.inbox.clone(),
            data.admin.clone(),
            data.new_groups.clone(),
            shards::SUPERVISE_INTERVAL,
        )
        .run(),
    );

    // And the migration driver, which closes the difference between the map
    // and the shard groups this node leads. Every node runs one; a node that
    // leads nothing proposes nothing. Held in an `Arc` because `AdminService`
    // answers `Rebalance` by running a pass on this same driver.
    let migrate = Arc::new(migrate::MigrationDriver::new(
        config.clone(),
        data.admin.clone(),
        meta.admin.clone(),
        Arc::clone(&published),
        migrate::MIGRATE_INTERVAL,
    ));
    tokio::spawn({
        let migrate = Arc::clone(&migrate);
        async move { migrate.run_loop().await }
    });

    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServer::routing(routing)))
        .add_service(KvServiceServer::new(KvApi::new(
            data.requests.clone(),
            config.id,
            Arc::clone(&published),
        )))
        .add_service(AdminServiceServer::new(AdminApi::new(
            meta.admin.clone(),
            meta.requests.clone(),
            data.admin.clone(),
            Arc::clone(&published),
            Arc::clone(&migrate),
        )))
        .serve(config.listen)
        .await?;

    // Both drivers' handles stay alive until the listener stops: dropping a
    // driver's request or admin sender closes that `select!` arm under it.
    drop((data, meta));
    Ok(())
}
