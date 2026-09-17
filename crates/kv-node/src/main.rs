//! `kv-node`: the impure shell binding kv-storage + kv-raft to real
//! tokio/tonic I/O (M6).
//!
//! One listener carries both services. There is no reason to make an operator
//! manage two ports for one process, and the Raft peers and the clients reach
//! the same driver either way.

mod command;
mod config;
mod driver;
mod kv_service;
mod session;
mod storage;
mod transport;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use clap::Parser;
use kv_proto::kv::kv_service_server::KvServiceServer;
use kv_proto::raft::raft_service_server::RaftServiceServer;
use kv_raft::RaftNode;
use kv_storage::Engine;
use tokio::sync::mpsc;

use crate::config::Args;
use crate::driver::Driver;
use crate::kv_service::KvApi;
use crate::storage::BitcaskStorage;
use crate::transport::PeerLink;
use crate::transport::peer::{PeerClient, PeerConfig};
use crate::transport::server::RaftServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Args::parse().into_config()?;
    std::fs::create_dir_all(config.raft_dir())?;
    std::fs::create_dir_all(config.state_dir())?;

    let node = RaftNode::new(config.raft_config(), BitcaskStorage::open(config.raft_dir())?);
    let engine = Engine::open(config.state_dir())?;

    let (inbox_tx, inbox) = mpsc::channel(256);
    let (replies_tx, replies) = mpsc::channel(256);
    let (requests_tx, requests) = mpsc::channel(256);

    // `connect` never blocks on a peer being up — a node must campaign whether
    // or not its peers exist yet, which is also what lets a cluster be started
    // in any order.
    let mut peers = BTreeMap::new();
    for (&id, addr) in &config.peers {
        let client =
            PeerClient::connect(id, addr.clone(), PeerConfig::default(), replies_tx.clone());
        peers.insert(id, Box::new(client) as Box<dyn PeerLink>);
    }

    tracing::info!(
        id = config.id,
        listen = %config.listen,
        peers = peers.len(),
        data_dir = %config.data_dir.display(),
        "kv-node starting"
    );

    let driver = Driver::new(&config, node, engine, peers, inbox, replies, requests);
    tokio::spawn(async move {
        if let Err(e) = driver.run().await {
            tracing::error!(error = %e, "driver stopped");
        }
    });

    tonic::transport::Server::builder()
        .add_service(RaftServiceServer::new(RaftServer::new(inbox_tx)))
        .add_service(KvServiceServer::new(KvApi::new(requests_tx)))
        .serve(config.listen)
        .await?;
    Ok(())
}
