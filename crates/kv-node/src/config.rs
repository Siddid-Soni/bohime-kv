//! Node configuration and CLI (M6).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use kv_raft::NodeId;

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub listen: SocketAddr,
    /// Peer id -> gRPC endpoint. Excludes ourselves.
    pub peers: BTreeMap<NodeId, String>,
    pub data_dir: PathBuf,
    /// Wall-clock length of one Raft logical tick.
    pub tick: Duration,
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
}

impl NodeConfig {
    /// The Raft log. Separate from the state machine: they are two Bitcask
    /// instances with independent key spaces, and sharing a directory would
    /// have log entry keys and user keys colliding in one keydir.
    pub fn raft_dir(&self) -> PathBuf {
        self.data_dir.join("raft")
    }

    pub fn state_dir(&self) -> PathBuf {
        self.data_dir.join("state")
    }

    pub fn raft_config(&self) -> kv_raft::Config {
        kv_raft::Config {
            id: self.id,
            peers: self.peers.keys().copied().collect(),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            // Derived from the id, never a constant: identical seeds make
            // every node draw the identical timeout, campaign on the same
            // tick, split the vote and repeat. That livelock is silent and
            // looks exactly like a network fault.
            seed: self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "kv-node", about = "A Bohime storage node")]
pub struct Args {
    /// This node's id within the Raft group.
    #[arg(long)]
    pub id: NodeId,

    /// Address to serve RaftService and KvService on.
    #[arg(long, default_value = "127.0.0.1:7001")]
    pub listen: SocketAddr,

    /// A peer, as `id=endpoint`. Repeat once per peer.
    #[arg(long = "peer", value_parser = parse_peer)]
    pub peers: Vec<(NodeId, String)>,

    /// Directory for the Raft log and the state machine.
    #[arg(long)]
    pub data_dir: PathBuf,

    #[arg(long, default_value_t = 20)]
    pub tick_ms: u64,

    /// Election timeout in ticks; the real one is drawn from `[t, 2t)`.
    #[arg(long, default_value_t = 15)]
    pub election_timeout: u64,

    /// Ticks between leader heartbeats. Must stay well under the election
    /// timeout, or a healthy leader is deposed between its own heartbeats.
    #[arg(long, default_value_t = 3)]
    pub heartbeat_interval: u64,
}

fn parse_peer(s: &str) -> Result<(NodeId, String), String> {
    let (id, addr) = s.split_once('=').ok_or_else(|| format!("expected id=endpoint, got {s:?}"))?;
    let id: NodeId = id.parse().map_err(|_| format!("bad peer id {id:?}"))?;
    Ok((id, addr.to_string()))
}

impl Args {
    pub fn into_config(self) -> anyhow::Result<NodeConfig> {
        let mut peers = BTreeMap::new();
        for (id, addr) in self.peers {
            if id == self.id {
                anyhow::bail!("node {id} cannot be its own peer");
            }
            if peers.insert(id, addr).is_some() {
                anyhow::bail!("peer {id} given twice");
            }
        }
        Ok(NodeConfig {
            id: self.id,
            listen: self.listen,
            peers,
            data_dir: self.data_dir,
            tick: Duration::from_millis(self.tick_ms),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
        })
    }
}
