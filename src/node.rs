//! One process: a Raft group per shard this node replicates, all driven by a
//! single loop over one Bitcask, served over gRPC.
//!
//! Placement is fixed at startup: a key's shard is `fnv1a(key) % shards`, and
//! shard `s` lives on the `rf` nodes following position `s` in the sorted id
//! list. Every node must be started with the same peers, shards and rf.

use crate::bitcask::Bitcask;
use crate::pb::{self, Batch, Command, Empty, Entry, HardState, Msg, Op, Reply};
use crate::raft::Raft;
use prost::Message;
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};

pub struct Config {
    pub id: u64,
    /// Every node in the cluster, this one included: id -> `host:port`.
    pub nodes: BTreeMap<u64, String>,
    pub shards: u32,
    pub rf: usize,
    pub data_dir: PathBuf,
    pub tick: Duration,
}

impl Config {
    pub fn replicas(&self, shard: u32) -> Vec<u64> {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        (0..self.rf).map(|i| ids[(shard as usize + i) % ids.len()]).collect()
    }
}

pub fn shard_of(key: &[u8], shards: u32) -> u32 {
    let hash =
        key.iter().fold(0xcbf29ce484222325u64, |h, &b| (h ^ b as u64).wrapping_mul(0x100000001b3));
    (hash % shards as u64) as u32
}

enum Event {
    Raft(Msg),
    Client(Command, oneshot::Sender<Reply>),
}

struct Group {
    raft: Raft,
    /// Log index -> (term it was proposed at, the waiting client).
    pending: HashMap<u64, (u64, oneshot::Sender<Reply>)>,
    saved: HardState,
    proposed: bool,
}

struct Node {
    cfg: Config,
    db: Bitcask,
    groups: BTreeMap<u32, Group>,
    peers: HashMap<u64, mpsc::Sender<Msg>>,
}

/// Runs a node until the future is dropped or the server fails.
pub async fn serve(cfg: Config, listener: TcpListener) -> Result<(), tonic::transport::Error> {
    assert!(cfg.rf >= 1 && cfg.rf <= cfg.nodes.len(), "rf must be between 1 and the node count");
    let node = Node::open(cfg).expect("open data dir");
    let (tx, rx) = mpsc::channel(4096);
    let server = Server::builder()
        .add_service(pb::kv_server::KvServer::new(Service(tx.clone())))
        .add_service(pb::raft_server::RaftServer::new(Service(tx)))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::select! {
        () = node.run(rx) => Ok(()),
        result = server => result,
    }
}

impl Node {
    fn open(cfg: Config) -> io::Result<Self> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let db = Bitcask::open(&cfg.data_dir.join("data"))?;
        let mut groups = BTreeMap::new();
        for shard in 0..cfg.shards {
            let replicas = cfg.replicas(shard);
            if !replicas.contains(&cfg.id) {
                continue;
            }
            let saved = match db.get(&hard_state_key(shard))? {
                Some(bytes) => HardState::decode(&bytes[..])?,
                None => HardState::default(),
            };
            let log = (1..=saved.last)
                .map(|i| Ok(Entry::decode(&db.get(&log_key(shard, i))?.expect("log entry")[..])?))
                .collect::<io::Result<_>>()?;
            let peers = replicas.into_iter().filter(|&p| p != cfg.id).collect();
            let raft = Raft::new(cfg.id, peers, saved.term, saved.voted_for, log);
            groups.insert(shard, Group { raft, pending: HashMap::new(), saved, proposed: false });
        }
        let peers = cfg.nodes.iter().filter(|(id, _)| **id != cfg.id);
        let peers = peers.map(|(&id, addr)| (id, spawn_peer(addr))).collect();
        Ok(Self { cfg, db, groups, peers })
    }

    async fn run(mut self, mut events: mpsc::Receiver<Event>) {
        let mut ticker = tokio::time::interval(self.cfg.tick);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.groups.values_mut().for_each(|g| g.raft.tick()),
                Some(event) = events.recv() => {
                    self.handle(event);
                    while let Ok(event) = events.try_recv() {
                        self.handle(event);
                    }
                }
            }
            self.flush().expect("storage failed");
        }
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Raft(m) => {
                if let Some(g) = self.groups.get_mut(&m.shard) {
                    g.raft.step(m);
                }
            }
            Event::Client(cmd, reply) => {
                let shard = shard_of(&cmd.key, self.cfg.shards);
                let Some(g) = self.groups.get_mut(&shard) else {
                    let replica =
                        *self.cfg.replicas(shard).choose(&mut rand::thread_rng()).unwrap();
                    return drop(reply.send(redirect(self.cfg.nodes[&replica].clone())));
                };
                match g.raft.propose(cmd) {
                    Some(index) => {
                        g.pending.insert(index, (g.raft.term, reply));
                        g.proposed = true;
                    }
                    None => {
                        let leader =
                            self.cfg.nodes.get(&g.raft.leader).cloned().unwrap_or_default();
                        let _ = reply.send(redirect(leader));
                    }
                }
            }
        }
    }

    /// Persist, fsync once, then send and apply: a vote or an entry never
    /// leaves this node, and no client hears "done", before it is on disk.
    fn flush(&mut self) -> io::Result<()> {
        let mut wrote = false;
        for (&shard, g) in &mut self.groups {
            if std::mem::take(&mut g.proposed) {
                g.raft.broadcast();
            }
            wrote |= persist(&mut self.db, shard, g)?;
        }
        if wrote {
            self.db.sync()?;
        }
        for (&shard, g) in &mut self.groups {
            for mut m in g.raft.outbox.drain(..) {
                m.shard = shard;
                // A full queue drops the message; Raft retries on its own.
                let _ = self.peers[&m.to].try_send(m);
            }
            while g.raft.applied < g.raft.commit {
                let index = g.raft.applied + 1;
                g.raft.applied = index;
                let entry = &g.raft.log[index as usize];
                let result = entry.cmd.as_ref().map(|cmd| apply(&mut self.db, shard, cmd));
                if let Some((term, reply)) = g.pending.remove(&index) {
                    // A different term here means our entry was overwritten.
                    let _ = reply.send(match result {
                        Some(result) if term == entry.term => result?,
                        _ => redirect(String::new()),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Writes whatever changed since the last call. The hard state goes after the
/// entries it counts, so a torn write never leaves it pointing past them.
fn persist(db: &mut Bitcask, shard: u32, g: &mut Group) -> io::Result<bool> {
    let r = &mut g.raft;
    let hs = HardState { term: r.term, voted_for: r.voted_for, last: r.log.len() as u64 - 1 };
    if hs == g.saved && r.stable == r.log.len() {
        return Ok(false);
    }
    for i in r.stable..r.log.len() {
        db.put(&log_key(shard, i as u64), &r.log[i].encode_to_vec())?;
    }
    db.put(&hard_state_key(shard), &hs.encode_to_vec())?;
    r.stable = r.log.len();
    g.saved = hs;
    Ok(true)
}

/// The state machine is not fsynced: after a crash the log is replayed over
/// it from the start, and replaying puts and deletes in order is idempotent.
fn apply(db: &mut Bitcask, shard: u32, cmd: &Command) -> io::Result<Reply> {
    let key = [b"d".as_slice(), &shard.to_be_bytes(), &cmd.key].concat();
    let value = match cmd.op() {
        Op::Get => db.get(&key)?,
        Op::Put => db.put(&key, &cmd.value).map(|()| None)?,
        Op::Delete => db.delete(&key).map(|()| None)?,
    };
    Ok(Reply { ok: true, value, leader: String::new() })
}

fn hard_state_key(shard: u32) -> Vec<u8> {
    [b"h".as_slice(), &shard.to_be_bytes()].concat()
}

fn log_key(shard: u32, index: u64) -> Vec<u8> {
    [b"l".as_slice(), &shard.to_be_bytes(), &index.to_be_bytes()].concat()
}

fn redirect(leader: String) -> Reply {
    Reply { ok: false, value: None, leader }
}

/// A queue drained into one `Send` RPC per round trip. The channel reconnects
/// by itself; failures are dropped, since Raft resends on the next tick.
fn spawn_peer(addr: &str) -> mpsc::Sender<Msg> {
    let (tx, mut rx) = mpsc::channel(4096);
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).expect("peer address");
    let timeout = Duration::from_secs(1);
    let channel = endpoint.connect_timeout(timeout).timeout(timeout).connect_lazy();
    let mut client = pb::raft_client::RaftClient::new(channel);
    tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            let mut msgs = vec![m];
            while let Ok(m) = rx.try_recv() {
                msgs.push(m);
            }
            let _ = client.send(Batch { msgs }).await;
        }
    });
    tx
}

struct Service(mpsc::Sender<Event>);

#[tonic::async_trait]
impl pb::kv_server::Kv for Service {
    async fn call(&self, request: Request<Command>) -> Result<Response<Reply>, Status> {
        let (tx, rx) = oneshot::channel();
        let gone = || Status::unavailable("node is shutting down");
        self.0.send(Event::Client(request.into_inner(), tx)).await.map_err(|_| gone())?;
        rx.await.map(Response::new).map_err(|_| gone())
    }
}

#[tonic::async_trait]
impl pb::raft_server::Raft for Service {
    async fn send(&self, request: Request<Batch>) -> Result<Response<Empty>, Status> {
        for m in request.into_inner().msgs {
            let _ = self.0.send(Event::Raft(m)).await;
        }
        Ok(Response::new(Empty {}))
    }
}
