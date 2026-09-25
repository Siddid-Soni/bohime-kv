//! One process: one replica of one shard, a Raft group driven by one loop
//! over one Bitcask file, served over gRPC.
//!
//! Placement is fixed at startup: the sorted node ids are cut into runs of
//! `rf`, one shard per run, so there are `nodes / rf` shards. A key's shard
//! is `fnv1a(key) % shards`. Every node must be started with the same nodes
//! and rf.

use crate::bitcask::Bitcask;
use crate::pb::{self, Batch, Command, Empty, Entry, HardState, Msg, Op, Reply};
use crate::raft::Raft;
use prost::Message;
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};

pub struct Config {
    pub id: u64,
    /// Every node in the cluster, this one included: id -> `host:port`.
    pub nodes: BTreeMap<u64, String>,
    pub rf: usize,
    pub data_dir: PathBuf,
    pub tick: Duration,
}

impl Config {
    pub fn shards(&self) -> u32 {
        (self.nodes.len() / self.rf) as u32
    }

    /// Shard `s` is the `s`th run of `rf` nodes in id order.
    pub fn replicas(&self, shard: u32) -> Vec<u64> {
        self.nodes.keys().copied().skip(shard as usize * self.rf).take(self.rf).collect()
    }

    /// The one shard this node replicates.
    pub fn shard(&self) -> u32 {
        let position = self.nodes.keys().position(|&id| id == self.id).expect("own id in nodes");
        (position / self.rf) as u32
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

type Peers = Arc<HashMap<u64, mpsc::Sender<Msg>>>;

/// A batch whose fsync is running, with the messages and the commit index
/// that wait for it.
type Syncing = (JoinHandle<io::Result<()>>, Vec<Msg>, u64);

/// This node's replica of its shard: the Raft group and its file.
struct Shard {
    id: u32,
    cfg: Arc<Config>,
    db: Bitcask,
    /// The same file as `db`, to fsync on a blocking thread.
    file: Arc<File>,
    raft: Raft,
    /// Log index -> (term it was proposed at, the waiting client).
    pending: HashMap<u64, (u64, oneshot::Sender<Reply>)>,
    saved: HardState,
    proposed: bool,
    peers: Peers,
}

/// Runs a node until the future is dropped or the server fails.
pub async fn serve(cfg: Config, listener: TcpListener) -> Result<(), tonic::transport::Error> {
    assert!(
        cfg.rf >= 1 && cfg.nodes.len().is_multiple_of(cfg.rf),
        "the node count must be a multiple of rf"
    );
    let cfg = Arc::new(cfg);
    let shard = cfg.shard();
    let peers = cfg.replicas(shard).into_iter().filter(|&id| id != cfg.id);
    let peers: Peers = Arc::new(peers.map(|id| (id, spawn_peer(&cfg.nodes[&id]))).collect());
    let node = Shard::open(cfg.clone(), shard, peers).expect("open data dir");
    let (tx, rx) = mpsc::channel(4096);
    let service = Service { cfg, shard, driver: tx };
    // A peer batch merges every queued append, so it can outgrow gRPC's 4 MB
    // default even though each append is capped; a rejected batch would be
    // resent forever.
    let raft =
        pb::raft_server::RaftServer::new(service.clone()).max_decoding_message_size(usize::MAX);
    let server = Server::builder()
        .add_service(pb::kv_server::KvServer::new(service))
        .add_service(raft)
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::select! {
        () = node.run(rx) => Ok(()),
        result = server => result,
    }
}

impl Shard {
    fn open(cfg: Arc<Config>, id: u32, peers: Peers) -> io::Result<Self> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let db = Bitcask::open(&cfg.data_dir.join("data"))?;
        let file = Arc::new(db.file()?);
        let saved = match db.get(HARD_STATE)? {
            Some(bytes) => HardState::decode(&bytes[..])?,
            None => HardState::default(),
        };
        let log = (1..=saved.last)
            .map(|i| Ok(Entry::decode(&db.get(&log_key(i))?.expect("log entry")[..])?))
            .collect::<io::Result<_>>()?;
        let others = cfg.replicas(id).into_iter().filter(|&p| p != cfg.id).collect();
        let raft = Raft::new(cfg.id, others, saved.term, saved.voted_for, log);
        Ok(Self { id, cfg, db, file, raft, pending: HashMap::new(), saved, proposed: false, peers })
    }

    /// Group commit: while one batch's fsync runs, events keep arriving and
    /// stepping Raft; the next batch is cut when it finishes.
    async fn run(mut self, mut events: mpsc::Receiver<Event>) {
        let mut ticker = tokio::time::interval(self.cfg.tick);
        let mut syncing: Option<Syncing> = None;
        loop {
            tokio::select! {
                _ = ticker.tick() => self.raft.tick(),
                Some(event) = events.recv() => {
                    self.handle(event);
                    while let Ok(event) = events.try_recv() {
                        self.handle(event);
                    }
                }
                synced = async { (&mut syncing.as_mut().unwrap().0).await }, if syncing.is_some() => {
                    synced.expect("fsync task").expect("storage failed");
                    let (_, msgs, commit) = syncing.take().unwrap();
                    self.release(msgs, commit).expect("storage failed");
                }
            }
            if syncing.is_none() {
                syncing = self.cut().expect("storage failed");
            }
        }
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Raft(m) => self.raft.step(m),
            Event::Client(cmd, reply) => match self.raft.propose(cmd) {
                Some(index) => {
                    self.pending.insert(index, (self.raft.term, reply));
                    self.proposed = true;
                }
                None => {
                    let leader = self.cfg.nodes.get(&self.raft.leader).cloned().unwrap_or_default();
                    let _ = reply.send(redirect(leader));
                }
            },
        }
    }

    /// Persists and starts the fsync; this batch's messages and commit are
    /// released once it finishes, so a vote or an entry never leaves this
    /// node, and no client hears "done", before it is on disk. With nothing
    /// to persist they are released at once.
    fn cut(&mut self) -> io::Result<Option<Syncing>> {
        if std::mem::take(&mut self.proposed) {
            self.raft.broadcast();
        }
        let wrote = self.persist()?;
        self.db.flush()?;
        let msgs = std::mem::take(&mut self.raft.outbox);
        let commit = self.raft.commit;
        if !wrote {
            self.release(msgs, commit)?;
            return Ok(None);
        }
        let file = self.file.clone();
        Ok(Some((tokio::task::spawn_blocking(move || file.sync_data()), msgs, commit)))
    }

    /// Sends a synced batch's messages and applies up to its commit index:
    /// entries committed since may not be on disk here yet.
    fn release(&mut self, msgs: Vec<Msg>, commit: u64) -> io::Result<()> {
        for mut m in msgs {
            m.shard = self.id;
            // A full queue drops the message; Raft retries on its own.
            let _ = self.peers[&m.to].try_send(m);
        }
        while self.raft.applied < commit {
            let index = self.raft.applied + 1;
            self.raft.applied = index;
            let entry = &self.raft.log[index as usize];
            let result = entry.cmd.as_ref().map(|cmd| apply(&mut self.db, cmd));
            if let Some((term, reply)) = self.pending.remove(&index) {
                // A different term here means our entry was overwritten.
                let _ = reply.send(match result {
                    Some(result) if term == entry.term => result?,
                    _ => redirect(String::new()),
                });
            }
        }
        Ok(())
    }

    /// Buffers whatever changed since the last call. The hard state goes
    /// after the entries it counts, so a torn write never leaves it pointing
    /// past them.
    fn persist(&mut self) -> io::Result<bool> {
        let r = &mut self.raft;
        let hs = HardState { term: r.term, voted_for: r.voted_for, last: r.log.len() as u64 - 1 };
        if hs == self.saved && r.stable == r.log.len() {
            return Ok(false);
        }
        for i in r.stable..r.log.len() {
            self.db.put(&log_key(i as u64), &r.log[i].encode_to_vec())?;
        }
        self.db.put(HARD_STATE, &hs.encode_to_vec())?;
        r.stable = r.log.len();
        self.saved = hs;
        Ok(true)
    }
}

/// The state machine is not fsynced: after a crash the log is replayed over
/// it from the start, and replaying puts and deletes in order is idempotent.
fn apply(db: &mut Bitcask, cmd: &Command) -> io::Result<Reply> {
    let key = [b"d".as_slice(), &cmd.key].concat();
    let value = match cmd.op() {
        Op::Get => db.get(&key)?,
        Op::Put => db.put(&key, &cmd.value).map(|()| None)?,
        Op::Delete => db.delete(&key).map(|()| None)?,
    };
    Ok(Reply { ok: true, value, leader: String::new() })
}

const HARD_STATE: &[u8] = b"h";

fn log_key(index: u64) -> Vec<u8> {
    [b"l".as_slice(), &index.to_be_bytes()].concat()
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

#[derive(Clone)]
struct Service {
    cfg: Arc<Config>,
    shard: u32,
    driver: mpsc::Sender<Event>,
}

#[tonic::async_trait]
impl pb::kv_server::Kv for Service {
    async fn call(&self, request: Request<Command>) -> Result<Response<Reply>, Status> {
        let cmd = request.into_inner();
        let shard = shard_of(&cmd.key, self.cfg.shards());
        if shard != self.shard {
            let replica = *self.cfg.replicas(shard).choose(&mut rand::thread_rng()).unwrap();
            return Ok(Response::new(redirect(self.cfg.nodes[&replica].clone())));
        }
        let (tx, rx) = oneshot::channel();
        let gone = || Status::unavailable("node is shutting down");
        self.driver.send(Event::Client(cmd, tx)).await.map_err(|_| gone())?;
        rx.await.map(Response::new).map_err(|_| gone())
    }
}

#[tonic::async_trait]
impl pb::raft_server::Raft for Service {
    async fn send(&self, request: Request<Batch>) -> Result<Response<Empty>, Status> {
        for m in request.into_inner().msgs {
            if m.shard == self.shard {
                let _ = self.driver.send(Event::Raft(m)).await;
            }
        }
        Ok(Response::new(Empty {}))
    }
}
