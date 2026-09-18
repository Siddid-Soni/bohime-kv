//! Node configuration and CLI (M6).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use kv_raft::NodeId;
use kv_ring::ShardId;
use kv_storage::{EngineConfig, FsyncPolicy, IndexKind};

use crate::transport::group::GroupId;

/// `--keydir`'s values, as clap sees them. A separate enum from
/// [`IndexKind`] only so the CLI's spelling is the CLI's business.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum KeydirImpl {
    #[default]
    #[value(name = "left-right")]
    LeftRight,
    #[value(name = "locked")]
    Locked,
}

impl From<KeydirImpl> for IndexKind {
    fn from(value: KeydirImpl) -> Self {
        match value {
            KeydirImpl::LeftRight => IndexKind::LeftRight,
            KeydirImpl::Locked => IndexKind::Locked,
        }
    }
}

/// `--log-fsync`'s values. A separate enum from [`FsyncPolicy`] for the same
/// reason [`KeydirImpl`] is separate from [`IndexKind`]: the CLI's spelling is
/// the CLI's business, and `GroupCommit`'s two parameters are not knobs an
/// operator should have to reason about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum LogFsync {
    /// One fsync per entry, plus one for the log metadata. What the node did
    /// through M11.5, and ~7× more fsyncs per write than the log needs.
    #[value(name = "every-write")]
    EveryWrite,
    /// One fsync per drain, however many entries the drain persisted.
    #[default]
    #[value(name = "group-commit")]
    GroupCommit,
    /// No fsync at all. **Loses acknowledged writes on a machine crash** and
    /// exists as the benchmark's no-durability ceiling, nothing else.
    #[value(name = "never")]
    Never,
}

/// The batch bound under `--log-fsync group-commit`.
///
/// Deliberately loose. The bound that actually holds is `Group::drain`, which
/// syncs before it sends anything, so the policy never decides whether a write
/// is durable in time — it is a backstop against a write that somehow reaches
/// the engine and is never drained. Tight values here would defeat the whole
/// point by firing between drains under light load.
const GROUP_COMMIT_MAX_RECORDS: usize = 4096;
const GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_millis(100);

impl From<LogFsync> for FsyncPolicy {
    fn from(value: LogFsync) -> Self {
        match value {
            LogFsync::EveryWrite => FsyncPolicy::EveryWrite,
            LogFsync::GroupCommit => FsyncPolicy::GroupCommit {
                max_records: GROUP_COMMIT_MAX_RECORDS,
                max_delay: GROUP_COMMIT_MAX_DELAY,
            },
            LogFsync::Never => FsyncPolicy::Never,
        }
    }
}

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
    /// Opt-in lease reads (§1.10). See `Args::lease_reads`.
    pub lease_reads: bool,
    /// Which keydir each state machine holds (M11.5). See `Args::keydir`.
    pub keydir: IndexKind,
    /// Applied log entries past the last snapshot before the next one is
    /// taken (M8). Bounds log growth and follower catch-up time; each snapshot
    /// scans the whole state, so this is also its amortized cost knob.
    pub snapshot_threshold: u64,
    /// This node is *joining* an existing cluster rather than founding one
    /// (M9.2): it starts as a learner and waits to be admitted, instead of
    /// counting itself a voter and campaigning. Only consulted on a fresh
    /// store — once the log or a snapshot holds membership, they own it.
    pub initial_learner: bool,
    /// Bootstrap-only placement parameters (M10).
    ///
    /// These seed **version 1** of the shard map and nothing else. Once the
    /// meta group holds a map, the map owns them — exactly as the log owns
    /// membership once it exists, which is what `initial_learner` above means
    /// by "bootstrap only". A node whose flags disagree with the replicated
    /// map is misconfigured rather than merely stale: `hash % 256` and
    /// `hash % 512` disagree about nearly every key, so continuing would
    /// misroute the whole keyspace.
    pub num_shards: u16,
    pub replication_factor: u8,
    pub vnodes_per_node: u32,
    /// How many shards this node will move at once during a rebalance
    /// (M12.1).
    ///
    /// Per node rather than per cluster: the expensive half of a move is one
    /// receiver's snapshot ingest and its two Bitcask opens, and a cap on
    /// each side bounds that without anybody coordinating. A node leading 154
    /// shards moves this many at a time; moving many at once is how a
    /// rebalance becomes an outage.
    pub max_migrations: usize,
    /// How the **Raft log** reaches stable storage (see `--log-fsync`).
    ///
    /// Not the state machine's: a replicated state machine is durable through
    /// the log, so its own engine's policy is a separate question with a
    /// separate answer.
    pub log_fsync: FsyncPolicy,
    /// How each **state machine** reaches stable storage (see
    /// `--state-fsync`). A different question from the log's, with a different
    /// answer: the state machine is a cache of the log.
    pub state_fsync: FsyncPolicy,
}

impl NodeConfig {
    /// How long a leadership lease is good for, once a quorum has acked.
    ///
    /// `election_timeout` minus a drift margin: no follower will start an
    /// election before its own election timeout elapses, so within that window
    /// nobody else can have become leader — **provided the clocks agree**.
    /// The margin is what pays for them not quite agreeing, and it is why this
    /// is off by default.
    pub fn lease_duration(&self) -> Duration {
        let full = self.tick * (self.election_timeout as u32);
        // Two thirds: a blunt but honest drift allowance. A real deployment
        // would derive it from a measured clock-drift bound, which is exactly
        // the assumption ReadIndex does not make.
        full * 2 / 3
    }
}

impl NodeConfig {
    /// Where every shard this node replicates lives (M11.1).
    pub fn shards_dir(&self) -> PathBuf {
        self.data_dir.join("shards")
    }

    /// One shard's directory. Zero-padded so a listing sorts numerically,
    /// which is what makes `placement::hosted_on_disk` reading it back in
    /// order free rather than a sort.
    pub fn shard_dir(&self, shard: ShardId) -> PathBuf {
        self.shards_dir().join(format!("{shard:05}"))
    }

    /// One shard's Raft log. Separate from that shard's state machine, and
    /// from every other shard's: they are all Bitcask instances with
    /// independent key spaces, and two sharing a directory would have one
    /// keydir holding two — shard 7's log uses the very same `e{index:020}`
    /// entry keys shard 8's does.
    pub fn shard_raft_dir(&self, shard: ShardId) -> PathBuf {
        self.shard_dir(shard).join("raft")
    }

    pub fn shard_state_dir(&self, shard: ShardId) -> PathBuf {
        self.shard_dir(shard).join("state")
    }

    /// How every state machine on this node is opened.
    ///
    /// The fsync policy here used to be the engine's default, `EveryWrite`,
    /// directly under a comment saying a replicated state machine "is durable
    /// through the Raft log rather than through its own fsync". The comment
    /// was right and the code disagreed with it: every applied `Put` cost two
    /// extra fsyncs a node, its value and its session-table row, and measured
    /// at 8 shards those were the larger half of the whole write budget —
    /// 119 writes/s with them, 286 without.
    ///
    /// What makes dropping them safe is `Group::maybe_snapshot`, which fsyncs
    /// the state machine before `take_snapshot` drops the log prefix that
    /// would otherwise replay it. See `--state-fsync`.
    pub fn engine_config(&self) -> EngineConfig {
        EngineConfig { index: self.keydir, fsync_policy: self.state_fsync, ..Default::default() }
    }

    /// The single data group's directories, as they were through M10.
    ///
    /// Nothing writes here from M11 on. They are named only so
    /// `placement::check_layout` can refuse a store that has them: one log
    /// holding keys from every shard cannot be split without moving data,
    /// which is M12's migration driver rather than a startup path.
    pub fn legacy_raft_dir(&self) -> PathBuf {
        self.data_dir.join("raft")
    }

    pub fn legacy_state_dir(&self) -> PathBuf {
        self.data_dir.join("state")
    }

    /// The meta group's log and state machine (M10.5).
    ///
    /// Four Bitcask instances under one data dir, and all four must stay
    /// apart: one keydir cannot hold two key spaces, and the meta group's log
    /// uses the very same `e{index:020}` entry keys the data group's does.
    pub fn meta_raft_dir(&self) -> PathBuf {
        self.data_dir.join("meta-raft")
    }

    pub fn meta_state_dir(&self) -> PathBuf {
        self.data_dir.join("meta-state")
    }

    /// The data group's core config. `cfg(test)`: `main` starts each group
    /// through `raft_config_for`, and a bare `raft_config()` would quietly
    /// mean "the data one" at a call site that hosts two.
    #[cfg(test)]
    pub fn raft_config(&self) -> kv_raft::Config {
        self.raft_config_for(crate::transport::group::DATA)
    }

    /// One group's core config. The membership and timings are shared — both
    /// groups run on the same nodes at the same tick — but the seed is not:
    /// two groups drawing identical election timeouts on every node would
    /// campaign in lockstep, so each node's two groups would contend for the
    /// same disk at the same instant, forever.
    pub fn raft_config_for(&self, group: GroupId) -> kv_raft::Config {
        kv_raft::Config {
            id: self.id,
            peers: self.peers.keys().copied().collect(),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            // Derived from the id, never a constant: identical seeds make
            // every node draw the identical timeout, campaign on the same
            // tick, split the vote and repeat. That livelock is silent and
            // looks exactly like a network fault.
            seed: self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ (group as u64).wrapping_mul(0xD1B5_4A32_D192_ED03),
            // Bootstrap only: a process that starts with an empty store is a
            // founder unless `--join` says otherwise. After that the log owns
            // the membership and this is history.
            initial_learner: self.initial_learner,
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

    /// A cluster member, as `id=endpoint`. Repeat once per member.
    ///
    /// This node may appear in its own list and is filtered out, so the same
    /// `--peer` flags can be handed to every process in the cluster and only
    /// `--id` and `--listen` differ. Rejecting self instead would make the
    /// natural launch script an error.
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

    /// Serve reads from the leader's lease instead of confirming a quorum per
    /// read (§1.10). Faster — zero round trips — but **correct only if clock
    /// drift between nodes stays within the margin**, an assumption ReadIndex
    /// does not make. Off by default, deliberately: the safe path is the one
    /// you get without asking.
    #[arg(long, default_value_t = false)]
    pub lease_reads: bool,

    /// Which keydir the state machines hold (M11.5, §1.15).
    ///
    /// `left-right` keeps two copies so readers never take a lock; `locked`
    /// is one `RwLock<HashMap>`, where every reader contends with every other
    /// and with the writer. left-right is the default and the faster one under
    /// concurrent reads, and it costs a second copy of the keydir — ~40 bytes
    /// a key, all keys resident, so 10M keys a shard is 400MB against 800MB.
    /// `locked` exists so that is a decision an operator can make, and so the
    /// two can be compared on one build.
    #[arg(long, value_enum, default_value_t = KeydirImpl::LeftRight)]
    pub keydir: KeydirImpl,

    /// Applied entries past the last snapshot before the next one is taken.
    #[arg(long, default_value_t = 10_000)]
    pub snapshot_threshold: u64,

    /// Shards to split the keyspace into (M10). **Fixed at cluster creation.**
    ///
    /// Only the founding cluster's value is ever used: it seeds version 1 of
    /// the shard map, after which the replicated map owns it. Changing it
    /// later is not a reconfiguration but a full remap of every key, so a
    /// node whose flag disagrees with the stored map refuses to start.
    #[arg(long = "shards", default_value_t = 256)]
    pub num_shards: u16,

    /// Replicas per shard (M10). Also fixed at cluster creation.
    ///
    /// 1 is legal and means no replication — a single-node development mode
    /// that loses the data with the disk. 2 is legal but tolerates zero
    /// failures *and* is less available than 1, since either node being down
    /// stops the group; what it buys is durability. 3 is the first value that
    /// survives losing a node. A factor larger than the cluster is refused
    /// rather than quietly under-replicated.
    #[arg(long = "replication-factor", default_value_t = 3)]
    pub replication_factor: u8,

    /// Ring positions per physical node (M10).
    ///
    /// Balance, not correctness: with one position per node the ring is carved
    /// into arcs of wildly uneven length and one node ends up holding several
    /// times its share of shards.
    #[arg(long = "vnodes", default_value_t = kv_ring::DEFAULT_VNODES)]
    pub vnodes_per_node: u32,

    /// Shards this node will move at once during a rebalance (M12.1).
    ///
    /// A rebalance is carried out by each shard's leader independently, so
    /// there is no cluster-wide throttle to set — this bounds what one node
    /// will start, and the receiving node's supervisor opens shards one at a
    /// time on top of that. Raising it moves data faster and takes more of
    /// the cluster's disk and network away from foreground traffic.
    #[arg(long, default_value_t = 4)]
    pub max_migrations: usize,

    /// Start as a learner waiting to be admitted, instead of as a founding
    /// member.
    ///
    /// Pass `--peer` for the cluster as it stands: those become this node's
    /// voters, and this node becomes a learner among them. It still needs
    /// them from argv because the founders' own voter-hood came from argv
    /// too and was never written to the log — there is no conf entry to
    /// replay it from. Everything that changed *since* is in the log, so
    /// members admitted after those founders arrive on their own, addresses
    /// included.
    ///
    /// Bring the node in with `AddNode` on the AdminService once it is
    /// listening. Without this flag a fresh node counts itself a voter and
    /// campaigns, which on an existing cluster is a node forming a rival
    /// group of one.
    #[arg(long, default_value_t = false)]
    pub join: bool,

    /// How the Raft log reaches stable storage.
    ///
    /// `group-commit` (the default) fsyncs once per drain of the Raft
    /// `Ready`, however many entries that drain persisted. It is safe because
    /// the drain syncs *before* it puts anything on the wire — §1.5, disk
    /// before network — so no vote is granted and no entry counted toward a
    /// commit until it is on the disk. `every-write` is the pre-M11.5
    /// behaviour, one fsync per entry plus one for the log metadata, and is
    /// kept so the two can be compared on one build. `never` is for
    /// benchmarks: it loses acknowledged writes on a machine crash.
    #[arg(long, value_enum, default_value_t = LogFsync::GroupCommit)]
    pub log_fsync: LogFsync,

    /// How each state machine reaches stable storage.
    ///
    /// Separate from `--log-fsync` because the two are not the same question.
    /// A state machine holds nothing the Raft log does not already hold
    /// durably, and a restart replays the log tail over it, so its own fsyncs
    /// buy no data that would otherwise be lost — they only shorten replay.
    /// The one place that stops being true is a snapshot, which drops the log
    /// prefix; `Group::maybe_snapshot` fsyncs the state machine first, which
    /// is the single fsync this policy still needs and cannot skip.
    ///
    /// `group-commit` is the default. `every-write` is the pre-M11.5
    /// behaviour: two extra fsyncs per applied write per node, for a faster
    /// restart.
    #[arg(long, value_enum, default_value_t = LogFsync::GroupCommit)]
    pub state_fsync: LogFsync,
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
            // Ourselves: expected in a uniform cluster list, and not a peer.
            if id == self.id {
                continue;
            }
            if peers.insert(id, addr).is_some() {
                anyhow::bail!("peer {id} given twice");
            }
        }
        // The cluster this node is founding, counting itself. Refused here
        // rather than at the first rebalance so the operator sees it at
        // startup, which is the decision recorded in the M10 plan: a config
        // that cannot be satisfied fails loudly instead of under-replicating.
        let cluster_size = peers.len() + 1;
        if self.replication_factor == 0 {
            anyhow::bail!("--replication-factor must be at least 1");
        }
        if !self.join && self.replication_factor as usize > cluster_size {
            anyhow::bail!(
                "--replication-factor {} exceeds the {cluster_size}-node cluster in --peer; \
                 a shard cannot have more replicas than there are nodes to put them on",
                self.replication_factor,
            );
        }
        if self.num_shards == 0 {
            anyhow::bail!("--shards must be at least 1");
        }
        if self.vnodes_per_node == 0 {
            anyhow::bail!("--vnodes must be at least 1");
        }

        Ok(NodeConfig {
            id: self.id,
            listen: self.listen,
            peers,
            data_dir: self.data_dir,
            tick: Duration::from_millis(self.tick_ms),
            election_timeout: self.election_timeout,
            heartbeat_interval: self.heartbeat_interval,
            lease_reads: self.lease_reads,
            keydir: self.keydir.into(),
            snapshot_threshold: self.snapshot_threshold,
            initial_learner: self.join,
            num_shards: self.num_shards,
            replication_factor: self.replication_factor,
            vnodes_per_node: self.vnodes_per_node,
            max_migrations: self.max_migrations,
            log_fsync: self.log_fsync.into(),
            state_fsync: self.state_fsync.into(),
        })
    }
}
