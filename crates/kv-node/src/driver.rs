//! The driver loop (M6, multi-group since M11.4): the one task that owns this
//! process's Raft nodes and their state machines, and the only thing that
//! touches either.
//!
//! Everything else in the process — the gRPC services, the per-peer clients —
//! reaches it over a bounded channel and waits on a `oneshot`. That is what
//! keeps `RaftNode` single-threaded and synchronous, which is the property
//! M4's determinism rests on, while the shell around it is fully async.
//!
//! **One tick loop across every group, never one task per group.** A node
//! replicating 154 of 256 shards hosts 154 Raft groups; a task and an
//! interval each would be 154 timers firing every tick and 154 `select!`
//! loops contending for one disk. So `Driver` owns a map of [`Group`]s, one
//! `interval` drives all of their `tick`s, and a drain runs per group. That is
//! the design M11 exists to get right, and the naive one it exists to avoid.
//!
//! Every drain runs in one order, and the order is a correctness requirement
//! rather than an optimisation (§1.5): **sync the log, then send, then
//! apply.** A vote that reaches the wire before it reaches the disk lets a
//! crash produce a second vote in the same term, and election safety is gone.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kv_raft::membership::{ConfChange, ConfOp, decode_conf, is_conf_change};
use kv_raft::storage::RaftStorage;
use kv_raft::{
    ConfProposeError, LogIndex, Message, NodeId, ProposeError, RaftNode, ReadIndexError, Role,
    Snapshot, Term,
};
use kv_ring::ShardId;
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, Mutation};
use crate::config::NodeConfig;
use crate::membership::{ConfEffect, apply_conf, encode_book, endpoints};
use crate::read_view::{GroupReads, ReadResolvers, Visibility};
use crate::session::{self, CommandResponse, RequestCtx};
use crate::storage::BitcaskStorage;
use crate::transport::group::{self, GroupId};
use crate::transport::server::Inbound;
use crate::transport::{PeerFactory, PeerLink};

#[derive(Debug)]
pub enum ClientOp {
    Get {
        key: Vec<u8>,
    },
    /// A mutation, with the context that makes a retry recognisable as the
    /// same request. `ctx: None` opts out of retry tracking — legitimate for
    /// `Put` and `Delete`, which are idempotent, and a mistake for `Cas`.
    Mutate {
        ctx: Option<RequestCtx>,
        op: Mutation,
    },
}

/// Convenience constructors for call sites that do not track retries.
/// `cfg(test)` because only tests build a `ClientOp` by hand — the service
/// always has a `ClientContext` from the request to pass through.
#[cfg(test)]
impl ClientOp {
    pub fn put(key: &[u8], value: &[u8]) -> ClientOp {
        ClientOp::Mutate {
            ctx: None,
            op: Mutation::Put { key: key.to_vec(), value: value.to_vec() },
        }
    }

    pub fn delete(key: &[u8]) -> ClientOp {
        ClientOp::Mutate { ctx: None, op: Mutation::Delete { key: key.to_vec() } }
    }

    pub fn get(key: &[u8]) -> ClientOp {
        ClientOp::Get { key: key.to_vec() }
    }
}

#[derive(Debug)]
pub enum ClientReply {
    Value(Option<Vec<u8>>),
    Applied,
    /// Whether a compare-and-swap took effect.
    Swapped(bool),
    NotLeader {
        hint: Option<NodeId>,
    },
    /// This node does not replicate the shard the key belongs to (M11.4).
    ///
    /// An ordinary answer rather than an error, for the same reason
    /// `NotLeader` is: it is what a client with a shard map one version old
    /// gets, and a client handed an error drops the connection instead of
    /// re-routing. The client refreshes its map and retries.
    NotHosted {
        shard: ShardId,
    },
}

impl From<CommandResponse> for ClientReply {
    fn from(response: CommandResponse) -> Self {
        match response {
            CommandResponse::Applied => ClientReply::Applied,
            CommandResponse::Swapped(swapped) => ClientReply::Swapped(swapped),
        }
    }
}

/// One cluster-administration request (M9.2). Membership changes go through
/// the log like everything else, so they arrive on the same driver task as
/// client traffic rather than touching `RaftNode` from a service handler.
#[derive(Debug)]
pub enum AdminOp {
    /// Bring `id` into the cluster, reachable at `address`.
    ///
    /// It joins as a **learner**: it replicates and it does not vote. A cold
    /// node made a voter on arrival counts toward every quorum while holding
    /// none of the log, which stalls commits for as long as the catch-up
    /// takes — the unavailability window M9's criteria rule out. The leader
    /// promotes it once it has caught up.
    AddNode {
        id: NodeId,
        address: String,
    },
    /// Take `id` out of the cluster, whether it votes or learns.
    RemoveNode {
        id: NodeId,
    },
    Status,
    /// Every shard group this node hosts, and who leads each (M11.6).
    ///
    /// Node-wide rather than per-group: it is the question an operator
    /// actually asks — "what is this node carrying, and which of it does it
    /// lead" — and answering it per group would be 154 round trips. The
    /// `group` on the request is ignored.
    ShardStatuses,
    /// This replica's locally-applied shard map, as bytes (M10.6).
    ///
    /// Deliberately **not** linearizable, and deliberately not a `Get`: it is
    /// what feeds the `ArcSwap` cache every node reads on every request, so a
    /// follower has to be able to answer it. It is stale by at most this
    /// replica's apply lag, which is what "publish the map to request
    /// handlers" in the design means. The linearizable read of the map is an
    /// ordinary `ClientOp::Get` through ReadIndex, and it is a different
    /// question with a different cost.
    ///
    /// Bytes rather than a decoded map because the `Cas` that replaces it
    /// compares bytes — a value decoded and re-encoded is not necessarily the
    /// value the log holds.
    LocalShardMap,
}

#[derive(Debug)]
pub enum AdminReply {
    /// The change committed, at this log index.
    Accepted {
        index: LogIndex,
    },
    Status(ClusterStatus),
    ShardStatuses(Vec<ShardStatus>),
    /// `None` before the meta group has bootstrapped a map.
    ShardMap(Option<Vec<u8>>),
    NotLeader {
        hint: Option<NodeId>,
    },
    /// The change is not one this config can make — removing the last voter,
    /// adding a node that is already a member. Refused before the log, so
    /// every replica is spared a conf entry they would all have to agree to
    /// ignore.
    Rejected {
        reason: String,
    },
}

/// What this node believes about the cluster right now. A view from one
/// replica, not a distributed snapshot: a follower's copy lags the leader's
/// by however far behind its log is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterStatus {
    pub id: NodeId,
    pub leader: Option<NodeId>,
    pub term: Term,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    pub endpoints: BTreeMap<NodeId, String>,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    /// The live log's bounds. `log_first_index` above 1 means the prefix below
    /// it has been replaced by a snapshot (M8).
    pub log_first_index: LogIndex,
    pub log_last_index: LogIndex,
}

/// One shard group as this node sees it (M11.6).
///
/// `leader` is what this replica believes, which on a follower is whatever it
/// last heard — the same one-replica view `ClusterStatus` is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardStatus {
    pub shard: ShardId,
    pub leader: Option<NodeId>,
    pub term: Term,
    pub replicas: Vec<NodeId>,
    /// Whether *this* node leads the shard. The gate's whole question.
    pub leading: bool,
    pub applied_index: LogIndex,
}

#[derive(Debug)]
pub struct AdminRequest {
    /// The group the op applies to. A driver hosting many groups cannot
    /// guess which one an operator meant.
    pub group: GroupId,
    pub op: AdminOp,
    pub reply: oneshot::Sender<AdminReply>,
}

/// Everything the driver listens on.
///
/// One struct rather than four positional parameters of near-identical type:
/// the compiler catches a swap between two of them, but nothing catches a
/// reader misreading the call site.
pub struct GroupChannels {
    pub inbox: mpsc::Receiver<Inbound>,
    /// Responses from peers, tagged with the group that asked (M11.2). One
    /// link per peer carries every group's traffic, and a Raft response
    /// carries no group of its own, so the link is what remembers.
    pub peer_replies: mpsc::Receiver<(GroupId, NodeId, Message)>,
    pub requests: mpsc::Receiver<ClientRequest>,
    pub admin: mpsc::Receiver<AdminRequest>,
    /// Groups founded while the driver is already running (M11.5).
    ///
    /// Opening a shard means opening two Bitcask instances and replaying
    /// their logs. Doing that on this loop would stall every group already
    /// ticking for the whole replay — and a node founding 154 shards would
    /// stall them all at once — so founding happens in
    /// [`crate::shards::ShardSupervisor`] and the finished group arrives here.
    pub new_groups: mpsc::Receiver<Group>,
}

#[derive(Debug)]
pub struct ClientRequest {
    /// The group holding this key's shard, resolved by the router before the
    /// driver sees it.
    pub group: GroupId,
    pub op: ClientOp,
    pub reply: oneshot::Sender<ClientReply>,
}

/// A client waiting on the entry it proposed.
struct Pending {
    /// The term the entry was proposed in. If the entry that finally commits
    /// at this index carries a different term, a later leader overwrote ours
    /// and it never committed — answering `Applied` there would report a lost
    /// write as a success, which is the worst bug available in this milestone.
    term: Term,
    reply: oneshot::Sender<ClientReply>,
    deadline: Instant,
}

/// An operator waiting on a membership change to commit. Same shape as
/// `Pending`, and the term check matters for the same reason: a conf entry
/// overwritten by a later leader before it committed did not happen.
struct PendingConf {
    term: Term,
    reply: oneshot::Sender<AdminReply>,
    deadline: Instant,
}

/// A client waiting on a linearizable read.
struct PendingRead {
    key: Vec<u8>,
    reply: oneshot::Sender<ClientReply>,
    deadline: Instant,
    /// Set when ReadIndex confirms the leadership quorum. The read may then be
    /// served as soon as the state machine has applied up to this index —
    /// reading earlier would serve a value the confirmed index does not cover.
    confirmed_at: Option<LogIndex>,
}

/// One Raft group this process hosts: its core, its state machine, and
/// everyone waiting on it.
///
/// Held by [`Driver`], which ticks and drains every one of these from a
/// single loop. A `Group` has no idea it is one of many — the same boundary
/// that lets `RaftNode` not know either.
pub struct Group {
    group: GroupId,
    node: RaftNode<BitcaskStorage>,
    /// The state machine: a second Bitcask instance, in its own directory.
    engine: Engine,
    /// Where each member of *this group* can be reached. Mirrors the address
    /// book in this group's state machine so reconciliation does not scan the
    /// engine every drain; seeded from `--peer` on a fresh store and owned by
    /// the log after that.
    endpoints: BTreeMap<NodeId, String>,
    /// Members with no known address, so the warning about them is said once
    /// rather than once per tick.
    unreachable: BTreeSet<NodeId>,
    pending: BTreeMap<LogIndex, Pending>,
    pending_conf: BTreeMap<LogIndex, PendingConf>,
    pending_reads: BTreeMap<u64, PendingRead>,
    next_token: u64,
    /// What the *driver* has applied to the `Engine`. Deliberately not
    /// `RaftNode`'s own count: the core considers an entry applied the moment
    /// it hands it over in `Ready::committed`, and this is the only place that
    /// knows it actually reached the state machine.
    ///
    /// Starts at the stored snapshot's index, not 0: the state on disk already
    /// reflects everything through it, and the replay below only re-applies
    /// the live tail.
    applied_index: LogIndex,
    /// How far this group's state machine is **visible to readers** (M11.5).
    ///
    /// Behind `applied_index`, because the keydir's readers are on whichever
    /// copy was last published and applying an entry only writes the other
    /// one. A confirmed read waits on this; see [`Visibility`] for why the
    /// difference is the milestone and not a detail.
    ///
    /// Shared with the read resolvers rather than owned outright, so the task
    /// that finally performs a read can check the same counter the driver
    /// dispatched it on.
    visible: Arc<Visibility>,
    /// The index of the last snapshot taken or installed. The next one is due
    /// once `applied_index` runs `snapshot_threshold` past it.
    snapshot_index: LogIndex,
    snapshot_threshold: u64,
    /// How long a client request may wait before the driver gives up on it. A
    /// partitioned leader still believes it leads, so a read never confirms
    /// and a write never commits — without a deadline both hang forever and
    /// the client cannot tell that from slowness.
    request_timeout: Duration,
    /// §1.10's lease reads, off unless asked for. When on, a leader that has
    /// recently had a quorum confirm it may serve reads with **no** round trip
    /// at all — correct only while clock drift stays inside the margin, which
    /// is the assumption ReadIndex does not make and the reason this is
    /// opt-in.
    lease_reads: bool,
    lease_duration: Duration,
    /// When the current lease lapses. Set every time a read quorum confirms,
    /// which is the only evidence of leadership this process gets.
    ///
    /// Tracked here rather than in `kv-raft` because the core has no clock —
    /// a lease is a wall-clock claim, and putting one in there would break the
    /// purity M4's determinism rests on.
    lease_until: Option<Instant>,
    /// Whether a caught-up learner is promoted to voter.
    ///
    /// False for the meta group, and that is the whole answer to the question
    /// M10 left open. A node admitted after bootstrap joins the meta group as
    /// a learner so that it replicates the shard map and can route; promoting
    /// it would grow the meta quorum with the data cluster, which is exactly
    /// what keeping placement in a small fixed group was for.
    promote_learners: bool,
}

pub struct Driver {
    /// Every group this process's shard set covers, ticked from one loop.
    groups: BTreeMap<GroupId, Group>,
    /// Resolves reads: the keydir lookup and the disk half both, off this
    /// loop. One pool per driver, shared by every group — a ring per shard
    /// would be 154 rings' worth of submission and completion queues for one
    /// node's reads.
    reads: ReadResolvers,
    /// One link per peer, carrying every group's traffic (M11.2).
    peers: BTreeMap<NodeId, Box<dyn PeerLink>>,
    /// Opens a link to a member this process did not start with (M9.2).
    /// Membership is a moving target once conf changes exist, and only the
    /// driver knows the current one.
    peer_factory: Box<dyn PeerFactory>,
    inbox: mpsc::Receiver<Inbound>,
    peer_replies: mpsc::Receiver<(GroupId, NodeId, Message)>,
    requests: mpsc::Receiver<ClientRequest>,
    admin: mpsc::Receiver<AdminRequest>,
    new_groups: mpsc::Receiver<Group>,
    tick: Duration,
    /// Ticks since the peer links were last reconciled against membership.
    ///
    /// Reconciliation walks every group, so doing it after every drain — which
    /// is what one group made free and 256 make expensive — cost more CPU than
    /// the entire rest of the loop: 256 group visits per inbound message, at
    /// thousands of messages a second. Measured: an idle 256-shard node sat at
    /// 120% of a core and thrashed elections on a tenth of its shards, because
    /// the loop could not keep up with its own ticks.
    ///
    /// So it runs when membership actually moves, and on a slow sweep for the
    /// case nothing reported: a link that failed to open earlier gets another
    /// chance without anything having to notice.
    ticks_since_reconcile: u32,
}

/// How many ticks may pass before the peer links are reconciled anyway.
///
/// A second at the default tick. Membership changes announce themselves, so
/// this only covers a link that could not be opened when it was first wanted.
const RECONCILE_EVERY_TICKS: u32 = 50;

impl Group {
    /// Opens one group over an already-open log and state machine.
    ///
    /// Cheap and synchronous: whatever disk work a group needs happened in
    /// `BitcaskStorage::open` and `Engine::open` before this was called,
    /// which is what lets the supervisor do it off the driver loop.
    pub fn new(
        config: &NodeConfig,
        group: GroupId,
        node: RaftNode<BitcaskStorage>,
        engine: Engine,
    ) -> Self {
        // The state on disk already reflects the stored snapshot; replay must
        // continue past it, not from the log's beginning.
        let snapshot_index = node
            .storage()
            .snapshot()
            .expect("freshly opened raft storage")
            .map(|s| s.last_included_index)
            .unwrap_or(0);
        // The flags are a bootstrap default; the state machine's address book
        // is the truth. A node added at runtime appears only in the book, and
        // a founder that has since moved appears in both — book wins.
        let mut addresses = config.peers.clone();
        // Our own address belongs in the book we hand to a joining node: it
        // has to be able to dial us back, and we are the one thing not in
        // `--peer`.
        addresses.insert(config.id, config.listen.to_string());
        addresses.extend(endpoints(&engine).expect("freshly opened state machine"));
        Self {
            group,
            node,
            engine,
            endpoints: addresses,
            unreachable: BTreeSet::new(),
            pending: BTreeMap::new(),
            pending_conf: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            next_token: 0,
            applied_index: snapshot_index,
            // The state on disk is already published — `Engine::open` does
            // that — so everything through the stored snapshot is readable.
            visible: {
                let visible = Arc::new(Visibility::default());
                visible.publish(snapshot_index);
                visible
            },
            snapshot_index,
            snapshot_threshold: config.snapshot_threshold,
            // Comfortably longer than an election, so an ordinary failover is
            // ridden out rather than reported as a failure.
            request_timeout: config.tick * (config.election_timeout as u32) * 6,
            lease_reads: config.lease_reads,
            lease_duration: config.lease_duration(),
            lease_until: None,
            // See the field's comment: the meta group must not grow its
            // quorum with the data cluster.
            promote_learners: group != group::META,
        }
    }

    pub fn id(&self) -> GroupId {
        self.group
    }

    /// The handles a reader needs to serve this group's `Get`s from another
    /// task: a factory to mint its own view from, and how far that view is
    /// published.
    pub(crate) fn reads(&self) -> GroupReads {
        GroupReads { factory: self.engine.read_view_factory(), visible: Arc::clone(&self.visible) }
    }

    /// Makes every write the apply loop has absorbed visible to readers, and
    /// records how far that reaches.
    ///
    /// One publish per committed batch, which is what §1.15 asks for and what
    /// Raft makes free: `commit_index` already advances in batches, so the
    /// whole batch is absorbed and swapped in once.
    ///
    /// A publish can be **refused** — `Engine::publish` will not stall this
    /// loop waiting for a reader to leave the copy it would overwrite. Then
    /// the writes stay applied and invisible and `visible` does not move,
    /// which is precisely the state a confirmed read has to wait out. The
    /// next drain tries again, and a drain happens every tick.
    fn publish(&mut self) {
        if self.engine.has_unpublished() && !self.engine.publish() {
            // Not an error: the next drain retries. Worth saying at debug
            // because a reader parked forever would stall every read on this
            // group, and this is the only place that would show it.
            tracing::debug!(
                group = self.group,
                applied = self.applied_index,
                visible = self.visible.index(),
                "keydir publish deferred by an in-flight reader"
            );
            return;
        }
        // Either the swap happened or there was nothing to swap. Both mean
        // every applied write is now readable.
        self.visible.publish(self.applied_index);
    }
}

impl Driver {
    /// `groups` are hosted from the first tick. Anything founded later
    /// arrives through `channels.new_groups` — the distinction matters because
    /// `select!` picks among ready arms at random, so a group handed over
    /// through the channel is not reliably present for the *next* request.
    pub fn new(
        config: &NodeConfig,
        groups: Vec<Group>,
        peers: BTreeMap<NodeId, Box<dyn PeerLink>>,
        peer_factory: Box<dyn PeerFactory>,
        channels: GroupChannels,
    ) -> Self {
        let reads = ReadResolvers::start();
        let groups: BTreeMap<GroupId, Group> = groups.into_iter().map(|g| (g.id(), g)).collect();
        for (&id, group) in &groups {
            reads.host(id, group.reads());
        }
        Self {
            groups,
            reads,
            peers,
            peer_factory,
            inbox: channels.inbox,
            peer_replies: channels.peer_replies,
            requests: channels.requests,
            admin: channels.admin,
            new_groups: channels.new_groups,
            tick: config.tick,
            // Zero, so the first drain reconciles: the links `main` opened
            // from `--peer` still have to be checked against what the log
            // says the membership is.
            ticks_since_reconcile: RECONCILE_EVERY_TICKS,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.tick);
        // A stalled drain must not make the loop catch up on missed ticks in a
        // burst: that would fire several elections' worth of timeout in one
        // pass and depose a perfectly healthy leader.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // Which group this pass has work for, and — for an inbound RPC —
            // the channel its reply travels back on. `None` means every
            // group: a tick advances all of them, so all of them drain.
            let mut touched: Option<GroupId> = None;
            let mut answer: Option<(NodeId, oneshot::Sender<Message>)> = None;
            let mut ticked = false;

            tokio::select! {
                _ = ticker.tick() => {
                    ticked = true;
                    // One interval for every group. The alternative — a timer
                    // per group — is what this milestone exists to avoid.
                    for group in self.groups.values_mut() {
                        group.node.tick();
                    }
                }
                Some(Inbound { group, from, msg, reply }) = self.inbox.recv() => {
                    // The server only routes to groups it was told this
                    // process hosts, but the table it routes by and this map
                    // move independently, so a message for a group that has
                    // since gone is ordinary. Dropping the `reply` answers the
                    // RPC as unavailable, which is what the peer retries.
                    if let Some(hosted) = self.groups.get_mut(&group) {
                        hosted.node.step(from, msg);
                        touched = Some(group);
                        answer = Some((from, reply));
                    }
                }
                Some((group, peer, msg)) = self.peer_replies.recv() => {
                    // A link carries every group's traffic, so a reply for a
                    // group this driver does not host is not ours to step.
                    // Dropping it is safe for the reason every shed Raft
                    // message is: the sender retries on its next heartbeat.
                    if let Some(hosted) = self.groups.get_mut(&group) {
                        hosted.node.step(peer, msg);
                        touched = Some(group);
                    }
                }
                // The one arm that handles its own close. `select!` disables a
                // `Some(..) =` arm whose channel has closed, and the `else`
                // branch only fires when *every* arm is disabled — which the
                // ticker never is. Without this the loop would outlive the
                // process's last client handle and tick forever, holding every
                // Bitcask directory open.
                request = self.requests.recv() => {
                    match request {
                        Some(request) => touched = self.handle_request(request),
                        None => return Ok(()),
                    }
                }
                // Not a shutdown signal: an admin channel with no senders
                // left is an ordinary state (nothing is administering this
                // node), and the arm simply stays disabled.
                Some(request) = self.admin.recv() => {
                    touched = self.handle_admin(request);
                }
                // A shard founded while we were running. Its log and state
                // machine are already open — that work happened in the
                // supervisor, off this loop.
                Some(group) = self.new_groups.recv() => {
                    let id = group.id();
                    // Readable before it is drained: a read cannot arrive for
                    // a group the router does not yet point at, but
                    // registering after the first drain would leave a window
                    // where one could.
                    self.reads.host(id, group.reads());
                    if self.groups.insert(id, group).is_some() {
                        // Two `Group`s for one id would be two `RaftNode`s
                        // over one log. The supervisor founds each shard once,
                        // so this is a bug rather than a race.
                        tracing::error!(group = id, "a group was founded twice; the first is gone");
                    }
                    tracing::info!(group = id, "hosting a new group");
                    touched = Some(id);
                }
            }

            if ticked {
                self.ticks_since_reconcile += 1;
            }
            self.drain(touched, answer)?;
        }
    }

    /// Runs one drain, on the group this pass touched or on every group when a
    /// tick moved all of them.
    fn drain(
        &mut self,
        touched: Option<GroupId>,
        answer: Option<(NodeId, oneshot::Sender<Message>)>,
    ) -> anyhow::Result<()> {
        let mut membership_moved = false;
        match touched {
            Some(id) => {
                if let Some(group) = self.groups.get_mut(&id) {
                    membership_moved = group.drain(answer, &self.peers, &self.reads)?;
                }
            }
            None => {
                for group in self.groups.values_mut() {
                    membership_moved |= group.drain(None, &self.peers, &self.reads)?;
                }
            }
        }
        // Peer links are shared across groups, so reconciliation is the
        // driver's rather than any one group's: the union of every group's
        // membership is who this node has to be able to reach.
        //
        // Only when something moved, or on the slow sweep. See
        // `ticks_since_reconcile` for what doing it every drain cost.
        if membership_moved || self.ticks_since_reconcile >= RECONCILE_EVERY_TICKS {
            self.ticks_since_reconcile = 0;
            self.reconcile_peers();
        }
        Ok(())
    }

    /// Every shard group this node hosts, in shard order.
    ///
    /// The meta group is left out: it is not a shard, and an operator asking
    /// what data this node carries is not asking about placement metadata.
    fn shard_statuses(&self) -> Vec<ShardStatus> {
        self.groups
            .values()
            .filter_map(|group| {
                let shard = group::shard_of(group.group)?;
                let config = group.node.cluster_config();
                Some(ShardStatus {
                    shard,
                    leader: group.node.leader_id(),
                    term: group.node.current_term(),
                    replicas: config.voters.iter().copied().collect(),
                    leading: group.node.role() == Role::Leader,
                    applied_index: group.applied_index,
                })
            })
            .collect()
    }

    /// Brings the peer links in line with the membership every group now says.
    ///
    /// Runs after every drain rather than only after a conf entry, because
    /// membership also moves when a *snapshot* is installed — a node that was
    /// behind learns the whole config in one step, with no conf entry to
    /// react to.
    ///
    /// The union across groups, because one link carries them all: a node that
    /// is a member of any group this process hosts has to be reachable, and
    /// dropping the link the moment one shard stops needing it would cut the
    /// other 153.
    fn reconcile_peers(&mut self) {
        let mut members: BTreeSet<NodeId> = BTreeSet::new();
        let mut addresses: BTreeMap<NodeId, String> = BTreeMap::new();
        for group in self.groups.values() {
            let me = group.node.id();
            let config = group.node.cluster_config();
            members.extend(
                config.voters.iter().chain(config.learners.iter()).copied().filter(|id| *id != me),
            );
            for (id, address) in &group.endpoints {
                addresses.entry(*id).or_insert_with(|| address.clone());
            }
        }

        // Gone from every group: drop the link, which drops its queue and its
        // connection. The removed node is not told — it cannot be, since it is
        // no longer a replication target once the entry is *appended*. It
        // campaigns into a void until an operator stops it, which is what the
        // stranger gate in `RaftNode::step` makes harmless.
        self.peers.retain(|id, _| members.contains(id));

        for id in members {
            if self.peers.contains_key(&id) {
                continue;
            }
            let Some(address) = addresses.get(&id) else {
                // A member with no known address: a conf entry whose context
                // was lost or never set. Said once per group rather than once
                // per tick — the drain runs every tick, and a warning per tick
                // is a log nobody reads.
                for group in self.groups.values_mut() {
                    if group.node.cluster_config().contains(id) && group.unreachable.insert(id) {
                        tracing::warn!(
                            group = group.group,
                            peer = id,
                            "member has no known endpoint, not dialling"
                        );
                    }
                }
                continue;
            };
            for group in self.groups.values_mut() {
                group.unreachable.remove(&id);
            }
            tracing::info!(peer = id, %address, "dialling a new member");
            self.peers.insert(id, self.peer_factory.connect(id, address));
        }
    }

    /// Routes one client request to the group holding its shard.
    ///
    /// Returns the group to drain, or `None` when the request was answered
    /// here — a `NotHosted` moves no group's state.
    fn handle_request(&mut self, request: ClientRequest) -> Option<GroupId> {
        let ClientRequest { group, op, reply } = request;
        let Some(hosted) = self.groups.get_mut(&group) else {
            // The service resolved this key to a shard we do not replicate —
            // its map is a version behind ours, or ahead of it. Either way the
            // client re-routes.
            let _ = reply
                .send(ClientReply::NotHosted { shard: group::shard_of(group).unwrap_or_default() });
            return None;
        };
        hosted.handle_request(ClientRequest { group, op, reply }, &self.reads);
        Some(group)
    }

    fn handle_admin(&mut self, request: AdminRequest) -> Option<GroupId> {
        let AdminRequest { group, op, reply } = request;
        if matches!(op, AdminOp::ShardStatuses) {
            let _ = reply.send(AdminReply::ShardStatuses(self.shard_statuses()));
            return None;
        }
        let Some(hosted) = self.groups.get_mut(&group) else {
            let _ = reply.send(AdminReply::Rejected {
                reason: format!("this node does not host group {group}"),
            });
            return None;
        };
        hosted.handle_admin(AdminRequest { group, op, reply });
        Some(group)
    }
}

impl Group {
    fn handle_request(&mut self, request: ClientRequest, reads: &ReadResolvers) {
        let ClientRequest { op, reply, .. } = request;
        let command = match op {
            // Reads go through ReadIndex (§1.10), never straight to the
            // engine. M6 read local state, which let a deposed leader serve a
            // value that had already been overwritten — see
            // `tests::linearizability`. Only the leader can serve a read, and
            // only after a quorum confirms it still leads.
            ClientOp::Get { key } => {
                self.begin_read(key, reply, reads);
                return;
            }
            ClientOp::Mutate { ctx, op } => Command::new(ctx, op),
        };

        match self.node.propose(command.encode()) {
            Ok(index) => {
                self.pending.insert(
                    index,
                    Pending {
                        term: self.node.current_term(),
                        reply,
                        deadline: Instant::now() + self.request_timeout,
                    },
                );
            }
            Err(ProposeError::NotLeader) => {
                let _ = reply.send(ClientReply::NotLeader { hint: self.node.leader_id() });
            }
        }
    }

    fn handle_admin(&mut self, request: AdminRequest) {
        let AdminRequest { op, reply, .. } = request;
        let change = match op {
            AdminOp::Status => {
                let _ = reply.send(AdminReply::Status(self.status()));
                return;
            }
            // Answered by the driver, across every group, before the request
            // ever reaches one.
            AdminOp::ShardStatuses => {
                let _ = reply.send(AdminReply::Rejected {
                    reason: "shard statuses are answered node-wide, not per group".into(),
                });
                return;
            }
            AdminOp::LocalShardMap => {
                let stored = crate::shard_map::encoded(&self.engine).unwrap_or_else(|e| {
                    tracing::error!(error = %e, "reading the local shard map");
                    None
                });
                let _ = reply.send(AdminReply::ShardMap(stored));
                return;
            }
            // Addresses travel in the conf entry's `context`, which the core
            // preserves and never reads. That is what lets every replica —
            // including ones that join later and see this entry only as
            // history — learn where a node lives from the log rather than from
            // an operator remembering to update their flags.
            //
            // The context carries the *whole* book, not just the newcomer's
            // address: the founding members never appear in a conf entry, so
            // an entry naming only the newcomer would admit a node that can
            // receive from the leader and dial nobody — a node that can never
            // campaign once it is promoted.
            AdminOp::AddNode { id, address } => {
                let mut book = self.endpoints.clone();
                book.insert(id, address);
                ConfChange { op: ConfOp::AddLearner, node: id, context: encode_book(&book) }
            }
            // Which removal it is depends on what the node currently is, and
            // the core refuses the wrong one rather than guessing. A node
            // that is neither is refused outright: silently succeeding would
            // tell an operator a node had been removed that never existed.
            AdminOp::RemoveNode { id } => {
                let op = if self.node.cluster_config().is_voter(id) {
                    ConfOp::RemoveVoter
                } else {
                    ConfOp::RemoveLearner
                };
                ConfChange { op, node: id, context: Vec::new() }
            }
        };

        match self.node.propose_conf_change(change) {
            Ok(index) => {
                self.pending_conf.insert(
                    index,
                    PendingConf {
                        term: self.node.current_term(),
                        reply,
                        deadline: Instant::now() + self.request_timeout,
                    },
                );
            }
            Err(ConfProposeError::NotLeader) => {
                let _ = reply.send(AdminReply::NotLeader { hint: self.node.leader_id() });
            }
            // One at a time is the safety rule single-server changes rest on
            // (§1.7): with two in flight, an old majority and a new majority
            // need not overlap. The operator retries once the first commits.
            Err(e @ ConfProposeError::ConfInFlight) | Err(e @ ConfProposeError::Invalid(_)) => {
                let _ = reply.send(AdminReply::Rejected { reason: e.to_string() });
            }
        }
    }

    fn status(&self) -> ClusterStatus {
        let cluster = self.node.cluster_config();
        ClusterStatus {
            id: self.node.id(),
            leader: self.node.leader_id(),
            term: self.node.current_term(),
            voters: cluster.voters.iter().copied().collect(),
            learners: cluster.learners.iter().copied().collect(),
            endpoints: self.endpoints.clone(),
            commit_index: self.node.commit_index(),
            applied_index: self.applied_index,
            // Degrade rather than panic: a status query is what an operator
            // reaches for when something is already wrong, and taking the
            // driver down with it would remove the tool mid-diagnosis.
            log_first_index: self.node.storage().first_index().unwrap_or(0),
            log_last_index: self.node.storage().last_index().unwrap_or(0),
        }
    }

    /// Promotes a learner that has caught up (M9.2).
    ///
    /// "Caught up" is `match_index >= commit_index`: the learner holds every
    /// entry the cluster has agreed on, so making it a voter cannot stall a
    /// quorum — which is the whole reason it joined as a learner. Only the
    /// leader does this, one node at a time, and a `ConfInFlight` refusal is
    /// simply retried on the next drain.
    fn maybe_promote(&mut self) {
        if !self.promote_learners || self.node.role() != Role::Leader {
            return;
        }
        let commit = self.node.commit_index();
        let ready: Option<NodeId> = self
            .node
            .cluster_config()
            .learners
            .iter()
            .copied()
            .find(|id| self.node.match_index_of(*id).is_some_and(|m| m >= commit));
        let Some(id) = ready else {
            return;
        };
        // No context: a promotion changes a node's standing, not its address,
        // and an empty context leaves the address book entry alone.
        let change = ConfChange { op: ConfOp::Promote, node: id, context: Vec::new() };
        match self.node.propose_conf_change(change) {
            Ok(index) => tracing::info!(peer = id, index, "promoting a caught-up learner"),
            // Another change is still in flight, or someone promoted it
            // first. Both resolve themselves; the next drain looks again.
            Err(e) => tracing::debug!(peer = id, error = %e, "promotion deferred"),
        }
    }

    fn begin_read(
        &mut self,
        key: Vec<u8>,
        reply: oneshot::Sender<ClientReply>,
        reads: &ReadResolvers,
    ) {
        // Lease read: a quorum confirmed our leadership recently enough that
        // no other node can have become leader since — *if* the clocks agree.
        // Zero round trips, and the whole correctness argument rests on that
        // proviso, which is why it is off by default.
        //
        // `visible`, not `applied`: a lease read skips the round trip, not the
        // visibility rule. A leader whose keydir publish is still pending
        // falls through to the ReadIndex path below and waits there, which
        // costs a round trip and is the only honest alternative.
        if self.lease_reads
            && self.node.role() == Role::Leader
            && self.lease_until.is_some_and(|until| Instant::now() < until)
            && self.visible.index() >= self.node.commit_index()
        {
            self.answer_read(key, self.node.commit_index(), reply, reads);
            return;
        }

        self.next_token += 1;
        let token = self.next_token;
        match self.node.read_index(token) {
            Ok(()) => {
                self.pending_reads.insert(
                    token,
                    PendingRead {
                        key,
                        reply,
                        deadline: Instant::now() + self.request_timeout,
                        confirmed_at: None,
                    },
                );
            }
            // Either we do not lead, or we lead but have not yet committed an
            // entry in our own term. Both mean "ask someone else", and the
            // client's retry loop handles it.
            Err(ReadIndexError::NotLeader | ReadIndexError::NoQuorumInTerm) => {
                let _ = reply.send(ClientReply::NotLeader { hint: self.node.leader_id() });
            }
        }
    }

    /// Answers every read whose confirmed index is **visible to readers**.
    ///
    /// `visible`, not `applied`, and that is the whole of M11.5's correctness
    /// story. `applied_index` moves when an entry is absorbed into the
    /// keydir's write copy; readers are on the other copy until `publish()`
    /// swaps it in. A read confirmed at index N and served on
    /// `applied_index >= N` can therefore miss a write that has already been
    /// acknowledged to a client — a stale read that violates linearizability,
    /// appears only under concurrency, and passes every single-threaded test.
    /// `tests::wait_free_reads` is the regression test, and it fails against
    /// the `applied_index` form.
    fn serve_ready_reads(&mut self, reads: &ReadResolvers) {
        let visible = self.visible.index();
        let ready: Vec<u64> = self
            .pending_reads
            .iter()
            .filter(|(_, r)| r.confirmed_at.is_some_and(|i| i <= visible))
            .map(|(token, _)| *token)
            .collect();

        for token in ready {
            let read = self.pending_reads.remove(&token).expect("just listed");
            let at = read.confirmed_at.expect("just filtered on it");
            self.answer_read(read.key, at, read.reply, reads);
        }
    }

    /// Hands a confirmed read off, and does no part of it here.
    ///
    /// Until M11.5 the keydir lookup happened on this loop: `Engine::put`
    /// mutates the keydir, so a reader could not hold `&Engine` across a
    /// write and the only safe place to look a key up was the engine's owner.
    /// With the keydir behind `left-right` a reader can, so both halves of a
    /// read — the lookup and the disk — now happen elsewhere and the driver's
    /// share of a read is one channel send.
    ///
    /// `at` travels with the read so the resolver can check for itself that
    /// what it is about to read covers the index the quorum confirmed.
    fn answer_read(
        &self,
        key: Vec<u8>,
        at: LogIndex,
        reply: oneshot::Sender<ClientReply>,
        reads: &ReadResolvers,
    ) {
        reads.submit(self.group, key, at, reply);
    }

    /// Gives up on requests that have waited too long.
    ///
    /// A partitioned leader still believes it leads: `propose` is accepted and
    /// `read_index` starts a round, but neither can ever reach a quorum. The
    /// node has no way to learn this — Raft leaders do not step down on their
    /// own — so without a deadline the client waits forever.
    fn expire_stale_requests(&mut self) {
        let now = Instant::now();
        let hint = self.node.leader_id();

        let expired: Vec<LogIndex> =
            self.pending.iter().filter(|(_, p)| p.deadline <= now).map(|(i, _)| *i).collect();
        for index in expired {
            let pending = self.pending.remove(&index).expect("just listed");
            let _ = pending.reply.send(ClientReply::NotLeader { hint });
        }

        let expired: Vec<u64> =
            self.pending_reads.iter().filter(|(_, r)| r.deadline <= now).map(|(t, _)| *t).collect();
        for token in expired {
            let read = self.pending_reads.remove(&token).expect("just listed");
            let _ = read.reply.send(ClientReply::NotLeader { hint });
        }

        let expired: Vec<LogIndex> =
            self.pending_conf.iter().filter(|(_, c)| c.deadline <= now).map(|(i, _)| *i).collect();
        for index in expired {
            let conf = self.pending_conf.remove(&index).expect("just listed");
            let _ = conf.reply.send(AdminReply::NotLeader { hint });
        }
    }

    /// Applies one committed command to the state machine, deduplicating
    /// retries through the session table (§1.8).
    ///
    /// Every replica runs this over the same log in the same order, so the
    /// session table is replicated like everything else — which is the point.
    /// A table kept beside the state machine would die with the leader whose
    /// death made it necessary.
    fn apply(&mut self, index: LogIndex, command: Command) -> anyhow::Result<CommandResponse> {
        // A retry of something already applied returns the answer that was
        // given the first time. Re-evaluating would be wrong, not merely
        // wasteful: a replayed `Cas` sees the value it already swapped in and
        // answers `swapped: false`.
        if let Some(ctx) = command.ctx
            && let Some(cached) = session::cached(&mut self.engine, &ctx)?
        {
            return Ok(cached);
        }

        let response = match command.op {
            Mutation::Put { key, value } => {
                self.engine.put(&key, &value)?;
                CommandResponse::Applied
            }
            Mutation::Delete { key } => {
                self.engine.delete(&key)?;
                CommandResponse::Applied
            }
            Mutation::Cas { key, expected, new_value } => {
                let current = self.engine.get(&key)?;
                if current == expected {
                    self.engine.put(&key, &new_value)?;
                    CommandResponse::Swapped(true)
                } else {
                    CommandResponse::Swapped(false)
                }
            }
        };

        if let Some(ctx) = command.ctx {
            session::record(&mut self.engine, &ctx, &response, index)?;
        }
        Ok(response)
    }

    /// Scans live state into a portable image once the log past the last
    /// snapshot is long enough, and drops the prefix it replaces.
    fn maybe_snapshot(&mut self) -> anyhow::Result<()> {
        if self.applied_index == self.snapshot_index
            || self.applied_index < self.snapshot_index.saturating_add(self.snapshot_threshold)
        {
            return Ok(());
        }
        // An applied index always has a term: a live entry's, or the snapshot
        // boundary's when the whole tail was restored rather than applied.
        let term = self
            .node
            .storage()
            .term(self.applied_index)?
            .expect("an applied index always has a term");
        // The state machine has to be on the disk **before** the log prefix
        // that could rebuild it is dropped.
        //
        // `Group::new` starts replay at the stored snapshot's index, on the
        // assumption stated there that the state on disk already reflects it.
        // Under the state machine's own fsync policy that assumption is not
        // free: `--state-fsync group-commit` leaves applied writes in the page
        // cache, and `take_snapshot` below truncates exactly the entries that
        // would have replayed them. One fsync per snapshot is what buys back
        // the two per applied write, and it is the only one the state machine
        // needs.
        self.engine.sync()?;
        let pairs = self.engine.scan()?;
        let data = crate::snapshot::encode(self.applied_index, term, &pairs);
        let snap = Snapshot {
            last_included_index: self.applied_index,
            last_included_term: term,
            data,
            config: self.node.cluster_config().clone(),
        };
        // Refusals are stale bookkeeping, not errors: `take_snapshot` says no
        // when the prefix is already past the index, and the threshold trips
        // again next drain if state really advanced.
        if self.node.take_snapshot(snap) {
            self.node.storage().sync()?;
            self.snapshot_index = self.applied_index;
        }
        Ok(())
    }

    /// Replaces local state with a received snapshot image wholesale. Deletes
    /// first, then writes: keys the image drops stay dropped, and the session
    /// entries ride along as ordinary pairs — no separate session restore, so
    /// the two cannot disagree about what was deduplicated.
    fn restore_snapshot(&mut self, snap: &Snapshot) -> anyhow::Result<()> {
        let image = crate::snapshot::decode(&snap.data).map_err(|e| {
            anyhow::anyhow!("snapshot image at index {} undecodable: {e}", snap.last_included_index)
        })?;
        if image.last_included_index != snap.last_included_index
            || image.last_included_term != snap.last_included_term
        {
            anyhow::bail!(
                "snapshot image boundary ({}, {}) disagrees with its log boundary ({}, {})",
                image.last_included_index,
                image.last_included_term,
                snap.last_included_index,
                snap.last_included_term,
            );
        }
        for (key, _) in self.engine.scan()? {
            self.engine.delete(&key)?;
        }
        for (key, value) in &image.pairs {
            self.engine.put(key, value)?;
        }
        self.engine.sync()?;
        // The image replaced the whole state machine, address book included.
        // Rebuilding from it rather than merging is the point: the snapshot
        // is the authority on membership at its boundary.
        self.endpoints = endpoints(&self.engine)?;
        self.applied_index = self.applied_index.max(snap.last_included_index);
        self.snapshot_index = self.snapshot_index.max(snap.last_included_index);
        Ok(())
    }

    /// Executes one `Ready`. See the module header for why the order is what
    /// it is.
    /// Returns whether this group's membership may have moved, which is the
    /// driver's cue to reconcile the peer links it shares across groups.
    fn drain(
        &mut self,
        answer: Option<(NodeId, oneshot::Sender<Message>)>,
        peers: &BTreeMap<NodeId, Box<dyn PeerLink>>,
        reads: &ReadResolvers,
    ) -> anyhow::Result<bool> {
        let mut membership_moved = false;
        let mut ready = self.node.ready();

        // 1. Disk. `RaftNode` already wrote through to storage inside
        //    `step`/`propose`; this is what makes it durable, and it must
        //    happen before anything leaves this process.
        //
        //    Unconditional since group commit became the log's default. It
        //    used to be guarded on this `Ready` having entries or a hard
        //    state, which was a free optimisation under `EveryWrite` — every
        //    write had already synced on the way in, so the guard could not
        //    skip anything that mattered. Under `GroupCommit` the guard would
        //    be a correctness question instead: `truncate_suffix`,
        //    `truncate_prefix` and `save_snapshot` all dirty the log without
        //    putting anything in `ready.entries`, and deciding which of them
        //    can be left unsynced is exactly the kind of case analysis that
        //    breaks silently. `Engine::sync` is a no-op when nothing is
        //    pending, so asking every time costs a branch.
        self.node.storage().sync()?;

        // 2. The inbound RPC's reply, which is a send like any other and so
        //    comes after the sync. Inbound is only ever RequestVote or
        //    AppendEntries, and each produces exactly one response addressed
        //    back to its sender, so taking the first such message is exact
        //    rather than a heuristic.
        if let Some((from, channel)) = answer
            && let Some(i) = ready.messages.iter().position(|(to, _)| *to == from)
        {
            let (_, msg) = ready.messages.remove(i);
            let _ = channel.send(msg);
        }

        // 3. Everything else, shed on a full queue (M5's decision: Raft
        //    assumes a lossy network, and blocking the driver on one slow
        //    peer is what is not safe).
        for (to, msg) in ready.messages {
            if let Some(peer) = peers.get(&to) {
                let _ = peer.try_send(self.group, msg);
            }
        }

        // 4. Apply.
        //
        //    `RaftNode::new` starts `last_applied` at 0 and takes
        //    `commit_index` from `HardState`, so a restart replays the live
        //    tail over state that already reflects the stored snapshot (which
        //    is where `applied_index` starts). Replay stays safe for the old
        //    reason — `Put`/`Delete` are idempotent and `Cas` is deduplicated
        //    through the session table, which the snapshot carries along.
        for entry in ready.committed {
            let index = entry.index;
            // Membership entries (M9) carry no state machine command: the
            // core changed the quorum when the entry was appended. What is
            // left is the application's half — where the node can be reached
            // — which goes in the state machine so a snapshot carries it.
            if is_conf_change(&entry.command) {
                membership_moved = true;
                if let Some(change) = decode_conf(&entry.command) {
                    match apply_conf(&mut self.engine, &change)? {
                        ConfEffect::Learned(book) => self.endpoints.extend(book),
                        ConfEffect::Forgot(id) => {
                            self.endpoints.remove(&id);
                        }
                    }
                }
                self.applied_index = self.applied_index.max(index);
                if let Some(pending) = self.pending_conf.remove(&index) {
                    let reply = if pending.term == entry.term {
                        AdminReply::Accepted { index }
                    } else {
                        // A later leader overwrote our entry before it
                        // committed: the membership change did not happen.
                        AdminReply::NotLeader { hint: self.node.leader_id() }
                    };
                    let _ = pending.reply.send(reply);
                }
                continue;
            }
            let response = match Command::decode(&entry.command) {
                // The leader's no-op: committed and applied like any entry,
                // and it means nothing to the state machine.
                Ok(None) => None,
                Ok(Some(command)) => Some(self.apply(index, command)?),
                Err(e) => {
                    // A committed entry we cannot decode means the log and
                    // this binary disagree about the state machine. Applying
                    // past it would diverge the replicas silently.
                    anyhow::bail!("undecodable committed entry at index {}: {e}", entry.index);
                }
            };

            // Applied, for real, to the state machine. A confirmed read may
            // not be served before this reaches its index.
            self.applied_index = self.applied_index.max(index);

            if let Some(pending) = self.pending.remove(&entry.index) {
                let reply = match response {
                    // Our entry was overwritten by a later leader before it
                    // committed. The write did not happen.
                    _ if pending.term != entry.term => {
                        ClientReply::NotLeader { hint: self.node.leader_id() }
                    }
                    Some(response) => response.into(),
                    None => ClientReply::Applied,
                };
                let _ = pending.reply.send(reply);
            }
        }

        // 4b. Snapshots taken: the log past the last one is long enough to be
        //    worth replacing with a state image. The image is scanned from
        //    live state, so there is nothing to restore — the prefix is
        //    simply dropped, durably (disk before anything that could reveal
        //    the new base to a follower).
        self.maybe_snapshot()?;

        // 4c. Snapshots received: this node's log was too far behind and the
        //    leader shipped state instead. The image *replaces* local state —
        //    everything through its boundary in one go — and `applied_index`
        //    jumps to it, which is also what unblocks the reads below.
        if let Some(snap) = ready.snapshot {
            // A node that was behind learns the whole config in one step,
            // with no conf entry to react to.
            membership_moved = true;
            self.restore_snapshot(&snap)?;
        }

        // 4d. One publish for the whole batch, which makes everything the
        //    steps above wrote readable and moves `visible` up to it. Before
        //    the reads below, and after the snapshot handling: a restored
        //    snapshot replaces the state machine wholesale, and jumping
        //    `applied_index` to its boundary without publishing it would
        //    leave every reader on a copy that predates the whole image.
        self.publish();

        // 5. Reads whose leadership quorum just came back. A confirmation is
        //    also the only evidence this process gets that it still leads, so
        //    it is what renews the lease.
        if !ready.read_states.is_empty() {
            self.lease_until = Some(Instant::now() + self.lease_duration);
        }
        for state in ready.read_states {
            if let Some(read) = self.pending_reads.get_mut(&state.token) {
                read.confirmed_at = Some(state.index);
            }
        }
        self.serve_ready_reads(reads);

        // 5b. Membership: promote a learner that has caught up. After apply,
        //    because it reads state the apply above may have just moved. The
        //    peer *links* are reconciled by the driver instead, since one link
        //    carries every group's traffic.
        self.maybe_promote();

        // 6. If we are no longer the leader, nothing still pending will ever
        //    commit or confirm under us. Answering now beats making the client
        //    wait out its deadline.
        if self.node.role() != Role::Leader
            && !(self.pending.is_empty()
                && self.pending_reads.is_empty()
                && self.pending_conf.is_empty())
        {
            let hint = self.node.leader_id();
            for (_, pending) in std::mem::take(&mut self.pending) {
                let _ = pending.reply.send(ClientReply::NotLeader { hint });
            }
            for (_, read) in std::mem::take(&mut self.pending_reads) {
                let _ = read.reply.send(ClientReply::NotLeader { hint });
            }
            for (_, conf) in std::mem::take(&mut self.pending_conf) {
                let _ = conf.reply.send(AdminReply::NotLeader { hint });
            }
        }

        // 7. And whatever has simply waited too long — a partitioned leader
        //    never learns it was deposed, so nothing above will ever fire.
        self.expire_stale_requests();

        Ok(membership_moved)
    }
}
