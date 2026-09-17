//! The driver loop (M6): the one task that owns the Raft node and the state
//! machine, and the only thing that touches either.
//!
//! Everything else in the process — the two gRPC services, the per-peer
//! clients — reaches it over a bounded channel and waits on a `oneshot`. That
//! is what keeps `RaftNode` single-threaded and synchronous, which is the
//! property M4's determinism rests on, while the shell around it is fully
//! async.
//!
//! Every drain runs in one order, and the order is a correctness requirement
//! rather than an optimisation (§1.5): **sync the log, then send, then
//! apply.** A vote that reaches the wire before it reaches the disk lets a
//! crash produce a second vote in the same term, and election safety is gone.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use kv_raft::{LogIndex, Message, NodeId, ProposeError, RaftNode, ReadIndexError, Role, Term};
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, Mutation};
use crate::config::NodeConfig;
use crate::session::{self, CommandResponse, RequestCtx};
use crate::storage::BitcaskStorage;
use crate::transport::PeerLink;
use crate::transport::server::Inbound;

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
}

impl From<CommandResponse> for ClientReply {
    fn from(response: CommandResponse) -> Self {
        match response {
            CommandResponse::Applied => ClientReply::Applied,
            CommandResponse::Swapped(swapped) => ClientReply::Swapped(swapped),
        }
    }
}

#[derive(Debug)]
pub struct ClientRequest {
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

pub struct Driver {
    node: RaftNode<BitcaskStorage>,
    /// The state machine: a second Bitcask instance, in its own directory.
    engine: Engine,
    peers: BTreeMap<NodeId, Box<dyn PeerLink>>,
    inbox: mpsc::Receiver<Inbound>,
    peer_replies: mpsc::Receiver<(NodeId, Message)>,
    requests: mpsc::Receiver<ClientRequest>,
    pending: BTreeMap<LogIndex, Pending>,
    pending_reads: BTreeMap<u64, PendingRead>,
    next_token: u64,
    /// What the *driver* has applied to the `Engine`. Deliberately not
    /// `RaftNode`'s own count: the core considers an entry applied the moment
    /// it hands it over in `Ready::committed`, and this is the only place that
    /// knows it actually reached the state machine.
    applied_index: LogIndex,
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
    tick: Duration,
}

impl Driver {
    pub fn new(
        config: &NodeConfig,
        node: RaftNode<BitcaskStorage>,
        engine: Engine,
        peers: BTreeMap<NodeId, Box<dyn PeerLink>>,
        inbox: mpsc::Receiver<Inbound>,
        peer_replies: mpsc::Receiver<(NodeId, Message)>,
        requests: mpsc::Receiver<ClientRequest>,
    ) -> Self {
        Self {
            node,
            engine,
            peers,
            inbox,
            peer_replies,
            requests,
            pending: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            next_token: 0,
            applied_index: 0,
            // Comfortably longer than an election, so an ordinary failover is
            // ridden out rather than reported as a failure.
            request_timeout: config.tick * (config.election_timeout as u32) * 6,
            lease_reads: config.lease_reads,
            lease_duration: config.lease_duration(),
            lease_until: None,
            tick: config.tick,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.tick);
        // A stalled drain must not make the loop catch up on missed ticks in a
        // burst: that would fire several elections' worth of timeout in one
        // pass and depose a perfectly healthy leader.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let mut answer: Option<(NodeId, oneshot::Sender<Message>)> = None;

            tokio::select! {
                _ = ticker.tick() => {
                    self.node.tick();
                }
                Some(Inbound { from, msg, reply }) = self.inbox.recv() => {
                    self.node.step(from, msg);
                    answer = Some((from, reply));
                }
                Some((peer, msg)) = self.peer_replies.recv() => {
                    self.node.step(peer, msg);
                }
                // The one arm that handles its own close. `select!` disables a
                // `Some(..) =` arm whose channel has closed, and the `else`
                // branch only fires when *every* arm is disabled — which the
                // ticker never is. Without this the loop would outlive the
                // process's last client handle and tick forever, holding both
                // Bitcask directories open.
                request = self.requests.recv() => {
                    match request {
                        Some(request) => self.handle_request(request),
                        None => return Ok(()),
                    }
                }
            }

            self.drain(answer)?;
        }
    }

    fn handle_request(&mut self, ClientRequest { op, reply }: ClientRequest) {
        let command = match op {
            // Reads go through ReadIndex (§1.10), never straight to the
            // engine. M6 read local state, which let a deposed leader serve a
            // value that had already been overwritten — see
            // `tests::linearizability`. Only the leader can serve a read, and
            // only after a quorum confirms it still leads.
            ClientOp::Get { key } => {
                self.begin_read(key, reply);
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

    fn begin_read(&mut self, key: Vec<u8>, reply: oneshot::Sender<ClientReply>) {
        // Lease read: a quorum confirmed our leadership recently enough that
        // no other node can have become leader since — *if* the clocks agree.
        // Zero round trips, and the whole correctness argument rests on that
        // proviso, which is why it is off by default.
        if self.lease_reads
            && self.node.role() == Role::Leader
            && self.lease_until.is_some_and(|until| Instant::now() < until)
            && self.applied_index >= self.node.commit_index()
        {
            let value = self.engine.get(&key).ok().flatten();
            let _ = reply.send(ClientReply::Value(value));
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

    /// Answers every read whose confirmed index the state machine has caught
    /// up to.
    fn serve_ready_reads(&mut self) {
        let ready: Vec<u64> = self
            .pending_reads
            .iter()
            .filter(|(_, r)| r.confirmed_at.is_some_and(|i| i <= self.applied_index))
            .map(|(token, _)| *token)
            .collect();

        for token in ready {
            let read = self.pending_reads.remove(&token).expect("just listed");
            let value = self.engine.get(&read.key).ok().flatten();
            let _ = read.reply.send(ClientReply::Value(value));
        }
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
    }

    /// Applies one committed command to the state machine, deduplicating
    /// retries through the session table (§1.8).
    ///
    /// Every replica runs this over the same log in the same order, so the
    /// session table is replicated like everything else — which is the point.
    /// A table kept beside the state machine would die with the leader whose
    /// death made it necessary.
    fn apply(&mut self, command: Command) -> anyhow::Result<CommandResponse> {
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
            session::record(&mut self.engine, &ctx, &response)?;
        }
        Ok(response)
    }

    /// Executes one `Ready`. See the module header for why the order is what
    /// it is.
    fn drain(&mut self, answer: Option<(NodeId, oneshot::Sender<Message>)>) -> anyhow::Result<()> {
        let mut ready = self.node.ready();

        // 1. Disk. `RaftNode` already wrote through to storage inside
        //    `step`/`propose`; this is what makes it durable, and it must
        //    happen before anything leaves this process.
        if !ready.entries.is_empty() || ready.hard_state.is_some() {
            self.node.storage().sync()?;
        }

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
            if let Some(peer) = self.peers.get(&to) {
                let _ = peer.try_send(msg);
            }
        }

        // 4. Apply.
        //
        //    `RaftNode::new` starts `last_applied` at 0 and takes
        //    `commit_index` from `HardState`, so a restart re-applies
        //    everything up to the commit index. That is safe only because
        //    `Put`/`Delete` are idempotent, and it is why the state machine
        //    may hold nothing else until M8's snapshots persist an applied
        //    index.
        for entry in ready.committed {
            let index = entry.index;
            let response = match Command::decode(&entry.command) {
                // The leader's no-op: committed and applied like any entry,
                // and it means nothing to the state machine.
                Ok(None) => None,
                Ok(Some(command)) => Some(self.apply(command)?),
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
        self.serve_ready_reads();

        // 6. If we are no longer the leader, nothing still pending will ever
        //    commit or confirm under us. Answering now beats making the client
        //    wait out its deadline.
        if self.node.role() != Role::Leader
            && !(self.pending.is_empty() && self.pending_reads.is_empty())
        {
            let hint = self.node.leader_id();
            for (_, pending) in std::mem::take(&mut self.pending) {
                let _ = pending.reply.send(ClientReply::NotLeader { hint });
            }
            for (_, read) in std::mem::take(&mut self.pending_reads) {
                let _ = read.reply.send(ClientReply::NotLeader { hint });
            }
        }

        // 7. And whatever has simply waited too long — a partitioned leader
        //    never learns it was deposed, so nothing above will ever fire.
        self.expire_stale_requests();

        Ok(())
    }
}
