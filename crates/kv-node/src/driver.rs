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
use std::time::Duration;

use kv_raft::{LogIndex, Message, NodeId, ProposeError, RaftNode, Role, Term};
use kv_storage::Engine;
use tokio::sync::{mpsc, oneshot};

use crate::command::Command;
use crate::config::NodeConfig;
use crate::storage::BitcaskStorage;
use crate::transport::peer::PeerClient;
use crate::transport::server::Inbound;

#[derive(Debug)]
pub enum ClientOp {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub enum ClientReply {
    Value(Option<Vec<u8>>),
    Applied,
    NotLeader { hint: Option<NodeId> },
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
}

pub struct Driver {
    node: RaftNode<BitcaskStorage>,
    /// The state machine: a second Bitcask instance, in its own directory.
    engine: Engine,
    peers: BTreeMap<NodeId, PeerClient>,
    inbox: mpsc::Receiver<Inbound>,
    peer_replies: mpsc::Receiver<(NodeId, Message)>,
    requests: mpsc::Receiver<ClientRequest>,
    pending: BTreeMap<LogIndex, Pending>,
    tick: Duration,
}

impl Driver {
    pub fn new(
        config: &NodeConfig,
        node: RaftNode<BitcaskStorage>,
        engine: Engine,
        peers: BTreeMap<NodeId, PeerClient>,
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
                Some(request) = self.requests.recv() => {
                    self.handle_request(request);
                }
                else => return Ok(()),
            }

            self.drain(answer)?;
        }
    }

    fn handle_request(&mut self, ClientRequest { op, reply }: ClientRequest) {
        let command = match op {
            // The read is deliberately local and therefore deliberately
            // **stale**: a deposed leader that has not yet heard about the
            // election will happily serve its old value. That is expected at
            // M6 — the gate is "get k on any node returns it" — and M7
            // replaces it with ReadIndex. Do not mistake this for a finished
            // path.
            ClientOp::Get { key } => {
                let value = self.engine.get(&key).ok().flatten();
                let _ = reply.send(ClientReply::Value(value));
                return;
            }
            ClientOp::Put { key, value } => Command::Put { key, value },
            ClientOp::Delete { key } => Command::Delete { key },
        };

        match self.node.propose(command.encode()) {
            Ok(index) => {
                self.pending.insert(index, Pending { term: self.node.current_term(), reply });
            }
            Err(ProposeError::NotLeader) => {
                let _ = reply.send(ClientReply::NotLeader { hint: self.node.leader_id() });
            }
        }
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
            match Command::decode(&entry.command) {
                // The leader's no-op: committed and applied like any entry,
                // and it means nothing to the state machine.
                Ok(None) => {}
                Ok(Some(Command::Put { key, value })) => {
                    self.engine.put(&key, &value)?;
                }
                Ok(Some(Command::Delete { key })) => {
                    self.engine.delete(&key)?;
                }
                Err(e) => {
                    // A committed entry we cannot decode means the log and
                    // this binary disagree about the state machine. Applying
                    // past it would diverge the replicas silently.
                    anyhow::bail!("undecodable committed entry at index {}: {e}", entry.index);
                }
            }

            if let Some(pending) = self.pending.remove(&entry.index) {
                let reply = if pending.term == entry.term {
                    ClientReply::Applied
                } else {
                    // Our entry was overwritten by a later leader before it
                    // committed. The write did not happen.
                    ClientReply::NotLeader { hint: self.node.leader_id() }
                };
                let _ = pending.reply.send(reply);
            }
        }

        // 5. If we are no longer the leader, nothing still pending will ever
        //    commit under us. Answering now beats making the client wait out
        //    its deadline.
        if self.node.role() != Role::Leader && !self.pending.is_empty() {
            let hint = self.node.leader_id();
            for (_, pending) in std::mem::take(&mut self.pending) {
                let _ = pending.reply.send(ClientReply::NotLeader { hint });
            }
        }

        Ok(())
    }
}
