//! The simulated cluster (M4, tasks 3 and 4).
//!
//! One `step()` is: deliver everything due, tick every node, drain each
//! `Ready`, route outbound messages back into the network, record what was
//! committed — then run the M3.7 invariant suite. Every step, no exceptions.
//!
//! `RaftNode` persists to its own storage as it goes, so the driver does not
//! replay `Ready::entries`; those are a record of what was written, not a
//! to-do list. What the driver owns is the network and time.

use std::collections::{BTreeMap, BTreeSet};

use kv_raft::invariants::{NodeView, Violation, check_views};
use kv_raft::storage::MemStorage;
use kv_raft::{Config, Entry, LogIndex, NodeId, RaftNode, Role};

use crate::clock::{Clock, SimRng};
use crate::network::{Envelope, Network, NetworkConfig};

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub seed: u64,
    pub nodes: usize,
    pub network: NetworkConfig,
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
}

impl SimConfig {
    /// A perfect network, so a test that injects no faults sees none.
    pub fn new(seed: u64, nodes: usize) -> Self {
        Self {
            seed,
            nodes,
            network: NetworkConfig::default(),
            election_timeout: 10,
            heartbeat_interval: 2,
        }
    }

    pub fn with_network(mut self, network: NetworkConfig) -> Self {
        self.network = network;
        self
    }
}

/// One observable thing that happened, recorded in order.
///
/// Compared rather than hashed: a hash says "the runs differed", a trace says
/// where. Reproducibility is the foundation of this milestone, so its failures
/// need to be debuggable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEvent {
    Delivered { tick: u64, from: NodeId, to: NodeId },
    Committed { tick: u64, node: NodeId, index: LogIndex, term: u64 },
    RoleChange { tick: u64, node: NodeId, role: Role, term: u64 },
    Crashed { tick: u64, node: NodeId },
    Restarted { tick: u64, node: NodeId },
}

pub struct Cluster {
    clock: Clock,
    rng: SimRng,
    network: Network,
    /// BTreeMap, not HashMap: this is iterated every single step, and hash
    /// order would make the whole simulator irreproducible.
    nodes: BTreeMap<NodeId, RaftNode<MemStorage>>,
    /// Storage of crashed nodes, waiting for a restart to rebuild from it.
    down: BTreeMap<NodeId, MemStorage>,
    config: SimConfig,
    roles: BTreeMap<NodeId, (Role, u64)>,
    trace: Vec<TraceEvent>,
    /// Everything any node has ever reported committed, by index.
    committed: BTreeMap<LogIndex, Entry>,
}

impl Cluster {
    pub fn new(config: SimConfig) -> Self {
        let ids: Vec<NodeId> = (1..=config.nodes as u64).collect();
        let mut nodes = BTreeMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
            let cfg = Config {
                id,
                peers,
                election_timeout: config.election_timeout,
                heartbeat_interval: config.heartbeat_interval,
                // Per-node seeds derived from the run seed, so each node
                // randomises its timeout differently but reproducibly.
                seed: config.seed.wrapping_mul(1_000_003).wrapping_add(id),
            };
            nodes.insert(id, RaftNode::new(cfg, MemStorage::default()));
        }
        let roles = ids.iter().map(|&id| (id, (Role::Follower, 0))).collect();
        Self {
            clock: Clock::default(),
            rng: SimRng::new(config.seed),
            network: Network::new(config.network),
            nodes,
            down: BTreeMap::new(),
            config,
            roles,
            trace: Vec::new(),
            committed: BTreeMap::new(),
        }
    }

    pub fn now(&self) -> u64 {
        self.clock.now()
    }

    pub fn trace(&self) -> &[TraceEvent] {
        &self.trace
    }

    pub fn seed(&self) -> u64 {
        self.rng.seed()
    }

    /// The single leader, or `None` when there is no leader or more than one.
    /// More than one is not reported as a leader rather than being asserted on
    /// here — `check_all` is what decides whether that is a violation.
    pub fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.role() == Role::Leader)
            .map(|(id, _)| *id)
            .collect();
        match leaders.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    pub fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Everything any node has ever reported committed.
    pub fn committed(&self) -> Vec<Entry> {
        self.committed.values().cloned().collect()
    }

    pub fn partition(&mut self, groups: &[&[NodeId]]) {
        self.network.partition(groups);
    }

    pub fn heal(&mut self) {
        self.network.heal();
    }

    /// Drops everything the node held in memory. Only `RaftStorage` survives.
    pub fn crash(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.remove(&id) {
            self.down.insert(id, node.into_storage());
            self.roles.remove(&id);
            self.trace.push(TraceEvent::Crashed { tick: self.clock.now(), node: id });
        }
    }

    /// Rebuilds the node from persisted state alone.
    pub fn restart(&mut self, id: NodeId) {
        let Some(storage) = self.down.remove(&id) else { return };
        let ids: Vec<NodeId> = (1..=self.config.nodes as u64).collect();
        let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
        let cfg = Config {
            id,
            peers,
            election_timeout: self.config.election_timeout,
            heartbeat_interval: self.config.heartbeat_interval,
            seed: self.config.seed.wrapping_mul(1_000_003).wrapping_add(id),
        };
        self.nodes.insert(id, RaftNode::new(cfg, storage));
        self.roles.insert(id, (Role::Follower, 0));
        self.trace.push(TraceEvent::Restarted { tick: self.clock.now(), node: id });
    }

    pub fn propose(&mut self, cmd: Vec<u8>) -> Option<LogIndex> {
        let leader = self.leader()?;
        self.nodes.get_mut(&leader)?.propose(cmd).ok()
    }

    /// One tick of the whole cluster, ending in a full invariant check.
    pub fn step(&mut self) -> Result<(), Violation> {
        let tick = self.clock.advance();

        // 1. Deliver. A message to a crashed node is dropped, which is what a
        //    crashed node looks like from the network's side.
        let mut outbound: Vec<Envelope> = Vec::new();
        for envelope in self.network.take_due(tick) {
            let Some(node) = self.nodes.get_mut(&envelope.to) else { continue };
            self.trace.push(TraceEvent::Delivered { tick, from: envelope.from, to: envelope.to });
            node.step(envelope.from, envelope.msg);
        }

        // 2. Tick every live node, then drain what it produced.
        for (&id, node) in self.nodes.iter_mut() {
            node.tick();
            let ready = node.ready();
            for (to, msg) in ready.messages {
                outbound.push(Envelope { from: id, to, msg });
            }
            for e in ready.committed {
                self.trace.push(TraceEvent::Committed {
                    tick,
                    node: id,
                    index: e.index,
                    term: e.term,
                });
                self.committed.entry(e.index).or_insert(e);
            }
        }

        // 3. Record role changes, for the trace and for debugging a failure.
        for (&id, node) in self.nodes.iter() {
            let current = (node.role(), node.current_term());
            let previous = self.roles.insert(id, current);
            if previous != Some(current) {
                self.trace.push(TraceEvent::RoleChange {
                    tick,
                    node: id,
                    role: current.0,
                    term: current.1,
                });
            }
        }

        // 4. Back into the network, where they may be dropped or delayed.
        for envelope in outbound {
            self.network.send(tick, &mut self.rng, envelope);
        }

        // 5. The whole point.
        let views: Vec<NodeView> = self.nodes.values().map(NodeView::of).collect();
        check_views(&views)
    }

    pub fn run(&mut self, ticks: u64) -> Result<(), Violation> {
        for _ in 0..ticks {
            self.step()?;
        }
        Ok(())
    }

    /// Runs until there is a single leader, or `None` if `max_ticks` passes.
    pub fn run_until_leader(&mut self, max_ticks: u64) -> Result<Option<NodeId>, Violation> {
        for _ in 0..max_ticks {
            self.step()?;
            if let Some(id) = self.leader() {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// Term, role, commit index and log of one node — for diagnosing a
    /// failing seed.
    pub fn debug_node(&self, id: NodeId) -> (u64, Role, LogIndex, Vec<Entry>) {
        let n = &self.nodes[&id];
        (n.current_term(), n.role(), n.commit_index(), n.log_entries())
    }

    /// Node ids that are currently crashed.
    pub fn down_nodes(&self) -> BTreeSet<NodeId> {
        self.down.keys().copied().collect()
    }
}
