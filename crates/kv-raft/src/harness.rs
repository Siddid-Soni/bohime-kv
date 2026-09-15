//! Test-only cluster driver: ticks nodes, delivers messages until quiescence.
//! Every M3.3+ test that needs agreement runs through this.

use crate::message::{Action, Config, Message, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;
use crate::types::NodeId;

pub fn config(id: NodeId, peers: Vec<NodeId>, seed: u64) -> Config {
    Config { id, peers, election_timeout: 10, heartbeat_interval: 2, seed }
}

pub struct Cluster {
    pub nodes: Vec<RaftNode<MemStorage>>,
}

impl Cluster {
    pub fn of_three() -> Self {
        let ids = [1, 2, 3];
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|p| *p != id).collect();
                RaftNode::new(config(id, peers, 1000 + id), MemStorage::default())
            })
            .collect();
        Self { nodes }
    }

    /// Tick every node once, then deliver all resulting messages (and their
    /// responses, recursively) until no messages remain in flight.
    pub fn tick_all(&mut self) {
        let mut pending = Vec::new();
        for node in self.nodes.iter_mut() {
            let from = node.id();
            for action in node.tick() {
                if let Action::Send { to, msg } = action {
                    pending.push((from, to, msg));
                }
            }
        }
        self.deliver_until_quiet(pending);
    }

    fn deliver_until_quiet(&mut self, mut pending: Vec<(NodeId, NodeId, Message)>) {
        for _ in 0..16 {
            if pending.is_empty() {
                return;
            }
            let mut next = Vec::new();
            for (from, to, msg) in pending.drain(..) {
                if let Some(node) = self.nodes.iter_mut().find(|n| n.id() == to) {
                    let responder = node.id();
                    for action in node.step(from, msg) {
                        if let Action::Send { to, msg } = action {
                            next.push((responder, to, msg));
                        }
                    }
                }
            }
            pending = next;
        }
    }

    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes.iter().filter(|n| n.role() == Role::Leader).map(|n| n.id()).collect()
    }

    pub fn run_until_leader(&mut self, max_ticks: u64) -> NodeId {
        for _ in 0..max_ticks {
            self.tick_all();
            let leaders = self.leaders();
            if leaders.len() == 1 {
                return leaders[0];
            }
        }
        panic!("no single leader within {max_ticks} ticks");
    }
}
