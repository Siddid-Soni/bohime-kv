//! The adversarial in-memory network (M4, task 2).
//!
//! Messages are not delivered when sent. They go into an in-flight queue keyed
//! by `(deliver_at, seq)` and come out when the clock reaches them — which is
//! where drop, delay, duplication and reordering all come from.
//!
//! Reorder needs no mechanism of its own: two messages sent in the same tick
//! with different delays arrive out of order by construction. The `seq` tie-
//! breaker exists because a `BTreeMap<u64, Vec<Envelope>>` would still force an
//! ordering decision on the `Vec`, just less visibly.

use std::collections::{BTreeMap, BTreeSet};

use kv_raft::{Message, NodeId};

use crate::clock::SimRng;

/// How unreliable the network is. All rates are integer percentages — see
/// `SimRng::chance` for why not floats.
#[derive(Debug, Clone, Copy)]
pub struct NetworkConfig {
    pub drop_percent: u32,
    pub duplicate_percent: u32,
    /// Delivery is delayed by a uniform draw from `[1, max_delay_ticks]`. A
    /// value of 1 means every message arrives on the next tick.
    pub max_delay_ticks: u64,
}

impl Default for NetworkConfig {
    /// A perfect network: nothing dropped, nothing duplicated, one-tick
    /// delivery. Tests opt into adversity explicitly, so a test that fails has
    /// exactly the faults it asked for.
    fn default() -> Self {
        Self { drop_percent: 0, duplicate_percent: 0, max_delay_ticks: 1 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

pub struct Network {
    config: NetworkConfig,
    /// Keyed by `(deliver_at, seq)` so iteration order is total and seeded.
    in_flight: BTreeMap<(u64, u64), Envelope>,
    next_seq: u64,
    /// Partition groups. Nodes in different groups cannot reach each other;
    /// empty means fully connected.
    partitions: Vec<BTreeSet<NodeId>>,
}

impl Network {
    pub fn new(config: NetworkConfig) -> Self {
        Self { config, in_flight: BTreeMap::new(), next_seq: 0, partitions: Vec::new() }
    }

    /// Splits the cluster. Nodes reach only nodes in their own group; a node
    /// named in no group is isolated from everything.
    pub fn partition(&mut self, groups: &[&[NodeId]]) {
        self.partitions = groups.iter().map(|g| g.iter().copied().collect()).collect();
    }

    /// Removes every partition. In-flight messages are unaffected: they were
    /// already accepted, and dropping them here would mean a heal could lose a
    /// message the network had promised to deliver.
    pub fn heal(&mut self) {
        self.partitions.clear();
    }

    fn reachable(&self, from: NodeId, to: NodeId) -> bool {
        if self.partitions.is_empty() {
            return true;
        }
        self.partitions.iter().any(|g| g.contains(&from) && g.contains(&to))
    }

    /// Offers a message to the network. It may be dropped, delayed, or
    /// duplicated. A message across a partition is dropped silently — that is
    /// what a partition looks like from inside a node.
    pub fn send(&mut self, now: u64, rng: &mut SimRng, envelope: Envelope) {
        if !self.reachable(envelope.from, envelope.to) {
            return;
        }
        if rng.chance(self.config.drop_percent) {
            return;
        }
        self.enqueue(now, rng, envelope.clone());
        if rng.chance(self.config.duplicate_percent) {
            // A duplicate draws its own delay, so it rarely lands with the
            // original — a duplicate that always arrived alongside its twin
            // would not exercise anything the original does not.
            self.enqueue(now, rng, envelope);
        }
    }

    fn enqueue(&mut self, now: u64, rng: &mut SimRng, envelope: Envelope) {
        let delay = 1 + rng.below(self.config.max_delay_ticks.max(1));
        let seq = self.next_seq;
        self.next_seq += 1;
        self.in_flight.insert((now + delay, seq), envelope);
    }

    /// Everything due at or before `now`, in delivery order.
    ///
    /// Messages to or from a node that became unreachable *after* they were
    /// sent are discarded here rather than delivered: a partition that only
    /// filtered at send time would let a message cross it by being in flight
    /// when it went up.
    pub fn take_due(&mut self, now: u64) -> Vec<Envelope> {
        let due: Vec<(u64, u64)> =
            self.in_flight.range(..=(now, u64::MAX)).map(|(k, _)| *k).collect();
        let taken: Vec<Envelope> =
            due.into_iter().filter_map(|k| self.in_flight.remove(&k)).collect();
        taken.into_iter().filter(|e| self.reachable(e.from, e.to)).collect()
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}
