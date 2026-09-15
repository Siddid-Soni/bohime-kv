//! Randomised fault injection (M4, task 5).
//!
//! Partition, heal, crash and restart, drawn from a stream derived from the
//! run seed — so a nemesis schedule is reproducible, but does not shift when
//! the network's draw count changes.

use kv_raft::NodeId;

use crate::clock::SimRng;
use crate::cluster::Cluster;

#[derive(Debug, Clone, Copy)]
pub struct NemesisConfig {
    /// Chance per step that *some* fault is injected.
    pub fault_percent: u32,
    /// Chance that a node that is down is brought back, per step. Kept
    /// well above `fault_percent` so the cluster spends most of its time
    /// recovering rather than dead — a cluster that is always in pieces never
    /// commits anything, and a test where nothing commits proves nothing.
    pub recover_percent: u32,
}

impl Default for NemesisConfig {
    fn default() -> Self {
        Self { fault_percent: 4, recover_percent: 25 }
    }
}

pub struct Nemesis {
    rng: SimRng,
    config: NemesisConfig,
}

impl Nemesis {
    /// Stream 1 of the run seed; the network holds stream 0.
    pub fn new(seed: u64, config: NemesisConfig) -> Self {
        Self { rng: SimRng::derive(seed, 1), config }
    }

    /// Possibly injects one fault. Call once per step, before `Cluster::step`.
    pub fn act(&mut self, cluster: &mut Cluster) {
        let down = cluster.down_nodes();
        if !down.is_empty() && self.rng.chance(self.config.recover_percent) {
            let victims: Vec<NodeId> = down.iter().copied().collect();
            let pick = self.rng.below(victims.len() as u64) as usize;
            cluster.restart(victims[pick]);
            return;
        }
        if !self.rng.chance(self.config.fault_percent) {
            return;
        }

        let live = cluster.live_nodes();
        match self.rng.below(4) {
            0 => {
                // Isolate one node from the rest.
                if let Some(&victim) = live.get(self.rng.below(live.len() as u64) as usize) {
                    let rest: Vec<NodeId> = live.iter().copied().filter(|n| *n != victim).collect();
                    cluster.partition(&[&[victim], &rest]);
                }
            }
            1 => cluster.heal(),
            2 => {
                // Never crash the last node standing: an empty cluster cannot
                // make progress and the run stops testing anything.
                if live.len() > 1
                    && let Some(&victim) = live.get(self.rng.below(live.len() as u64) as usize)
                {
                    cluster.crash(victim);
                }
            }
            _ => cluster.heal(),
        }
    }
}
