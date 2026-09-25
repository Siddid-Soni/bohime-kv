use crate::pb::{Command, Op};
use crate::raft::{Raft, Role};
use std::collections::HashSet;

/// Raft nodes 1..=n wired by direct delivery; a node in `down` neither ticks
/// nor sends nor receives.
struct Cluster {
    nodes: Vec<Raft>,
    down: HashSet<u64>,
}

impl Cluster {
    fn new(n: u64) -> Self {
        let nodes = (1..=n).map(|id| Raft::new(id, peers(id, n), 0, 0, vec![])).collect();
        Self { nodes, down: HashSet::new() }
    }

    fn node(&mut self, id: u64) -> &mut Raft {
        &mut self.nodes[id as usize - 1]
    }

    fn deliver(&mut self) {
        loop {
            let mut msgs = vec![];
            for n in &mut self.nodes {
                let out = std::mem::take(&mut n.outbox);
                if !self.down.contains(&n.id) {
                    msgs.extend(out);
                }
            }
            if msgs.is_empty() {
                return;
            }
            for m in msgs {
                if !self.down.contains(&m.to) {
                    self.node(m.to).step(m);
                }
            }
        }
    }

    fn tick(&mut self, times: usize) {
        for _ in 0..times {
            for n in &mut self.nodes {
                if !self.down.contains(&n.id) {
                    n.tick();
                }
            }
            self.deliver();
        }
    }

    fn leaders(&self) -> Vec<u64> {
        let up = self.nodes.iter().filter(|n| !self.down.contains(&n.id));
        up.filter(|n| n.role == Role::Leader).map(|n| n.id).collect()
    }

    fn elect(&mut self) -> u64 {
        for _ in 0..1000 {
            self.tick(1);
            if let [leader] = self.leaders()[..] {
                return leader;
            }
        }
        panic!("no leader elected");
    }

    fn propose(&mut self, id: u64, key: &str) -> u64 {
        let index = self.node(id).propose(put(key)).expect("not the leader");
        self.node(id).broadcast();
        self.deliver();
        index
    }

    fn restart(&mut self, id: u64) {
        let n = self.nodes.len() as u64;
        let old = self.node(id);
        let log = old.log[1..].to_vec();
        *old = Raft::new(id, peers(id, n), old.term, old.voted_for, log);
    }
}

fn peers(id: u64, n: u64) -> Vec<u64> {
    (1..=n).filter(|&p| p != id).collect()
}

fn put(key: &str) -> Command {
    Command { op: Op::Put as i32, key: key.into(), value: b"v".to_vec() }
}

fn key_at(r: &Raft, index: u64) -> Option<Vec<u8>> {
    r.log.get(index as usize)?.cmd.as_ref().map(|c| c.key.clone())
}

#[test]
fn exactly_one_leader_is_elected() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.tick(50);
    assert_eq!(c.leaders(), vec![leader]);
}

#[test]
fn a_single_node_group_elects_itself_and_commits() {
    let mut c = Cluster::new(1);
    c.elect();
    let index = c.propose(1, "k");
    assert_eq!(c.node(1).commit, index);
}

#[test]
fn a_proposal_commits_on_every_node() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    let index = c.propose(leader, "k");
    c.tick(2);
    for n in &c.nodes {
        assert!(n.commit >= index, "node {} commit {}", n.id, n.commit);
        assert_eq!(key_at(n, index), Some(b"k".to_vec()));
    }
}

#[test]
fn a_leader_cut_off_from_the_majority_cannot_commit() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.down.extend(peers(leader, 3));
    let index = c.propose(leader, "k");
    c.tick(50);
    assert!(c.node(leader).commit < index);
}

#[test]
fn a_new_leader_overwrites_the_old_leaders_uncommitted_entry() {
    let mut c = Cluster::new(3);
    let old = c.elect();
    c.down.insert(old);
    let lost = c.node(old).propose(put("lost")).unwrap();

    let new = c.elect();
    assert_ne!(new, old);
    let kept = c.propose(new, "kept");
    c.tick(2);
    assert!(c.node(new).commit >= kept);

    c.down.clear();
    c.tick(30);
    assert_eq!(c.leaders(), vec![new]);
    let log = c.node(new).log.clone();
    assert_eq!(c.node(old).log, log);
    assert!(c.node(old).commit >= kept);
    assert_ne!(key_at(c.node(old), lost), Some(b"lost".to_vec()));
}

#[test]
fn a_restarted_follower_catches_up_from_its_persisted_log() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.propose(leader, "before");
    c.tick(2);
    let follower = peers(leader, 3)[0];
    c.down.insert(follower);
    let last = (0..20).map(|i| c.propose(leader, &format!("k{i}"))).last().unwrap();
    c.tick(2);

    c.restart(follower);
    assert_eq!(c.node(follower).commit, 0, "commit is not persisted");
    c.down.clear();
    c.tick(30);
    let log = c.node(leader).log.clone();
    assert_eq!(c.node(follower).log, log);
    assert!(c.node(follower).commit >= last);
}

#[test]
fn an_entry_is_sent_to_a_follower_once_while_unacknowledged() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    c.node(leader).propose(put("a"));
    c.node(leader).broadcast();
    c.node(leader).propose(put("b"));
    c.node(leader).broadcast();
    c.node(leader).tick();
    let to = peers(leader, 3)[0];
    let sent: Vec<_> = c.node(leader).outbox.iter().filter(|m| m.to == to).collect();
    let entries: usize = sent.iter().map(|m| m.entries.len()).sum();
    assert_eq!(entries, 2, "{sent:?}");
}

#[test]
fn a_lost_append_is_resent() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    let index = c.node(leader).propose(put("k")).unwrap();
    c.node(leader).broadcast();
    c.node(leader).outbox.clear();
    c.tick(3);
    for n in &c.nodes {
        assert!(n.commit >= index, "node {} commit {}", n.id, n.commit);
        assert_eq!(key_at(n, index), Some(b"k".to_vec()));
    }
}

#[test]
fn an_append_is_capped_at_a_megabyte_but_carries_at_least_one_entry() {
    let mut c = Cluster::new(3);
    let leader = c.elect();
    let value =
        |kb: usize| Command { op: Op::Put as i32, key: b"k".to_vec(), value: vec![0; kb << 10] };
    let to = peers(leader, 3)[0];
    let sent = |r: &Raft| -> Vec<usize> {
        r.outbox.iter().filter(|m| m.to == to).map(|m| m.entries.len()).collect()
    };

    c.node(leader).propose(value(2048));
    c.node(leader).broadcast();
    assert_eq!(sent(c.node(leader)), [1], "an entry over the cap still goes");
    c.deliver();

    for _ in 0..10 {
        c.node(leader).propose(value(300));
    }
    c.node(leader).broadcast();
    assert_eq!(sent(c.node(leader)), [3], "three 300 KB entries fit in 1 MB, four do not");
    c.tick(10);
    let log = c.node(leader).log.clone();
    assert_eq!(c.node(to).log, log, "the rest follows on later broadcasts");
}
