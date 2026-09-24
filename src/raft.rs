//! Raft for one group: leader election and log replication, nothing else — no
//! snapshots, no membership changes. Pure: `tick`, `step` and `propose`
//! change state and queue messages in `outbox`; the caller persists `term`,
//! `voted_for` and `log[stable..]`, then sends, then applies up to `commit`.

use crate::pb::{Command, Entry, Kind, Msg};
use rand::Rng;
use std::collections::{HashMap, HashSet};

/// Election timeout, in ticks, drawn from `[ELECTION_TICKS, 2 * ELECTION_TICKS)`.
/// The leader heartbeats every tick.
const ELECTION_TICKS: u64 = 10;
/// Most entries sent in one append, so a far-behind follower catches up in
/// bounded steps.
const MAX_BATCH: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Raft {
    pub id: u64,
    peers: Vec<u64>,
    pub term: u64,
    /// 0 = nobody; node ids start at 1.
    pub voted_for: u64,
    /// `log[0]` is a sentinel at term 0, so a log index is a `Vec` index.
    pub log: Vec<Entry>,
    /// `log[..stable]` is on disk; truncation lowers it.
    pub stable: usize,
    pub commit: u64,
    pub applied: u64,
    pub role: Role,
    /// 0 = unknown.
    pub leader: u64,
    pub outbox: Vec<Msg>,
    votes: HashSet<u64>,
    next: HashMap<u64, u64>,
    matched: HashMap<u64, u64>,
    elapsed: u64,
    timeout: u64,
}

impl Raft {
    /// `log` is the persisted log from index 1. `commit` starts at 0 even on
    /// a restart: a node relearns it from the leader.
    pub fn new(id: u64, peers: Vec<u64>, term: u64, voted_for: u64, log: Vec<Entry>) -> Self {
        let log: Vec<Entry> = std::iter::once(Entry::default()).chain(log).collect();
        let mut raft = Self {
            id,
            peers,
            term,
            voted_for,
            stable: log.len(),
            log,
            commit: 0,
            applied: 0,
            role: Role::Follower,
            leader: 0,
            outbox: vec![],
            votes: HashSet::new(),
            next: HashMap::new(),
            matched: HashMap::new(),
            elapsed: 0,
            timeout: 0,
        };
        raft.reset_timer();
        raft
    }

    pub fn tick(&mut self) {
        self.elapsed += 1;
        if self.role == Role::Leader {
            self.broadcast();
        } else if self.elapsed >= self.timeout {
            self.campaign();
        }
    }

    /// Appends to the leader's log and returns the entry's index. Nothing is
    /// sent until `broadcast`, so a burst of proposals goes out together.
    pub fn propose(&mut self, cmd: Command) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        self.log.push(Entry { term: self.term, cmd: Some(cmd) });
        Some(self.last().0)
    }

    pub fn broadcast(&mut self) {
        for to in self.peers.clone() {
            self.send_append(to);
        }
        self.advance_commit();
    }

    pub fn step(&mut self, m: Msg) {
        if m.term > self.term {
            self.term = m.term;
            self.voted_for = 0;
            self.role = Role::Follower;
            self.leader = 0;
        }
        if m.term < self.term {
            // Tell a deposed leader about the new term so it steps down.
            if m.kind() == Kind::Append {
                self.send(Msg { kind: Kind::AppendResp as i32, to: m.from, ..Default::default() });
            }
            return;
        }
        match m.kind() {
            Kind::Vote => {
                let up_to_date = (m.log_term, m.index) >= (self.last().1, self.last().0);
                let grant = up_to_date && (self.voted_for == 0 || self.voted_for == m.from);
                if grant {
                    self.voted_for = m.from;
                    self.reset_timer();
                }
                let reply = Msg {
                    kind: Kind::VoteResp as i32,
                    to: m.from,
                    success: grant,
                    ..Default::default()
                };
                self.send(reply);
            }
            Kind::VoteResp if self.role == Role::Candidate && m.success => {
                self.votes.insert(m.from);
                self.maybe_win();
            }
            Kind::Append => self.handle_append(m),
            Kind::AppendResp if self.role == Role::Leader => self.handle_append_resp(m),
            _ => {}
        }
    }

    fn campaign(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = self.id;
        self.leader = 0;
        self.votes = HashSet::from([self.id]);
        self.reset_timer();
        let (index, log_term) = self.last();
        for to in self.peers.clone() {
            self.send(Msg { kind: Kind::Vote as i32, to, index, log_term, ..Default::default() });
        }
        self.maybe_win();
    }

    fn maybe_win(&mut self) {
        if self.votes.len() < self.quorum() {
            return;
        }
        self.role = Role::Leader;
        self.leader = self.id;
        let next = self.log.len() as u64;
        self.next = self.peers.iter().map(|&p| (p, next)).collect();
        self.matched = self.peers.iter().map(|&p| (p, 0)).collect();
        // A leader may only count replicas for entries of its own term, so
        // this no-op is what lets earlier terms' entries commit.
        self.log.push(Entry { term: self.term, cmd: None });
        self.broadcast();
    }

    fn handle_append(&mut self, m: Msg) {
        self.role = Role::Follower;
        self.leader = m.from;
        self.reset_timer();
        let prev = m.index as usize;
        if self.log.get(prev).is_none_or(|e| e.term != m.log_term) {
            // Back the leader up to just before the mismatch (or our end).
            let hint = self.last().0.min(m.index - 1);
            let reply = Msg {
                kind: Kind::AppendResp as i32,
                to: m.from,
                index: hint,
                ..Default::default()
            };
            return self.send(reply);
        }
        let last_new = prev + m.entries.len();
        for (i, entry) in m.entries.into_iter().enumerate() {
            let index = prev + 1 + i;
            if self.log.get(index).is_some_and(|e| e.term == entry.term) {
                continue;
            }
            self.log.truncate(index);
            self.stable = self.stable.min(index);
            self.log.push(entry);
        }
        self.commit = self.commit.max(m.commit.min(last_new as u64));
        let reply = Msg {
            kind: Kind::AppendResp as i32,
            to: m.from,
            index: last_new as u64,
            success: true,
            ..Default::default()
        };
        self.send(reply);
    }

    fn handle_append_resp(&mut self, m: Msg) {
        if m.success {
            let matched = self.matched.entry(m.from).or_default();
            *matched = (*matched).max(m.index);
            let next = self.next.entry(m.from).or_default();
            *next = (*next).max(m.index + 1);
            self.advance_commit();
        } else {
            self.next.insert(m.from, m.index + 1);
            self.send_append(m.from);
        }
    }

    fn send_append(&mut self, to: u64) {
        let next = self.next[&to] as usize;
        let entries = self.log[next..].iter().take(MAX_BATCH).cloned().collect();
        let m = Msg {
            kind: Kind::Append as i32,
            to,
            index: next as u64 - 1,
            log_term: self.log[next - 1].term,
            entries,
            commit: self.commit,
            ..Default::default()
        };
        self.send(m);
    }

    /// Commit the highest index a quorum holds, if it is from this term.
    fn advance_commit(&mut self) {
        let mut matched: Vec<u64> = self.peers.iter().map(|p| self.matched[p]).collect();
        matched.push(self.last().0);
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let n = matched[self.quorum() - 1];
        if n > self.commit && self.log[n as usize].term == self.term {
            self.commit = n;
        }
    }

    fn send(&mut self, mut m: Msg) {
        m.from = self.id;
        m.term = self.term;
        self.outbox.push(m);
    }

    fn last(&self) -> (u64, u64) {
        (self.log.len() as u64 - 1, self.log.last().unwrap().term)
    }

    fn quorum(&self) -> usize {
        let voters = self.peers.len() + 1;
        voters / 2 + 1
    }

    fn reset_timer(&mut self) {
        self.elapsed = 0;
        self.timeout = rand::thread_rng().gen_range(ELECTION_TICKS..2 * ELECTION_TICKS);
    }
}
