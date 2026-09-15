//! The Raft role state machine (M3.2–M3.6). `RaftNode` owns its storage,
//! accepts `tick`/`step`/`propose`, and returns `Action`s describing what the
//! caller must do — persist entries, persist hard state, send messages, apply
//! committed entries — in that order (§1.5: disk before network).

use std::collections::{BTreeMap, HashSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::election::{CandidateInfo, VoterState, should_grant_vote};
use crate::log::last_log;
use crate::message::{Action, Config, Message, Ready, Role};
use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, LogIndex, NodeId, Term};

pub struct RaftNode<S: RaftStorage> {
    config: Config,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    votes_received: HashSet<NodeId>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    storage: S,
    rng: StdRng,
    outbox: Vec<Action>,
}

impl<S: RaftStorage> RaftNode<S> {
    pub fn new(config: Config, storage: S) -> Self {
        let hs: HardState = storage.hard_state().expect("raft storage");
        let mut rng = StdRng::seed_from_u64(config.seed);
        let election_timeout = random_timeout(&mut rng, config.election_timeout);
        Self {
            config,
            role: Role::Follower,
            current_term: hs.term,
            voted_for: hs.voted_for,
            commit_index: hs.commit_index,
            last_applied: 0,
            votes_received: HashSet::new(),
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            storage,
            rng,
            outbox: Vec::new(),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn id(&self) -> NodeId {
        self.config.id
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    pub fn match_index_of(&self, peer: NodeId) -> Option<LogIndex> {
        self.match_index.get(&peer).copied()
    }

    /// One logical-clock step. Followers/candidates count toward an election;
    /// leaders count toward a heartbeat.
    pub fn tick(&mut self) -> Vec<Action> {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                self.broadcast_heartbeats();
            }
            return self.drain_actions();
        }

        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            self.start_election();
        }
        self.drain_actions()
    }

    /// Routes one inbound message through the term rules first, then dispatch.
    /// `from` is the sender's id — vote and replication accounting must know
    /// who answered, and anonymous votes would be double-countable.
    pub fn step(&mut self, from: NodeId, msg: Message) -> Vec<Action> {
        let term = msg.term();
        if term > self.current_term {
            self.observe_higher_term(term);
        }
        match msg {
            Message::RequestVote {
                term: candidate_term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(
                    from,
                    candidate_term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                );
            }
            Message::RequestVoteResp { term: resp_term, vote_granted } => {
                self.handle_request_vote_resp(from, resp_term, vote_granted);
            }
            Message::AppendEntries {
                term: msg_term,
                leader_id,
                prev_log_index: _,
                prev_log_term: _,
                entries: _,
                leader_commit: _,
            } => {
                if msg_term < self.current_term {
                    self.send(
                        leader_id,
                        Message::AppendEntriesResp {
                            term: self.current_term,
                            success: false,
                            conflict_term: None,
                            conflict_index: None,
                        },
                    );
                } else {
                    // A legitimate leader exists: suppress our election and
                    // follow it. Log consistency lands at M3.4.
                    if self.role != Role::Follower {
                        self.role = Role::Follower;
                    }
                    self.reset_election_timer();
                    // M3.4: consistency check + append + commit update + success reply.
                    let _ = leader_id;
                }
            }
            Message::AppendEntriesResp { .. } => {
                // Replication bookkeeping at M3.4.
            }
            Message::InstallSnapshot { .. } | Message::InstallSnapshotResp { .. } => {
                // Snapshots at M8.
            }
        }
        self.drain_actions()
    }

    /// Drains everything pending into a `Ready`. Execution order for the
    /// caller: persist entries, persist hard state, send messages, apply
    /// committed — disk before network (§1.5).
    pub fn ready(&mut self) -> Ready {
        let mut ready = Ready::default();
        for action in self.outbox.drain(..) {
            match action {
                Action::Send { to, msg } => ready.messages.push((to, msg)),
                Action::PersistEntries(entries) => ready.entries.extend(entries),
                Action::PersistHardState(hs) => ready.hard_state = Some(hs),
                Action::ApplyEntries { up_to } => {
                    let from = self.last_applied + 1;
                    if up_to >= from {
                        let entries = self.storage.entries(from, up_to + 1).expect("raft storage");
                        ready.committed.extend(entries);
                        self.last_applied = up_to;
                    }
                }
            }
        }
        ready
    }

    /// The two universal term rules (§1.5), applied before any other handling.
    /// Returns true if the message's term was higher and we stepped down.
    fn observe_higher_term(&mut self, term: Term) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes_received.clear();
        self.reset_election_timer();
        self.persist_hard_state();
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = random_timeout(&mut self.rng, self.config.election_timeout);
    }

    fn start_election(&mut self) {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.config.id);
        self.votes_received.clear();
        self.votes_received.insert(self.config.id);
        self.reset_election_timer();
        self.persist_hard_state();

        let (last_index, last_term) = last_log(&self.storage);
        let msg = Message::RequestVote {
            term: self.current_term,
            candidate_id: self.config.id,
            last_log_index: last_index,
            last_log_term: last_term,
        };
        for peer in self.config.peers.clone() {
            self.send(peer, msg.clone());
        }

        if self.votes_received.len() >= self.config.quorum() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        let (last_index, _) = last_log(&self.storage);
        for peer in self.config.peers.clone() {
            self.next_index.insert(peer, last_index + 1);
            self.match_index.insert(peer, 0);
        }
        self.heartbeat_elapsed = 0;
        // M3.5 appends the no-op entry here.
        self.broadcast_heartbeats();
    }

    fn broadcast_heartbeats(&mut self) {
        let (last_index, last_term) = last_log(&self.storage);
        for peer in self.config.peers.clone() {
            self.send(
                peer,
                Message::AppendEntries {
                    term: self.current_term,
                    leader_id: self.config.id,
                    prev_log_index: last_index,
                    prev_log_term: last_term,
                    entries: vec![],
                    leader_commit: self.commit_index,
                },
            );
        }
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        candidate_term: Term,
        candidate_id: NodeId,
        candidate_last_index: LogIndex,
        candidate_last_term: Term,
    ) {
        let (voter_last_index, voter_last_term) = last_log(&self.storage);
        let granted = should_grant_vote(
            &CandidateInfo {
                term: candidate_term,
                id: candidate_id,
                last_term: candidate_last_term,
                last_index: candidate_last_index,
            },
            &VoterState {
                current_term: self.current_term,
                voted_for: self.voted_for,
                last_term: voter_last_term,
                last_index: voter_last_index,
            },
        );
        if granted {
            // Persist the vote before responding: a crash between grant and
            // persist would let this node vote twice in one term.
            self.voted_for = Some(candidate_id);
            self.persist_hard_state();
            if self.role != Role::Follower {
                self.role = Role::Follower;
            }
            self.reset_election_timer();
        }
        self.send(
            from,
            Message::RequestVoteResp { term: self.current_term, vote_granted: granted },
        );
    }

    fn handle_request_vote_resp(&mut self, from: NodeId, resp_term: Term, granted: bool) {
        if self.role != Role::Candidate || resp_term != self.current_term {
            return;
        }
        if granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.config.quorum() {
                self.become_leader();
            }
        }
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Action::Send { to, msg });
    }

    fn persist_hard_state(&mut self) {
        let hs = HardState {
            term: self.current_term,
            voted_for: self.voted_for,
            commit_index: self.commit_index,
        };
        self.storage.save_hard_state(&hs).expect("raft storage");
        self.outbox.push(Action::PersistHardState(hs));
    }

    #[allow(dead_code)]
    fn persist_entries(&mut self, entries: Vec<Entry>) {
        if entries.is_empty() {
            return;
        }
        self.storage.append(&entries).expect("raft storage");
        self.outbox.push(Action::PersistEntries(entries));
    }

    fn drain_actions(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.outbox)
    }
}

fn random_timeout(rng: &mut StdRng, base: u64) -> u64 {
    rng.gen_range(base..base * 2)
}

#[cfg(test)]
#[path = "tests/node_tick.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/request_vote.rs"]
mod request_vote_tests;
