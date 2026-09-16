//! The Raft role state machine (M3.2–M3.6). `RaftNode` owns its storage,
//! accepts `tick`/`step`/`propose`, and returns `Action`s describing what the
//! caller must do — persist entries, persist hard state, send messages, apply
//! committed entries — in that order (§1.5: disk before network).

use std::collections::{BTreeMap, BTreeSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::election::{CandidateInfo, VoterState, should_grant_vote};
use crate::log::{Consistency, check_consistency, last_log};
use crate::message::{Action, Config, Message, ProposeError, Ready, Role};
use crate::replication::backtrack;
use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, LogIndex, NodeId, Term};

pub struct RaftNode<S: RaftStorage> {
    config: Config,
    role: Role,
    current_term: Term,
    voted_for: Option<NodeId>,
    /// Who we currently believe leads this term, for `NotLeader` hints (M6).
    /// Distinct from `voted_for`: that is who we voted for, who often lost.
    /// A follower learns this only from an `AppendEntries` it accepts.
    leader_id: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    // BTreeSet, not HashSet: never iterated today, but M4's reproducibility
    // dies silently the day someone iterates a hash container in here.
    votes_received: BTreeSet<NodeId>,
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    /// Last log index covered by the most recent `AppendEntries` sent to each
    /// peer. Lets a success response advance `match_index` without changing
    /// the response shape (the leader knows what it sent).
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
            leader_id: None,
            commit_index: hs.commit_index,
            last_applied: 0,
            votes_received: BTreeSet::new(),
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

    /// Who this node believes currently leads, for `NotLeader` hints (M6).
    /// `None` on a fresh follower, during an election, and whenever a higher
    /// term has made the last known leader stale.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Borrow the storage, for a driver that must flush it before sending
    /// (§1.5). Read-only on purpose: mutating the log behind the core's back
    /// would desynchronise its `last_index`.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn match_index_of(&self, peer: NodeId) -> Option<LogIndex> {
        self.match_index.get(&peer).copied()
    }

    /// One logical-clock step. Followers/candidates count toward an election;
    /// leaders count toward a heartbeat.
    ///
    /// Returns this call's new actions and retains them: `ready()` drains the
    /// same queue. A driver must consume one interface or the other, not both.
    pub fn tick(&mut self) -> Vec<Action> {
        let checkpoint = self.outbox.len();
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                self.broadcast_heartbeats();
            }
            return self.new_actions_since(checkpoint);
        }

        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            self.start_election();
        }
        self.new_actions_since(checkpoint)
    }

    /// Routes one inbound message through the term rules first, then dispatch.
    /// `from` is the sender's id — vote and replication accounting must know
    /// who answered, and anonymous votes would be double-countable.
    ///
    /// Like `tick`, returns this call's new actions while retaining them for
    /// `ready()`: consume one interface or the other, not both.
    pub fn step(&mut self, from: NodeId, msg: Message) -> Vec<Action> {
        let checkpoint = self.outbox.len();
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
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            } => {
                if msg_term < self.current_term {
                    self.send(
                        leader_id,
                        Message::AppendEntriesResp {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                            conflict_term: None,
                            conflict_index: None,
                            read_round,
                        },
                    );
                } else {
                    // A legitimate leader exists: suppress our election and
                    // follow it before touching the log.
                    if self.role != Role::Follower {
                        self.role = Role::Follower;
                    }
                    self.leader_id = Some(leader_id);
                    self.reset_election_timer();
                    self.handle_append_entries(
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        read_round,
                    );
                }
            }
            Message::AppendEntriesResp {
                term: resp_term,
                success,
                match_index,
                conflict_term,
                conflict_index,
                read_round,
            } => {
                self.handle_append_entries_resp(
                    from,
                    resp_term,
                    success,
                    match_index,
                    conflict_term,
                    conflict_index,
                    read_round,
                );
            }
            Message::InstallSnapshot { .. } | Message::InstallSnapshotResp { .. } => {
                // Snapshots at M8.
            }
        }
        self.new_actions_since(checkpoint)
    }

    /// Appends a client command to the log. Only the leader accepts proposals;
    /// anything else is `Err(NotLeader)` — clients find the leader via hints
    /// (M6) or the sim's `leader()` (M4).
    pub fn propose(&mut self, cmd: Vec<u8>) -> Result<LogIndex, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }
        let (last_index, _) = last_log(&self.storage);
        let index = last_index + 1;
        self.persist_entries(vec![Entry { term: self.current_term, index, command: cmd }]);
        for peer in self.config.peers.clone() {
            self.send_append(peer);
        }
        // Same reason as `become_leader`: a group of one has already reached
        // quorum the moment the entry is on its own disk.
        self.try_advance_commit();
        Ok(index)
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
        // The leader we knew belonged to the old term; directing a client
        // there now would send it to a deposed node.
        self.leader_id = None;
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
        self.leader_id = None;
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
        self.leader_id = Some(self.config.id);
        let (last_index, _) = last_log(&self.storage);
        for peer in self.config.peers.clone() {
            self.next_index.insert(peer, last_index + 1);
            self.match_index.insert(peer, 0);
        }
        self.heartbeat_elapsed = 0;
        // A new leader appends a no-op in its own term. Everything before it
        // becomes committable indirectly (figure-8 rule), and reads can
        // proceed. Applied like any entry.
        self.persist_entries(vec![Entry {
            term: self.current_term,
            index: last_index + 1,
            command: Vec::new(),
        }]);
        self.broadcast_heartbeats();
        // A group of one is its own majority. Without this the lone leader
        // appends its no-op and never commits it, because the only other path
        // into the commit rule is an AppendEntries response that never
        // arrives. For any cluster of three or more this is a no-op: `count`
        // starts at 1 and quorum is 2+, so the rule's own guard declines.
        self.try_advance_commit();
    }

    fn broadcast_heartbeats(&mut self) {
        for peer in self.config.peers.clone() {
            self.send_append(peer);
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

    /// Follower-side log replication: consistency check, conflict truncation,
    /// append, commit update, reply. The Log Matching Property falls out of
    /// truncating at the first divergence: after this handler, our log is
    /// identical to the leader's through the last sent entry.
    fn handle_append_entries(
        &mut self,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        read_round: Option<u64>,
    ) {
        match check_consistency(&self.storage, prev_log_index, prev_log_term) {
            Consistency::Mismatch { conflict_term, conflict_index } => {
                self.send(
                    leader_id,
                    Message::AppendEntriesResp {
                        term: self.current_term,
                        success: false,
                        match_index: 0,
                        conflict_term,
                        conflict_index,
                        read_round,
                    },
                );
                return;
            }
            Consistency::Match => {}
        }

        let base = prev_log_index + 1;
        let mut first_new = entries.len();
        for (i, entry) in entries.iter().enumerate() {
            let idx = base + i as LogIndex;
            if self.storage.term(idx).expect("raft storage") != Some(entry.term) {
                first_new = i;
                break;
            }
        }
        if first_new < entries.len() {
            // §5.3: an existing entry *conflicts* — same index, different term
            // — so delete it and everything after it, then append the rest.
            self.storage.truncate_suffix(base + first_new as LogIndex).expect("raft storage");
            self.persist_entries(entries[first_new..].to_vec());
        }
        // Deliberately no `else`. Entries past what this message covers are not
        // in conflict with anything, and deleting them is not a tidy-up: they
        // may already be committed, and dropping them shrinks the majority that
        // holds a committed entry below a quorum. A later candidate that never
        // had it can then win, and Leader Completeness is gone. Found by M4's
        // seeded sweep, not by M3's own tests.

        // Clamp to the range this AppendEntries actually confirmed, not to our
        // whole log: a longer tail left over from an older leader carries no
        // commitment from *this* one. Monotonic — commit_index never retreats.
        let covered = prev_log_index + entries.len() as LogIndex;
        let confirmed = leader_commit.min(covered);
        if confirmed > self.commit_index {
            self.commit_index = confirmed;
            self.persist_hard_state();
            self.outbox.push(Action::ApplyEntries { up_to: self.commit_index });
        }

        self.send(
            leader_id,
            Message::AppendEntriesResp {
                term: self.current_term,
                match_index: covered,
                success: true,
                conflict_term: None,
                conflict_index: None,
                read_round,
            },
        );
    }

    /// Leader-side bookkeeping: advance `match_index` on success, backtrack
    /// and resend immediately on rejection.
    fn handle_append_entries_resp(
        &mut self,
        from: NodeId,
        resp_term: Term,
        success: bool,
        reported_match: LogIndex,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
        read_round: Option<u64>,
    ) {
        if self.role != Role::Leader || resp_term != self.current_term {
            return;
        }
        if success {
            // Monotonic: a delayed or duplicated reply to an older, shorter
            // AppendEntries must never walk match_index backwards.
            let matched = self.match_index.get(&from).copied().unwrap_or(0);
            if reported_match > matched {
                self.match_index.insert(from, reported_match);
                self.next_index.insert(from, reported_match + 1);
            }
            self.try_advance_commit();
        } else {
            let current_next = self.next_index.get(&from).copied().unwrap_or(1);
            let next = backtrack(current_next, conflict_term, conflict_index, &self.storage);
            self.next_index.insert(from, next.max(1));
            self.send_append(from);
        }
    }

    /// Leader commit rule (§1.5): advance to the highest N such that a
    /// majority holds N **and `log[N].term == current_term`**. The term check
    /// is the figure-8 rule — committing a previous-term entry by count alone
    /// lets a later leader overwrite it. Stale entries commit only indirectly,
    /// under a current-term entry.
    fn try_advance_commit(&mut self) {
        let last = self.storage.last_index().expect("raft storage");
        let mut target = self.commit_index;
        for n in (self.commit_index + 1)..=last {
            if self.storage.term(n).expect("raft storage") != Some(self.current_term) {
                continue;
            }
            let mut count = 1; // the leader holds its whole log
            for matched in self.match_index.values() {
                if *matched >= n {
                    count += 1;
                }
            }
            if count >= self.config.quorum() {
                target = n;
            }
        }
        if target > self.commit_index {
            self.commit_index = target;
            self.persist_hard_state();
            self.outbox.push(Action::ApplyEntries { up_to: target });
        }
    }

    /// Sends one `AppendEntries` covering everything from `next_index[to]`
    /// onward — empty when the follower is caught up, in which case it is a
    /// heartbeat. Records the covered end index for the success path above.
    fn send_append(&mut self, to: NodeId) {
        let next = self.next_index.get(&to).copied().unwrap_or(1);
        let prev = next.saturating_sub(1);
        let prev_term =
            if prev == 0 { 0 } else { self.storage.term(prev).expect("raft storage").unwrap_or(0) };
        let last = self.storage.last_index().expect("raft storage");
        let entries = if next <= last {
            self.storage.entries(next, last + 1).expect("raft storage")
        } else {
            Vec::new()
        };
        self.send(
            to,
            Message::AppendEntries {
                term: self.current_term,
                leader_id: self.config.id,
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
                read_round: None,
            },
        );
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

    /// This call's new actions, retained in the outbox for `ready()`.
    fn new_actions_since(&mut self, checkpoint: usize) -> Vec<Action> {
        self.outbox[checkpoint..].to_vec()
    }

    /// Consumes the node, returning its storage.
    ///
    /// This is what makes a simulated crash honest: everything the node held
    /// in memory — role, votes received, timers, next_index — is dropped, and
    /// only what reached `RaftStorage` survives. Restarting is then
    /// `RaftNode::new(config, storage)`, which restores term, vote and commit
    /// index from `HardState` and nothing else.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Every entry currently in the log. Inspection only — M4's invariant
    /// checker compares these across nodes to verify Log Matching.
    pub fn log_entries(&self) -> Vec<Entry> {
        self.storage.entries(1, LogIndex::MAX).expect("raft storage")
    }

    // The three hooks below force a node into a state that would take many
    // ticks to reach naturally (a divergent log, a stale next_index, a node
    // that never times out). Test-only; kv-sim may need them promoted to `pub`
    // for fault injection at M4.
    /// Installs a divergent log fixture (paper figure 7).
    #[cfg(test)]
    pub(crate) fn replace_log_for_tests(&mut self, entries: Vec<Entry>) {
        self.storage.truncate_suffix(1).expect("raft storage");
        if !entries.is_empty() {
            self.storage.append(&entries).expect("raft storage");
        }
    }

    /// Suppresses elections, so a leader stays put while a test drives it.
    #[cfg(test)]
    pub(crate) fn set_election_timeout_for_tests(&mut self, timeout: u64) {
        self.election_timeout = timeout;
        self.election_elapsed = 0;
    }

    /// Forces the next send position — e.g. past the follower's end, to
    /// exercise the conflict-hint path.
    #[cfg(test)]
    pub(crate) fn set_next_index_for_tests(&mut self, peer: NodeId, next: LogIndex) {
        self.next_index.insert(peer, next);
    }
}

fn random_timeout(rng: &mut StdRng, base: u64) -> u64 {
    rng.gen_range(base..base * 2)
}
