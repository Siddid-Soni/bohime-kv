//! The Raft role state machine (M3.2–M3.6). `RaftNode` owns its storage,
//! accepts `tick`/`step`/`propose`, and returns `Action`s describing what the
//! caller must do — persist entries, persist hard state, send messages, apply
//! committed entries — in that order (§1.5: disk before network).

use std::collections::{BTreeMap, BTreeSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::election::{CandidateInfo, VoterState, should_grant_vote};
use crate::log::{Consistency, check_consistency, last_log};
use crate::membership::{ClusterConfig, ConfChange, ConfProposeError, decode_conf, encode_conf};
use crate::message::{
    Action, Config, Message, ProposeError, ReadIndexError, ReadState, Ready, Role,
};
use crate::replication::backtrack;
use crate::storage::RaftStorage;
use crate::types::{Entry, HardState, LogIndex, NodeId, Snapshot, Term};

/// The fields of an `AppendEntriesResp`, passed as one value rather than as
/// seven positional arguments where transposing two of the `Option<u64>`s
/// would compile and be wrong.
struct AppendResponse {
    term: Term,
    success: bool,
    match_index: LogIndex,
    conflict_term: Option<Term>,
    conflict_index: Option<LogIndex>,
    read_round: Option<u64>,
}

/// The fields of an `InstallSnapshot`, for the same reason: eight positional
/// arguments with three `u64`s in a row is a transposition waiting to happen.
struct SnapshotInstall {
    msg_term: Term,
    leader_id: NodeId,
    last_included_index: LogIndex,
    last_included_term: Term,
    data: Vec<u8>,
    config: ClusterConfig,
}

pub struct RaftNode<S: RaftStorage> {
    config: Config,
    /// The live membership (M9). Starts as all of `config.peers` voting, then
    /// moves exclusively through conf log entries — never through flags or
    /// restarts. Every quorum, fan-out, and eligibility check reads this, not
    /// `config.peers`.
    cluster: ClusterConfig,
    /// The conf entry currently replicating, if any. At most one change is
    /// uncommitted at a time (single-server rule); proposing another is an
    /// error, not a queue.
    pending_conf: Option<LogIndex>,
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
    /// ReadIndex (M7), leader-only. `read_round` stamps outbound heartbeats so
    /// an ack can be attributed to the round that carried it; `round_acks`
    /// collects the peers that echoed the *current* round; `pending_reads`
    /// holds `(token, index)` until a quorum confirms. All are cleared on any
    /// role change — evidence of leadership does not survive losing it.
    read_round: u64,
    round_acks: BTreeSet<NodeId>,
    pending_reads: Vec<(u64, LogIndex)>,
    /// Set by `propose`, cleared by any broadcast. Defers replication to the
    /// next `ready()` so concurrent writers share one AppendEntries per peer.
    replication_pending: bool,
    /// Reads that arrived while a round was already outstanding. They cannot
    /// join it — it was broadcast before they existed, so its acks say nothing
    /// about leadership at *their* request time — so they wait and share the
    /// next one. This is what keeps a burst of readers costing one extra round
    /// rather than one round each: before it, every read bumped `read_round`
    /// and cleared `round_acks`, discarding the confirmation the previous
    /// reader was waiting on, and a steady read rate starved every round.
    queued_reads: Vec<(u64, LogIndex)>,
    /// Reads whose quorum has been confirmed, waiting to be drained by
    /// `ready()`. Kept apart from the `Action` outbox because a read is not
    /// work for the caller to perform — it is an answer.
    confirmed_reads: Vec<ReadState>,
    /// Last log index covered by the most recent `AppendEntries` sent to each
    /// peer. Lets a success response advance `match_index` without changing
    /// the response shape (the leader knows what it sent).
    /// The snapshot index last shipped to each peer (M8). An
    /// `InstallSnapshotResp` carries no index, so the leader remembers what it
    /// sent to advance `next_index`/`match_index` on success.
    sent_snapshot: BTreeMap<NodeId, LogIndex>,
    storage: S,
    rng: StdRng,
    outbox: Vec<Action>,
}

impl<S: RaftStorage> RaftNode<S> {
    pub fn new(config: Config, storage: S) -> Self {
        let hs: HardState = storage.hard_state().expect("raft storage");
        let mut rng = StdRng::seed_from_u64(config.seed);
        let election_timeout = random_timeout(&mut rng, config.election_timeout);
        // The log is the source of truth for membership (M9): start from the
        // snapshot's config for the compacted prefix, then replay conf entries
        // in the live tail. A restart with different flags cannot change the
        // quorum out from under committed entries.
        //
        // The bootstrap config only matters on a genuinely fresh store (no
        // snapshot, empty log): a founding voter joins the voters, a joining
        // learner starts learning. After that the log owns the membership and
        // the flags are history.
        let cluster = storage
            .snapshot()
            .expect("raft storage")
            .map(|s| s.config)
            .unwrap_or_else(|| initial_cluster(&config));
        let mut node = Self {
            config,
            cluster,
            pending_conf: None,
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
            read_round: 0,
            round_acks: BTreeSet::new(),
            pending_reads: Vec::new(),
            queued_reads: Vec::new(),
            replication_pending: false,
            confirmed_reads: Vec::new(),
            sent_snapshot: BTreeMap::new(),
            storage,
            rng,
            outbox: Vec::new(),
        };
        let first = node.storage.first_index().expect("raft storage");
        node.replay_conf(first);
        node
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn id(&self) -> NodeId {
        self.config.id
    }

    /// The live membership. The driver reads this to reconcile its peer
    /// connections and to stamp snapshots; it changes only through conf log
    /// entries, never through this handle.
    pub fn cluster_config(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Voter quorum of the live membership — the only denominator any commit,
    /// election, or read rule may use.
    fn quorum(&self) -> usize {
        self.cluster.quorum()
    }

    /// Every node this node replicates to and hears from: voters plus
    /// learners. Learners get entries, heartbeats, and snapshots; they just
    /// never count.
    fn replication_targets(&self) -> Vec<NodeId> {
        self.cluster
            .voters
            .iter()
            .chain(self.cluster.learners.iter())
            .copied()
            .filter(|p| *p != self.config.id)
            .collect()
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
            // Whatever a heartbeat on this tick did not already carry. `tick`
            // and `ready` drain the same queue, so a deferred proposal has to
            // surface in either — the test harness uses only this one.
            self.flush_replication();
            return self.new_actions_since(checkpoint);
        }

        self.flush_replication();
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
        // Before the message is handled, matching the old immediate broadcast:
        // a proposal accepted while we still led goes out under the term we
        // held then, whatever this message does to that.
        self.flush_replication();
        // A removed node (or a process that was never a member) must not be
        // able to depose a healthy leader by waving a high term — §1.7's
        // disruptive-server problem. So a **campaign** from outside the
        // membership is dropped before the term rules run, not after: it must
        // not force a step-down, reset an election timer, or win a vote.
        //
        // The gate is about votes and nothing else, and that boundary is
        // load-bearing in both directions:
        //
        // - Replication from an unknown sender is *accepted*. A node admitted
        //   to a running cluster starts knowing nobody — its membership lives
        //   in a log it has not received yet — so the leader catching it up is
        //   a stranger by its own reckoning. Dropping those messages makes the
        //   join impossible, since they are the only way the membership ever
        //   arrives. Safety is untouched: `AppendEntries` is accepted only on
        //   a matching log prefix, and election safety already says nobody
        //   holds a term they did not win a quorum for.
        // - A removed leader's handoff arrives from outside the membership by
        //   construction — it was just removed — so an equal-term `TimeoutNow`
        //   is honoured. The exact term match is its authenticity, and all it
        //   can trigger is a campaign by a voter of this node's own cluster.
        //   A stale one, or a future term no legitimate handoff carries, falls
        //   through to the gate.
        let vote_traffic = match &msg {
            // A campaign, and the answers to one. Exactly what a node outside
            // the membership must not be able to start or decide.
            Message::RequestVote { .. } | Message::RequestVoteResp { .. } => true,
            // A handoff at our exact term is authentic by construction;
            // anything else claiming to be one is not.
            Message::TimeoutNow { term, .. } => *term != self.current_term,
            // Replication, snapshots, and their answers.
            _ => false,
        };
        if vote_traffic && !self.cluster.contains(from) {
            return self.new_actions_since(checkpoint);
        }
        if let Message::TimeoutNow { term: handoff_term, .. } = &msg
            && *handoff_term == self.current_term
        {
            self.handle_timeout_now(from, *handoff_term);
            return self.new_actions_since(checkpoint);
        }
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
                        self.abandon_reads();
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
                    AppendResponse {
                        term: resp_term,
                        success,
                        match_index,
                        conflict_term,
                        conflict_index,
                        read_round,
                    },
                );
            }
            Message::InstallSnapshot {
                term: msg_term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
                config,
            } => {
                self.handle_install_snapshot(
                    from,
                    SnapshotInstall {
                        msg_term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        data,
                        config,
                    },
                );
            }
            Message::InstallSnapshotResp { term: resp_term, success } => {
                self.handle_install_snapshot_resp(from, resp_term, success);
            }
            Message::TimeoutNow { term: msg_term, .. } => {
                self.handle_timeout_now(from, msg_term);
            }
            // A transport ack, not a protocol message: nothing to do. (It
            // arrives here only in tests that step every message; the driver
            // consumes RPC responses through the reply channel instead.)
            Message::TimeoutNowResp { .. } => {}
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
        // Replication is deferred to `ready()` rather than broadcast here, so
        // a burst of proposals ships one AppendEntries per peer instead of one
        // per proposal. `send_append` always sends from `next_index`, so the
        // flush carries everything that accumulated — the immediate broadcasts
        // were each a superset of the one before, and only the last mattered.
        //
        // Nothing waits on the flush: a proposal that never meets a `ready()`
        // still goes out on the next heartbeat, from `next_index` as always.
        self.replication_pending = true;
        // Same reason as `become_leader`: a group of one has already reached
        // quorum the moment the entry is on its own disk.
        self.try_advance_commit();
        Ok(index)
    }

    /// Appends a membership change to the log (M9). Only the leader accepts
    /// them, at most one goes uncommitted at a time, and the change takes
    /// effect on append — the entry commits under the *new* config. The
    /// driver routes these from the admin path, never from client writes.
    pub fn propose_conf_change(
        &mut self,
        change: ConfChange,
    ) -> Result<LogIndex, ConfProposeError> {
        if self.role != Role::Leader {
            return Err(ConfProposeError::NotLeader);
        }
        if self.pending_conf.is_some_and(|i| i > self.commit_index) {
            return Err(ConfProposeError::ConfInFlight);
        }
        // Dry-run first: an invalid change is refused before it touches the
        // log, so the log never carries a conf entry every replica would have
        // to agree to ignore.
        self.cluster.apply(&change)?;
        let (last_index, _) = last_log(&self.storage);
        let index = last_index + 1;
        self.persist_entries(vec![Entry {
            term: self.current_term,
            index,
            command: encode_conf(&change),
        }]);
        self.replication_pending = true;
        self.try_advance_commit();
        Ok(index)
    }

    /// Begins a linearizable read (M7, §1.10).
    ///
    /// Records the current commit index, then broadcasts a round-stamped
    /// heartbeat to confirm this node still leads. When a quorum echoes that
    /// round, the read surfaces in `Ready::read_states` and the caller may
    /// serve it once it has applied up to that index. No disk write, one
    /// network round trip — which is the entire point of ReadIndex over
    /// pushing the read through the log.
    ///
    /// A read that is never confirmed never appears. The caller times it out;
    /// the core has no clock.
    pub fn read_index(&mut self, token: u64) -> Result<(), ReadIndexError> {
        if self.role != Role::Leader {
            return Err(ReadIndexError::NotLeader);
        }
        // §1.5's figure-8 rule in read form: a commit index still pointing
        // into a previous term is not evidence this leader holds everything it
        // must. The no-op appended on election is what clears this.
        if self.storage.term(self.commit_index).expect("raft storage") != Some(self.current_term) {
            return Err(ReadIndexError::NoQuorumInTerm);
        }

        // A round already outstanding is one this read cannot use, but it is
        // also one it must not cancel. Queue behind it and share the round
        // that opens when it settles.
        if self.pending_reads.is_empty() {
            self.pending_reads.push((token, self.commit_index));
            self.start_read_round();
            // A group of one is already a quorum; for anything larger this
            // declines and the peers' acks decide.
            self.try_confirm_reads();
        } else {
            self.queued_reads.push((token, self.commit_index));
        }
        Ok(())
    }

    /// Drains everything pending into a `Ready`. Execution order for the
    /// caller: persist entries, persist hard state, send messages, apply
    /// committed — disk before network (§1.5).
    pub fn ready(&mut self) -> Ready {
        self.flush_replication();
        let mut ready =
            Ready { read_states: std::mem::take(&mut self.confirmed_reads), ..Ready::default() };
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
                Action::ApplySnapshot(snap) => {
                    // At most one install per drain; a second would mean two
                    // snapshots installed without the caller restoring between
                    // them, which cannot happen — one message, one handler.
                    debug_assert!(ready.snapshot.is_none());
                    ready.snapshot = Some(snap);
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
        self.abandon_reads();
        self.votes_received.clear();
        self.reset_election_timer();
        self.persist_hard_state();
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = random_timeout(&mut self.rng, self.config.election_timeout);
    }

    fn start_election(&mut self) {
        // Learners never campaign: they hold no vote, count toward no quorum,
        // and winning would mean leading a group whose majority never chose
        // them. They wait to be promoted instead.
        if !self.cluster.is_voter(self.config.id) {
            self.reset_election_timer();
            return;
        }
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.config.id);
        self.votes_received.clear();
        self.votes_received.insert(self.config.id);
        self.leader_id = None;
        self.abandon_reads();
        self.reset_election_timer();
        self.persist_hard_state();

        let (last_index, last_term) = last_log(&self.storage);
        let msg = Message::RequestVote {
            term: self.current_term,
            candidate_id: self.config.id,
            last_log_index: last_index,
            last_log_term: last_term,
        };
        // Voters only: asking a learner is asking someone who must refuse.
        // Collected first: the send borrows mutably and must not hold the
        // voter set across it.
        let voters: Vec<NodeId> =
            self.cluster.voters.iter().copied().filter(|p| *p != self.config.id).collect();
        for peer in voters {
            self.send(peer, msg.clone());
        }

        if self.votes_received.len() >= self.quorum() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        let (last_index, _) = last_log(&self.storage);
        for peer in self.replication_targets() {
            self.next_index.insert(peer, last_index + 1);
            self.match_index.insert(peer, 0);
        }
        self.sent_snapshot.clear();
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
        // A broadcast sends from `next_index` to every peer, so it already
        // carries whatever `propose` deferred: it *is* the flush.
        self.replication_pending = false;
        for peer in self.replication_targets() {
            self.send_append(peer);
        }
    }

    /// Ships everything proposed since the last broadcast, one AppendEntries
    /// per peer. A node that lost leadership mid-batch replicates nothing —
    /// the entries are no longer its to send.
    fn flush_replication(&mut self) {
        if self.replication_pending && self.role == Role::Leader {
            self.broadcast_heartbeats();
        }
        self.replication_pending = false;
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
        // Membership first, log second: learners never grant (their vote
        // counts nowhere), and nobody grants a non-voter — a removed node
        // must not assemble a majority from politeness.
        let granted = self.cluster.is_voter(self.config.id)
            && self.cluster.is_voter(candidate_id)
            && should_grant_vote(
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
            // Voters only: a learner's encouragement is not a vote, and a
            // removed node's is not either. Counting either would elect a
            // leader no quorum chose.
            if !self.cluster.is_voter(from) {
                return;
            }
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.quorum() {
                self.become_leader();
            }
        }
    }

    /// Campaigns at once on a leader's handoff (M9). Only a voter may take it
    /// up, and only while not leading — a leader receiving its own handoff
    /// back (duplicated message) must not depose itself. Answers with a
    /// transport ack either way: the RPC plumbing needs a response variant,
    /// and silence would read as a dead peer.
    fn handle_timeout_now(&mut self, from: NodeId, msg_term: Term) {
        if msg_term == self.current_term
            && self.role != Role::Leader
            && self.cluster.is_voter(self.config.id)
        {
            self.start_election();
        }
        self.send(from, Message::TimeoutNowResp { term: self.current_term });
    }

    /// Follower-side log replication: consistency check, conflict truncation,
    /// append, commit update, reply. The Log Matching Property falls out of
    /// truncating at the first divergence: after this handler, our log is
    /// identical to the leader's through the last sent entry.
    fn handle_append_entries(
        &mut self,
        leader_id: NodeId,
        mut prev_log_index: LogIndex,
        mut prev_log_term: Term,
        mut entries: Vec<Entry>,
        leader_commit: LogIndex,
        read_round: Option<u64>,
    ) {
        // The leader has not compacted but we have: everything at or below
        // our snapshot is settled state, so entries covered by it are already
        // applied and only the tail beyond it can be new. Rebase onto the
        // snapshot boundary instead of rejecting on terms we no longer store.
        let covered_by_snapshot = self
            .storage
            .snapshot()
            .expect("raft storage")
            .map(|s| s.last_included_index)
            .unwrap_or(0);
        let mut covered_floor = 0;
        if prev_log_index < covered_by_snapshot {
            let skip = (covered_by_snapshot - prev_log_index) as usize;
            if skip >= entries.len() {
                // All covered: confirm through the snapshot, not through the
                // stale prefix, so the leader's `next_index` jumps past it.
                covered_floor = covered_by_snapshot;
            } else {
                let snap_term = self
                    .storage
                    .snapshot()
                    .expect("raft storage")
                    .map(|s| s.last_included_term)
                    .unwrap_or(0);
                entries = entries[skip..].to_vec();
                prev_log_index = covered_by_snapshot;
                prev_log_term = snap_term;
            }
        }
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
            // Truncation can drop an uncommitted conf entry, which already
            // moved the membership on append: the cluster is recomputed from
            // the surviving prefix so it cannot disagree with the log.
            self.truncate_suffix(base + first_new as LogIndex);
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
        // When the whole message fell inside our snapshot, the boundary is the
        // confirmation — it is what the leader must advance past.
        let covered = (prev_log_index + entries.len() as LogIndex).max(covered_floor);
        let confirmed = leader_commit.min(covered);
        if confirmed > self.commit_index {
            self.commit_index = confirmed;
            self.persist_hard_state();
            self.outbox.push(Action::ApplyEntries { up_to: self.commit_index });
            self.clear_committed_conf();
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
    /// and resend immediately on rejection, and count the ack toward any
    /// outstanding ReadIndex round.
    fn handle_append_entries_resp(&mut self, from: NodeId, resp: AppendResponse) {
        let AppendResponse {
            term: resp_term,
            success,
            match_index: reported_match,
            conflict_term,
            conflict_index,
            read_round,
        } = resp;
        if self.role != Role::Leader || resp_term != self.current_term {
            return;
        }

        // ReadIndex confirmation. Only an ack echoing the *current* round
        // counts: one already in flight when the read arrived proves this node
        // led at some earlier instant, and it could have been deposed in
        // between. That is the stale read this whole mechanism removes, and it
        // is visible only under a partition. Voters only: a learner's echo is
        // not leadership evidence any quorum would accept.
        if read_round == Some(self.read_round)
            && !self.pending_reads.is_empty()
            && self.cluster.is_voter(from)
        {
            self.round_acks.insert(from);
            self.try_confirm_reads();
        }
        if success {
            // Monotonic: a delayed or duplicated reply to an older, shorter
            // AppendEntries must never walk match_index backwards.
            //
            // Learners are tracked exactly like voters. A leader that cannot
            // see how far a learner has got cannot tell when promoting it is
            // safe, and catch-up-then-promote is the entire reason learners
            // exist. What must not happen is *counting* a learner toward a
            // commit — that rule lives in `try_advance_commit`, which sums
            // voters only, and is where it belongs: one place decides
            // quorums.
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

    /// Follower-side snapshot install (M8). The snapshot is committed state, so
    /// everything through it is settled: the prefix is dropped, commit and
    /// last-applied jump to the boundary, and the driver restores its state
    /// machine from `Ready::snapshot` instead of a stream of entries.
    fn handle_install_snapshot(&mut self, from: NodeId, install: SnapshotInstall) {
        let SnapshotInstall {
            msg_term,
            leader_id,
            last_included_index,
            last_included_term,
            data,
            config,
        } = install;
        if msg_term < self.current_term {
            self.send(
                from,
                Message::InstallSnapshotResp { term: self.current_term, success: false },
            );
            return;
        }
        if self.role != Role::Follower {
            self.role = Role::Follower;
            self.abandon_reads();
        }
        self.leader_id = Some(leader_id);
        self.reset_election_timer();
        if last_included_index <= self.commit_index {
            // Already applied past this: ack so the leader advances, install
            // nothing.
            self.send(
                from,
                Message::InstallSnapshotResp { term: self.current_term, success: true },
            );
            return;
        }
        let snap = Snapshot { last_included_index, last_included_term, data, config };
        self.storage.save_snapshot(&snap).expect("raft storage");
        self.storage.truncate_prefix(last_included_index).expect("raft storage");
        self.commit_index = last_included_index;
        self.last_applied = self.last_applied.max(last_included_index);
        // The prefix carried the membership with it: restart from the
        // snapshot's config, then replay the surviving tail. Anything pending
        // past the boundary becomes pending again in the replay.
        self.cluster =
            self.storage.snapshot().expect("raft storage").map(|s| s.config).expect("just saved");
        self.pending_conf = None;
        let first = self.storage.first_index().expect("raft storage");
        self.replay_conf(first);
        self.persist_hard_state();
        self.outbox.push(Action::ApplySnapshot(snap));
        self.send(from, Message::InstallSnapshotResp { term: self.current_term, success: true });
    }

    /// Leader-side snapshot accounting (M8). The response carries no index,
    /// so the index recorded when the snapshot went out is what advances
    /// `match_index`/`next_index` — monotonically, like the append path.
    fn handle_install_snapshot_resp(&mut self, from: NodeId, resp_term: Term, success: bool) {
        if self.role != Role::Leader || resp_term != self.current_term {
            return;
        }
        if !success {
            self.send_append(from);
            return;
        }
        if let Some(sent) = self.sent_snapshot.remove(&from) {
            let matched = self.match_index.get(&from).copied().unwrap_or(0);
            if sent > matched && self.cluster.is_voter(from) {
                self.match_index.insert(from, sent);
            }
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            self.next_index.insert(from, next.max(sent + 1));
            self.try_advance_commit();
        }
        self.send_append(from);
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
            for (peer, matched) in self.match_index.iter() {
                // Voters only: learners replicate but never commit, or adding
                // a far-behind node would stall the group it was meant to join.
                if self.cluster.is_voter(*peer) && *matched >= n {
                    count += 1;
                }
            }
            if count >= self.quorum() {
                target = n;
            }
        }
        if target > self.commit_index {
            self.commit_index = target;
            self.persist_hard_state();
            self.outbox.push(Action::ApplyEntries { up_to: target });
            self.clear_committed_conf();
            self.step_down_if_removed();
        }
    }

    /// Sends one `AppendEntries` covering everything from `next_index[to]`
    /// onward — empty when the follower is caught up, in which case it is a
    /// heartbeat. Records the covered end index for the success path above.
    ///
    /// When `next_index[to]` points inside the compacted prefix (M8), there is
    /// no `prev_log_term` to send — the term is gone with the entries, and
    /// fabricating a `0` makes the follower reject forever. Sends the snapshot
    /// covering the prefix instead.
    fn send_append(&mut self, to: NodeId) {
        let next = self.next_index.get(&to).copied().unwrap_or(1);
        let first = self.storage.first_index().expect("raft storage");
        if next < first
            && let Some(snap) = self.storage.snapshot().expect("raft storage")
        {
            let index = snap.last_included_index;
            self.sent_snapshot.insert(to, index);
            self.send(
                to,
                Message::InstallSnapshot {
                    term: self.current_term,
                    leader_id: self.config.id,
                    last_included_index: index,
                    last_included_term: snap.last_included_term,
                    data: snap.data.clone(),
                    config: snap.config.clone(),
                },
            );
            return;
        }
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
                // Stamped only while a read is outstanding: an unstamped ack
                // must never be mistaken for confirmation of one.
                read_round: if self.pending_reads.is_empty() {
                    None
                } else {
                    Some(self.read_round)
                },
            },
        );
    }

    /// Confirms every outstanding read once this round has a quorum behind it.
    ///
    /// The `+ 1` is the leader itself, which trivially holds its own log — and
    /// it is what makes a group of one work. Without calling this from
    /// `read_index` too, a lone leader would wait forever for an ack no peer
    /// will ever send, exactly as the commit rule did before M6.
    /// A fresh round, and no credit carried over from the last one: only acks
    /// to heartbeats sent from here on prove leadership *now*. `send_append`
    /// stamps the round whenever a read is pending, so the periodic heartbeat
    /// re-carries it and a dropped broadcast recovers on the next tick.
    fn start_read_round(&mut self) {
        self.read_round += 1;
        self.round_acks.clear();
        self.broadcast_heartbeats();
    }

    /// Confirms the outstanding round's reads, then opens one round for
    /// whatever queued behind it. Loops rather than recurses because a
    /// one-node group reaches quorum inside `start_read_round`'s own call.
    fn try_confirm_reads(&mut self) {
        loop {
            if self.pending_reads.is_empty() {
                return;
            }
            if self.round_acks.len() + 1 < self.config.quorum() {
                return;
            }
            for (token, index) in self.pending_reads.drain(..) {
                self.confirmed_reads.push(ReadState { token, index });
            }
            self.round_acks.clear();
            if self.queued_reads.is_empty() {
                return;
            }
            // The queued reads all arrived before this round is broadcast, so
            // its acks prove leadership after every one of them was requested.
            self.pending_reads.append(&mut self.queued_reads);
            self.start_read_round();
        }
    }

    /// Drops every outstanding read. Called on any loss of leadership: a
    /// quorum ack confirms that we led when it was sent, which says nothing
    /// once we no longer lead.
    fn abandon_reads(&mut self) {
        self.pending_reads.clear();
        self.queued_reads.clear();
        self.round_acks.clear();
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
        // A conf entry takes effect the moment it is appended — on the leader
        // that proposed it and on every follower that stores it — not when it
        // commits (§1.7). Hooking the single append choke point covers propose,
        // replication, and re-append after conflict truncation uniformly.
        for entry in &entries {
            if let Some(change) = decode_conf(&entry.command) {
                self.apply_conf_appended(entry.index, &change);
            }
        }
        self.outbox.push(Action::PersistEntries(entries));
    }

    /// Discards the log suffix at `from` and recomputes the membership from
    /// the surviving prefix: truncation can drop an uncommitted conf entry
    /// that already moved the cluster on append, and the cluster must never
    /// disagree with the log it describes.
    fn truncate_suffix(&mut self, from: LogIndex) {
        self.storage.truncate_suffix(from).expect("raft storage");
        let base = self
            .storage
            .snapshot()
            .expect("raft storage")
            .map(|s| s.config)
            .unwrap_or_else(|| initial_cluster(&self.config));
        self.cluster = base;
        self.pending_conf = None;
        let first = self.storage.first_index().expect("raft storage");
        if from > first {
            self.replay_conf(first);
        }
    }

    /// Applies one appended conf entry to the live membership and tracks it
    /// as the uncommitted change. A malformed entry (one no valid proposal
    /// could have produced) is deterministically ignored — every replica sees
    /// the same bytes and reaches the same refusal.
    fn apply_conf_appended(&mut self, index: LogIndex, change: &ConfChange) {
        let Ok(next) = self.cluster.apply(change) else {
            return;
        };
        self.cluster = next;
        if index > self.commit_index {
            self.pending_conf = Some(index);
        }
        if self.role != Role::Leader {
            return;
        }
        match change.op {
            crate::membership::ConfOp::AddLearner | crate::membership::ConfOp::Promote => {
                // A new replication target needs tracking from this leader's
                // end at once, or it hears nothing until the next election.
                let (last, _) = last_log(&self.storage);
                self.next_index.entry(change.node).or_insert(last + 1);
                self.match_index.entry(change.node).or_insert(0);
                self.send_append(change.node);
            }
            crate::membership::ConfOp::RemoveVoter | crate::membership::ConfOp::RemoveLearner => {
                self.next_index.remove(&change.node);
                self.match_index.remove(&change.node);
                self.sent_snapshot.remove(&change.node);
            }
        }
    }

    /// Reapplies conf entries in `[from, last]` over the current cluster —
    /// after an install (base config replaced), a truncation (prefix
    /// recomputed), or at open (log replay). The latest uncommitted conf entry
    /// becomes the pending change again.
    fn replay_conf(&mut self, from: LogIndex) {
        let last = self.storage.last_index().expect("raft storage");
        if last >= from {
            for entry in self.storage.entries(from, last + 1).expect("raft storage") {
                if let Some(change) = decode_conf(&entry.command) {
                    if let Ok(next) = self.cluster.apply(&change) {
                        self.cluster = next;
                    }
                    if entry.index > self.commit_index {
                        self.pending_conf = Some(entry.index);
                    }
                }
            }
        }
    }

    /// A conf entry this node stored is now committed: the next change may
    /// proceed. Called at both commit-advance sites (leader rule and follower
    /// leader-commit tracking).
    fn clear_committed_conf(&mut self) {
        if self.pending_conf.is_some_and(|i| i <= self.commit_index) {
            self.pending_conf = None;
        }
    }

    /// A leader that just committed its own removal has no quorum left to
    /// lead: it steps down at once and hands the group to the most-caught-up
    /// voter, which campaigns immediately instead of waiting out a timeout.
    /// Without the handoff, removing the leader buys an election-timeout
    /// outage the operator never asked for.
    fn step_down_if_removed(&mut self) {
        if self.role != Role::Leader || self.cluster.is_voter(self.config.id) {
            return;
        }
        self.role = Role::Follower;
        self.leader_id = None;
        self.abandon_reads();
        self.votes_received.clear();
        self.reset_election_timer();
        let best =
            self.cluster.voters.iter().copied().max_by_key(|p| {
                (self.match_index.get(p).copied().unwrap_or(0), std::cmp::Reverse(*p))
            });
        if let Some(to) = best {
            self.send(
                to,
                Message::TimeoutNow { term: self.current_term, leader_id: self.config.id },
            );
        }
    }

    /// This call's new actions, retained in the outbox for `ready()`.
    fn new_actions_since(&mut self, checkpoint: usize) -> Vec<Action> {
        self.outbox[checkpoint..].to_vec()
    }

    /// Snapshots the prefix through `snapshot.last_included_index` (M8). The
    /// caller scanned the image from its own state machine, so unlike a
    /// received snapshot there is nothing to report — the prefix is dropped
    /// and the caller syncs storage before acting further.
    ///
    /// Returns whether the snapshot was taken. `false` means the caller asked
    /// for something incoherent: past the commit (unapplied state, not
    /// snapshottable) or at/below the existing base (a regression, never an
    /// advance). Both are refused rather than clamped — silently taking the
    /// wrong snapshot would diverge the log from the state it describes.
    pub fn take_snapshot(&mut self, snapshot: Snapshot) -> bool {
        let base = self.storage.first_index().expect("raft storage");
        if snapshot.last_included_index < base || snapshot.last_included_index > self.commit_index {
            return false;
        }
        self.storage.save_snapshot(&snapshot).expect("raft storage");
        self.storage.truncate_prefix(snapshot.last_included_index).expect("raft storage");
        self.last_applied = self.last_applied.max(snapshot.last_included_index);
        true
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
        self.truncate_suffix(1);
        if !entries.is_empty() {
            self.storage.append(&entries).expect("raft storage");
            self.replay_conf(1);
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

    /// Snapshots the prefix through `up_to` with an empty state image, then
    /// truncates it — what the driver does with a real scan at M8.3, minus the
    /// state machine. Test-only; the term comes from the stored entry so the
    /// boundary stays honest.
    #[cfg(test)]
    pub(crate) fn compact_prefix_for_tests(&mut self, up_to: LogIndex) {
        let term =
            self.storage.term(up_to).expect("raft storage").expect("compacted index must exist");
        let snap = Snapshot {
            last_included_index: up_to,
            last_included_term: term,
            data: Vec::new(),
            config: self.cluster.clone(),
        };
        self.storage.save_snapshot(&snap).expect("raft storage");
        self.storage.truncate_prefix(up_to).expect("raft storage");
    }
}

/// The membership of a genuinely fresh store: founding voters vote (plus
/// ourselves, since `peers` names everyone *else*), while a joining node
/// starts as the voters' learner.
fn initial_cluster(config: &Config) -> ClusterConfig {
    let mut voters: std::collections::BTreeSet<NodeId> = config.peers.iter().copied().collect();
    voters.remove(&config.id);
    if config.initial_learner {
        ClusterConfig { voters, learners: [config.id].into_iter().collect() }
    } else {
        voters.insert(config.id);
        ClusterConfig { voters, learners: std::collections::BTreeSet::new() }
    }
}

fn random_timeout(rng: &mut StdRng, base: u64) -> u64 {
    rng.gen_range(base..base * 2)
}
