//! Membership change at the core (M9): learners, single-server voter changes,
//! and the handoff. The transport and driver do not exist down here — nodes
//! are stepped directly, which is exactly what makes these the contract every
//! caller inherits.

use crate::membership::{
    ClusterConfig, ConfChange, ConfOp, ConfProposeError, decode_conf, encode_conf,
};
use crate::message::{Action, Config, Message, Role};
use crate::node::RaftNode;
use crate::storage::MemStorage;
use crate::types::{LogIndex, NodeId};

fn learner(node: NodeId) -> ConfChange {
    ConfChange { op: ConfOp::AddLearner, node, context: vec![] }
}

fn voted_config(ids: &[NodeId]) -> Config {
    Config {
        id: ids[0],
        peers: ids[1..].to_vec(),
        election_timeout: 10,
        heartbeat_interval: 2,
        seed: 1000 + ids[0],
        initial_learner: false,
    }
}

/// The magic prefix provably cannot collide with data: a data command is
/// either empty (the leader no-op) or starts with the `ctx: Option` tag byte,
/// `0x00` or `0x01` — never `0x52`.
#[test]
fn membership_prefix_cannot_collide_with_commands() {
    let change = learner(4);
    let encoded = encode_conf(&change);
    assert_eq!(decode_conf(&encoded), Some(change));
    assert_eq!(decode_conf(b""), None, "the leader no-op is not a conf entry");
    assert_eq!(decode_conf(&[0x00, 9, 9, 9]), None, "ctx: None starts 0x00");
    assert_eq!(decode_conf(&[0x01, 9, 9, 9]), None, "ctx: Some starts 0x01");
    assert_eq!(decode_conf(b"RCF9trailing-garbage"), None, "corrupt conf decodes to nothing");
}

/// Pure config math: each op moves exactly one member, and the voter set can
/// neither empty nor double-add.
#[test]
fn cluster_config_moves_one_member_at_a_time() {
    let base = ClusterConfig::voting([1, 2, 3]);
    assert_eq!(base.quorum(), 2);

    let with_learner = base.apply(&learner(4)).unwrap();
    assert_eq!(with_learner.quorum(), 2, "learners never change the denominator");
    assert!(with_learner.is_learner(4));

    let promoted =
        with_learner.apply(&ConfChange { op: ConfOp::Promote, node: 4, context: vec![] }).unwrap();
    assert_eq!(promoted.quorum(), 3);
    assert!(promoted.is_voter(4) && !promoted.is_learner(4));

    assert!(base.apply(&learner(2)).is_err(), "a voter cannot be re-added as learner");
    assert!(
        promoted.apply(&ConfChange { op: ConfOp::Promote, node: 2, context: vec![] }).is_err(),
        "a voter cannot be promoted"
    );
    let two = ClusterConfig::voting([1, 2]);
    assert!(two.apply(&ConfChange { op: ConfOp::RemoveVoter, node: 1, context: vec![] }).is_ok());
    let one = ClusterConfig::voting([1]);
    assert!(
        one.apply(&ConfChange { op: ConfOp::RemoveVoter, node: 1, context: vec![] }).is_err(),
        "the last voter cannot go"
    );
}

/// Four nodes stepped by hand: three voters plus a learner. The learner
/// replicates everything, commits nothing on its own, and is never needed for
/// a commit.
struct Net {
    nodes: Vec<RaftNode<MemStorage>>,
}

impl Net {
    fn four() -> Self {
        let mut nodes = Vec::new();
        for id in [1, 2, 3] {
            let peers: Vec<NodeId> = [1, 2, 3].into_iter().filter(|p| *p != id).collect();
            nodes.push(RaftNode::new(
                Config {
                    id,
                    peers,
                    election_timeout: 10,
                    heartbeat_interval: 2,
                    seed: 1000 + id,
                    initial_learner: false,
                },
                MemStorage::default(),
            ));
        }
        nodes.push(RaftNode::new(
            Config {
                id: 4,
                peers: vec![1, 2, 3],
                election_timeout: 10,
                heartbeat_interval: 2,
                seed: 1004,
                initial_learner: true,
            },
            MemStorage::default(),
        ));
        Self { nodes }
    }

    fn at(&mut self, id: NodeId) -> &mut RaftNode<MemStorage> {
        self.nodes.iter_mut().find(|n| n.id() == id).expect("known node")
    }

    /// One tick plus full message drain, skipping isolated nodes both ways.
    fn step_all(&mut self, isolated: &[NodeId]) {
        let mut pending = Vec::new();
        for node in self.nodes.iter_mut() {
            if isolated.contains(&node.id()) {
                continue;
            }
            let from = node.id();
            for action in node.tick() {
                if let Action::Send { to, msg } = action {
                    pending.push((from, to, msg));
                }
            }
        }
        for _ in 0..16 {
            if pending.is_empty() {
                return;
            }
            let mut next = Vec::new();
            for (from, to, msg) in pending.drain(..) {
                if isolated.contains(&from) || isolated.contains(&to) {
                    continue;
                }
                if let Some(node) = self.nodes.iter_mut().find(|n| n.id() == to) {
                    let me = node.id();
                    for action in node.step(from, msg) {
                        if let Action::Send { to, msg } = action {
                            next.push((me, to, msg));
                        }
                    }
                }
            }
            pending = next;
        }
    }

    fn run_until_leader(&mut self, isolated: &[NodeId]) -> NodeId {
        for _ in 0..500 {
            self.step_all(isolated);
            let leaders: Vec<NodeId> =
                self.nodes.iter().filter(|n| n.role() == Role::Leader).map(|n| n.id()).collect();
            if leaders.len() == 1 && !isolated.contains(&leaders[0]) {
                return leaders[0];
            }
        }
        panic!("no single unisolated leader");
    }
}

#[test]
fn learner_replicates_but_never_commits() {
    let mut net = Net::four();
    let leader = net.run_until_leader(&[]);
    assert_ne!(leader, 4, "a learner must never win");

    // The leader learns about the learner through a conf entry, like every
    // other membership fact. Until then it sends the learner nothing.
    let lid = leader;
    net.at(lid).propose_conf_change(learner(4)).unwrap();
    for _ in 0..30 {
        net.step_all(&[]);
    }
    assert!(net.at(lid).cluster_config().is_learner(4));
    // And the one-at-a-time rule holds: the committed change clears the way
    // for the next one.
    net.at(lid).propose_conf_change(learner(5)).expect("committed change clears the flight");

    // Partition the learner away: commits proceed without it, and its own
    // commit does not advance while isolated.
    let frozen = net.at(4).commit_index();
    net.at(lid).propose(b"a".to_vec()).unwrap();
    for _ in 0..30 {
        net.step_all(&[4]);
    }
    let commit = net.at(lid).commit_index();
    assert!(commit >= 2, "committed without the learner: {commit}");
    assert_eq!(net.at(4).commit_index(), frozen, "the isolated learner learns nothing");

    // Heal: the learner catches up from the leader that never counted it.
    for _ in 0..30 {
        net.step_all(&[]);
    }
    let leader_log = net.at(lid).log_entries();
    assert_eq!(net.at(4).log_entries(), leader_log, "the learner converges");
    assert_eq!(net.at(4).commit_index(), net.at(lid).commit_index());
}

#[test]
fn learner_neither_campaigns_nor_votes() {
    let mut lone = RaftNode::new(
        Config {
            id: 4,
            peers: vec![1, 2, 3],
            election_timeout: 10,
            heartbeat_interval: 2,
            seed: 1004,
            initial_learner: true,
        },
        MemStorage::default(),
    );
    for _ in 0..200 {
        for action in lone.tick() {
            assert!(
                !matches!(action, Action::Send { msg: Message::RequestVote { .. }, .. }),
                "a learner must never solicit votes"
            );
        }
    }
    assert_eq!(lone.role(), Role::Follower);

    let actions = lone.step(
        1,
        Message::RequestVote { term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0 },
    );
    let granted = actions.iter().find_map(|a| match a {
        Action::Send { msg: Message::RequestVoteResp { vote_granted, .. }, .. } => {
            Some(*vote_granted)
        }
        _ => None,
    });
    assert_eq!(granted, Some(false), "a learner grants no vote");
}

#[test]
fn votes_from_non_voters_are_ignored() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2]);
    // Node 1 campaigns; only node 2's answer may count.
    let n1 = cluster.nodes.iter().position(|n| n.id() == 1).unwrap();
    for _ in 0..30 {
        cluster.nodes[n1].tick();
        if cluster.nodes[n1].role() == Role::Candidate {
            break;
        }
    }
    assert_eq!(cluster.nodes[n1].role(), Role::Candidate);

    // A stranger's enthusiasm changes nothing.
    let term = cluster.nodes[n1].current_term();
    cluster.nodes[n1].step(99, Message::RequestVoteResp { term, vote_granted: true });
    assert_eq!(cluster.nodes[n1].role(), Role::Candidate, "stranger votes must not elect");

    // Neither does a learner's.
    cluster.nodes[n1].step(7, Message::RequestVoteResp { term, vote_granted: true });
    assert_eq!(cluster.nodes[n1].role(), Role::Candidate, "learner votes must not elect");
}

#[test]
fn stranger_messages_do_not_force_a_step_down() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2, 3]);
    let leader = cluster.run_until_leader(500);
    let term = cluster.nodes.iter().find(|n| n.id() == leader).unwrap().current_term();

    // A higher term from a node that was never a member: ignored entirely.
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    cluster.nodes[li].step(
        99,
        Message::RequestVote {
            term: term + 5,
            candidate_id: 99,
            last_log_index: 0,
            last_log_term: 0,
        },
    );
    assert_eq!(cluster.nodes[li].role(), Role::Leader, "strangers must not depose");
    assert_eq!(cluster.nodes[li].current_term(), term, "strangers must not advance the term");
}

#[test]
fn add_learner_then_promote_moves_the_quorum() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2, 3]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();

    let at = cluster.nodes[li].propose_conf_change(learner(4)).unwrap();
    assert_eq!(cluster.nodes[li].cluster_config().quorum(), 2, "learners never count");
    assert!(cluster.nodes[li].cluster_config().is_learner(4));
    for _ in 0..50 {
        cluster.tick_all();
    }
    assert!(cluster.nodes[li].commit_index() >= at, "the conf entry commits under voters");

    cluster.nodes[li]
        .propose_conf_change(ConfChange { op: ConfOp::Promote, node: 4, context: vec![] })
        .unwrap();
    assert_eq!(cluster.nodes[li].cluster_config().quorum(), 3, "four voters need three");
    assert!(cluster.nodes[li].cluster_config().is_voter(4));
}

#[test]
fn a_second_conf_change_while_one_is_uncommitted_is_rejected() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2, 3]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();

    cluster.nodes[li].propose_conf_change(learner(4)).unwrap();
    // No ticks: the first change is still in flight.
    let err = cluster.nodes[li].propose_conf_change(learner(5)).expect_err("one change at a time");
    assert_eq!(err, ConfProposeError::ConfInFlight);

    for _ in 0..50 {
        cluster.tick_all();
    }
    cluster.nodes[li].propose_conf_change(learner(5)).expect("the next change may proceed");
}

#[test]
fn invalid_conf_changes_are_refused_before_the_log() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();

    let last_before = cluster.nodes[li].log_entries().len();
    let err = cluster.nodes[li]
        .propose_conf_change(ConfChange { op: ConfOp::RemoveVoter, node: 9, context: vec![] })
        .expect_err("removing a stranger must fail");
    assert!(matches!(err, ConfProposeError::Invalid(_)));
    assert_eq!(
        cluster.nodes[li].log_entries().len(),
        last_before,
        "refused changes leave no trace"
    );

    let err = cluster.nodes[li]
        .propose_conf_change(ConfChange { op: ConfOp::Promote, node: 2, context: vec![] })
        .expect_err("a voter is not a learner");
    assert!(matches!(err, ConfProposeError::Invalid(_)));

    let follower = cluster.nodes.iter().find(|n| n.id() != leader).unwrap().id();
    let fi = cluster.nodes.iter().position(|n| n.id() == follower).unwrap();
    let err = cluster.nodes[fi].propose_conf_change(learner(7));
    assert_eq!(err, Err(ConfProposeError::NotLeader));
}

/// A truncated uncommitted conf entry must not leave the membership ahead of
/// the log: the cluster is recomputed from the surviving prefix.
#[test]
fn truncating_an_uncommitted_conf_entry_rolls_the_membership_back() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();

    cluster.nodes[li].propose_conf_change(learner(9)).unwrap();
    assert!(cluster.nodes[li].cluster_config().is_learner(9), "applied on append");

    // A rival history without the conf entry overwrites the suffix. The rival
    // history must genuinely lack the conf entry: same bytes under a new term
    // would re-apply it, correctly.
    use crate::membership::is_conf_change;
    let term = cluster.nodes[li].current_term();
    let last: Vec<crate::types::Entry> = cluster.nodes[li].log_entries();
    let overwritten: Vec<crate::types::Entry> = last
        .into_iter()
        .filter(|e| !is_conf_change(&e.command))
        .map(|mut e| {
            e.term = term + 1;
            e
        })
        .collect();
    assert!(!overwritten.is_empty(), "the no-op survives as the rival base");
    cluster.nodes[li].step(
        2,
        Message::AppendEntries {
            term: term + 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: overwritten,
            leader_commit: 0,
            read_round: None,
        },
    );
    assert!(
        !cluster.nodes[li].cluster_config().contains(9),
        "the truncated conf entry must un-apply"
    );
}

#[test]
fn removed_leader_steps_down_and_hands_off() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2, 3]);
    let leader = cluster.run_until_leader(500);
    for node in cluster.nodes.iter_mut() {
        if node.id() != leader {
            node.set_election_timeout_for_tests(1_000_000);
        }
    }
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    cluster.nodes[li].propose(b"x".to_vec()).unwrap();
    for _ in 0..50 {
        cluster.tick_all();
    }

    // Remove the leader itself. It must keep leading just long enough to
    // commit its own removal, then step down with a handoff — not linger, and
    // not vanish silently. The followers' timers are pinned, so the only way
    // anyone campaigns in the window below is the handoff itself.
    cluster.nodes[li]
        .propose_conf_change(ConfChange { op: ConfOp::RemoveVoter, node: leader, context: vec![] })
        .unwrap();
    // The removed leader must step down once its removal commits...
    let mut stepped_down = false;
    for _ in 0..60 {
        cluster.tick_all();
        if cluster.nodes[li].role() != Role::Leader {
            stepped_down = true;
            break;
        }
    }
    assert!(stepped_down, "a removed leader must step down");

    // ...and someone else must lead soon after. The followers' timers are
    // pinned at a million ticks, so a new leader inside this window cannot
    // come from a timeout — only from the handoff.
    let mut successor = None;
    for _ in 0..60 {
        cluster.tick_all();
        successor = cluster
            .nodes
            .iter()
            .find_map(|n| (n.id() != leader && n.role() == Role::Leader).then_some(n.id()));
        if successor.is_some() {
            break;
        }
    }
    let to = successor.expect("the handoff must produce a successor, timers being pinned");

    // The handoff itself travelled as a message: it sits in the removed
    // leader's drained outbox addressed to exactly that successor.
    let sent = cluster.nodes[li]
        .ready()
        .messages
        .into_iter()
        .any(|(dest, msg)| dest == to && matches!(msg, Message::TimeoutNow { .. }));
    assert!(sent, "the removal commits with a TimeoutNow handoff to the successor");
}

/// A restart rebuilds the membership from the snapshot base plus log replay —
/// never from flags alone. The flags seed a fresh store; after that the log
/// owns the membership, so reopening with the same flags restores every
/// change, and a snapshot carries the compacted prefix's config forward.
#[test]
fn restart_rebuilds_membership_from_the_log_not_the_flags() {
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    cluster.nodes[li].propose(b"x".to_vec()).unwrap();
    cluster.nodes[li].propose_conf_change(learner(3)).unwrap();
    for _ in 0..50 {
        cluster.tick_all();
    }

    let storage = std::mem::replace(
        &mut cluster.nodes[li],
        RaftNode::new(voted_config(&[leader]), MemStorage::default()),
    )
    .into_storage();
    // A normal restart (same flags, plus a stale extra that replay tolerates):
    // the learner survives via the log, not the flags.
    let reopened = RaftNode::new(voted_config(&[leader, 99]), storage);
    assert!(reopened.cluster_config().is_learner(3), "replay restores the learner");
    assert!(reopened.cluster_config().is_voter(leader));
    assert_eq!(reopened.cluster_config().quorum(), 2);

    // And a snapshot carries the same base: compact, reopen, replay the tail.
    let mut compacting = reopened;
    let last = compacting.log_entries().last().map(|e| e.index).unwrap_or(0);
    if last > 0 {
        compacting.compact_prefix_for_tests(last);
    }
    let storage = compacting.into_storage();
    let from_snapshot = RaftNode::new(voted_config(&[leader, 99]), storage);
    assert!(from_snapshot.cluster_config().is_learner(3), "the snapshot base restores the learner");
    assert_eq!(from_snapshot.cluster_config().quorum(), 2);
}

/// A learner's echo is not leadership evidence: a read confirms only on voter
/// acks, or adding a node would stall every linearizable read until it caught
/// up.
#[test]
fn read_quorum_ignores_learners() {
    let mut net = Net::four();
    let leader = net.run_until_leader(&[]);
    assert_ne!(leader, 4);
    net.at(leader).propose_conf_change(learner(4)).unwrap();
    for _ in 0..30 {
        net.step_all(&[]);
    }
    assert!(net.at(leader).cluster_config().is_learner(4));
    // The no-op committed under voters; reads may proceed.
    let ldr = net.at(leader);
    assert!(ldr.read_index(1).is_ok());
    let term = ldr.current_term();

    // Find the round the read opened by inspecting the next broadcast.
    let mut round = None;
    let ldr = net.at(leader);
    for action in ldr.tick() {
        if let Action::Send { to, msg: Message::AppendEntries { read_round: Some(r), .. } } = action
            && to == 4
        {
            round = Some(r);
        }
    }
    // Heartbeats tick every 2; the round stamp rides the next one regardless.
    let mut ticks = 0;
    while round.is_none() && ticks < 5 {
        let ldr = net.at(leader);
        for action in ldr.tick() {
            if let Action::Send { to, msg: Message::AppendEntries { read_round: Some(r), .. } } =
                action
                && to == 4
            {
                round = Some(r);
            }
        }
        ticks += 1;
    }
    let round = round.expect("a heartbeat carries the round");
    let last = net.at(leader).log_entries().last().map(|e| e.index).unwrap_or(0);

    // The learner echoes the round: nothing confirms, and its match index is
    // not even recorded.
    net.at(leader).step(
        4,
        Message::AppendEntriesResp {
            term,
            success: true,
            match_index: last,
            conflict_term: None,
            conflict_index: None,
            read_round: Some(round),
        },
    );
    assert!(
        net.at(leader).ready().read_states.is_empty(),
        "a learner's echo must not confirm a read"
    );
    // Tracked, but not counted. The learner's progress *is* recorded — that
    // is what tells a leader when promoting it would be safe (M9.2) — and it
    // still confirmed nothing above. Quorums are decided in one place, by
    // voter, not by which slots happen to be filled in.
    assert_eq!(net.at(leader).match_index_of(4), Some(last));

    // A voter echoing the same round confirms it.
    let voter = [1, 2, 3].into_iter().find(|v| *v != leader).unwrap();
    net.at(leader).step(
        voter,
        Message::AppendEntriesResp {
            term,
            success: true,
            match_index: last,
            conflict_term: None,
            conflict_index: None,
            read_round: Some(round),
        },
    );
    let states = net.at(leader).ready().read_states;
    assert_eq!(states.len(), 1, "one voter plus the leader is a quorum of three");
    assert_eq!(states[0].token, 1);
}

/// Conf entries travel the committed stream like any entry — the driver still
/// advances past them — but their bytes are conf, never data, so the state
/// machine must not evaluate them.
#[test]
fn committed_conf_entries_are_visible_but_not_data() {
    use crate::membership::is_conf_change;
    use crate::tests::harness::Cluster;

    let mut cluster = Cluster::of_n(&[1, 2]);
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    let at = cluster.nodes[li].propose_conf_change(learner(9)).unwrap();
    for _ in 0..50 {
        cluster.tick_all();
    }
    for node in cluster.nodes.iter_mut() {
        let committed = node.ready().committed;
        let indices: Vec<LogIndex> = committed.iter().map(|e| e.index).collect();
        assert!(indices.contains(&at), "node {} commits the conf entry", node.id());
        let entry = committed.into_iter().find(|e| e.index == at).unwrap();
        assert!(is_conf_change(&entry.command), "the conf entry commits as conf, never as data");
    }
}

/// A node admitted to a running cluster starts knowing nobody: its config is
/// itself as a learner and nothing else, because the membership it is about to
/// join lives in a log it has not received yet.
///
/// So the leader replicating to it is, by that node's own reckoning, a
/// stranger. Dropping its `AppendEntries` — which is what the blanket stranger
/// gate did — makes the join impossible: the only way to learn the membership
/// is over the very messages being dropped. The gate has to be about *votes*,
/// which is the disruption it exists to prevent (§1.7), not about replication.
///
/// Safety is unchanged: accepting an `AppendEntries` requires a matching log
/// prefix, and election safety already says a node cannot hold a term it did
/// not win a quorum for. A forged leader cannot produce entries a quorum
/// accepted.
#[test]
fn a_joining_learner_accepts_replication_from_a_leader_it_does_not_know_yet() {
    use crate::membership::encode_conf;
    use crate::message::{Action, Message};
    use crate::node::RaftNode;
    use crate::storage::MemStorage;
    use crate::tests::harness::config;
    use crate::types::Entry;

    let mut joining = RaftNode::new(
        Config { initial_learner: true, ..config(4, vec![], 7) },
        MemStorage::default(),
    );
    assert!(joining.cluster_config().voters.is_empty(), "a joiner knows nobody yet");

    // The entry that admits it, shipped by a leader it has never heard of.
    let admit = ConfChange { op: ConfOp::AddLearner, node: 4, context: b"4.example:7001".to_vec() };
    let actions = joining.step(
        1,
        Message::AppendEntries {
            term: 5,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![Entry { term: 5, index: 1, command: encode_conf(&admit) }],
            leader_commit: 0,
            read_round: None,
        },
    );

    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::Send { msg: Message::AppendEntriesResp { success: true, .. }, .. }
        )),
        "the joiner must accept and answer, or it can never catch up: {actions:?}"
    );
    assert_eq!(joining.current_term(), 5, "it adopts the leader's term");
    assert_eq!(joining.leader_id(), Some(1), "and knows who to redirect clients to");
}

/// The other half of the same rule: replication is accepted from an unknown
/// sender, *votes* are not. A stranger waving a high term must still not
/// depose anyone — that is the disruption single-server membership has to
/// prevent, and it is what `stranger_messages_do_not_force_a_step_down` pins.
#[test]
fn a_joining_learner_still_ignores_a_strangers_vote_request() {
    use crate::message::{Message, Role};
    use crate::node::RaftNode;
    use crate::storage::MemStorage;
    use crate::tests::harness::config;

    let mut joining = RaftNode::new(
        Config { initial_learner: true, ..config(4, vec![], 7) },
        MemStorage::default(),
    );

    joining.step(
        99,
        Message::RequestVote { term: 9, candidate_id: 99, last_log_index: 0, last_log_term: 0 },
    );

    assert_eq!(joining.current_term(), 0, "a stranger's campaign must not move our term");
    assert_eq!(joining.role(), Role::Follower);
}

/// A leader has to be able to tell when a learner has caught up, or it can
/// never decide that promoting it is safe — and the catch-up-then-promote
/// sequence is the whole reason learners exist.
///
/// So a learner's `match_index` is tracked like anyone else's. What must not
/// happen is *counting* it toward a commit, which is a separate rule enforced
/// where commits are decided (`learner_replicates_but_never_commits` pins
/// that end).
#[test]
fn a_leader_tracks_how_far_a_learner_has_caught_up() {
    let mut cluster = crate::tests::harness::Cluster::of_three();
    let leader = cluster.run_until_leader(500);
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();

    cluster.nodes[li].propose_conf_change(learner(4)).unwrap();
    for _ in 0..20 {
        cluster.tick_all();
    }
    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    ldr.propose(b"data".to_vec()).unwrap();

    // Node 4 is not in the harness, so play its side by hand: take what the
    // leader sends it and answer as a caught-up follower would.
    let li = cluster.nodes.iter().position(|n| n.id() == leader).unwrap();
    for _ in 0..10 {
        let sends: Vec<(NodeId, Message)> = cluster.nodes[li]
            .tick()
            .into_iter()
            .filter_map(|a| match a {
                Action::Send { to: 4, msg } => Some((4, msg)),
                _ => None,
            })
            .collect();
        for (_, msg) in sends {
            let Message::AppendEntries { term, prev_log_index, entries, .. } = msg else {
                continue;
            };
            let matched = prev_log_index + entries.len() as LogIndex;
            cluster.nodes[li].step(
                4,
                Message::AppendEntriesResp {
                    term,
                    success: true,
                    match_index: matched,
                    conflict_term: None,
                    conflict_index: None,
                    read_round: None,
                },
            );
        }
    }

    let ldr = cluster.nodes.iter().find(|n| n.id() == leader).unwrap();
    let matched = ldr.match_index_of(4).expect("a learner's progress is tracked");
    assert!(
        matched >= ldr.commit_index(),
        "the learner acked through {matched} but the leader sees no progress past {}",
        ldr.commit_index()
    );
}
