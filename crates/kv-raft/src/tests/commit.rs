use crate::harness::Cluster;
use crate::message::ProposeError;
use crate::types::Entry;

fn drain_committed(cluster: &mut Cluster) -> Vec<Vec<Entry>> {
    cluster.nodes.iter_mut().map(|n| n.ready().committed).collect()
}

#[test]
fn commits_at_majority_match_index() {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);

    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    assert_eq!(ldr.propose(b"a".to_vec()), Ok(2), "no-op takes index 1");
    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    assert_eq!(ldr.propose(b"b".to_vec()), Ok(3));

    for _ in 0..50 {
        cluster.tick_all();
    }

    for (i, committed) in drain_committed(&mut cluster).into_iter().enumerate() {
        assert_eq!(committed.len(), 3, "node {i} applied noop + both entries");
        assert!(committed[0].command.is_empty(), "node {i}: no-op applies first");
        assert_eq!(committed[1].command, b"a".to_vec());
        assert_eq!(committed[2].command, b"b".to_vec());
        assert_eq!(committed[1].index, 2);
        assert_eq!(committed[2].index, 3);
    }
}

#[test]
fn followers_reject_proposals() {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);
    let follower = cluster.nodes.iter().find(|n| n.id() != leader).unwrap().id();

    let res = cluster.nodes.iter_mut().find(|n| n.id() == follower).unwrap().propose(b"x".to_vec());
    assert_eq!(res, Err(ProposeError::NotLeader));
}

/// Paper figure 8: an entry from a previous term sits on a majority, but the
/// leader must NOT commit it by counting replicas alone. It becomes committed
/// only indirectly, once a current-term entry commits on top of it.
///
/// Faithful shape: the stale entry is replicated first while the leader's log
/// holds nothing newer; only afterwards does a current-term entry exist.
#[test]
fn figure_8_stale_term_entry_is_not_committed_by_count() {
    use crate::message::{Action, Message};

    let mut cluster = Cluster::of_n(&[1, 2, 3, 4, 5]);
    let leader = cluster.run_until_leader(1000);
    for node in cluster.nodes.iter_mut() {
        if node.id() != leader {
            node.set_election_timeout_for_tests(1_000_000);
        }
    }

    let node_idx =
        |cluster: &Cluster, id: u64| cluster.nodes.iter().position(|n| n.id() == id).unwrap();
    // One heartbeat round from the leader, delivering only to `targets`;
    // everything else is dropped, which Raft tolerates by design.
    let round = |cluster: &mut Cluster, targets: &[u64]| {
        let li = node_idx(cluster, leader);
        let mut sends = Vec::new();
        for _ in 0..2 {
            for action in cluster.nodes[li].tick() {
                if let Action::Send { to, msg } = action
                    && targets.contains(&to)
                {
                    sends.push((to, msg));
                }
            }
        }
        for (to, msg) in sends {
            assert!(matches!(msg, Message::AppendEntries { .. }));
            let ti = node_idx(cluster, to);
            let resps = cluster.nodes[ti].step(leader, msg);
            for r in resps {
                if let Action::Send { msg: m, .. } = r {
                    cluster.nodes[li].step(to, m);
                }
            }
        }
    };

    // The leader's log holds only stale entries; its term is newer than all
    // of them. (Reached by transplant + re-election: the election that
    // produces the term appends a no-op, which is removed again to isolate
    // the commit rule. Everybody's log is a term-1 prefix, so the re-election
    // is grantable regardless of earlier history.)
    {
        for node in cluster.nodes.iter_mut() {
            if node.id() == leader {
                node.replace_log_for_tests(vec![
                    Entry { term: 1, index: 1, command: b"old".to_vec() },
                    Entry { term: 1, index: 2, command: b"old".to_vec() },
                ]);
            } else {
                node.replace_log_for_tests(vec![Entry {
                    term: 1,
                    index: 1,
                    command: b"old".to_vec(),
                }]);
            }
        }
        // Force a new term the honest way: step down on a higher term, then
        // win it back with the freshest log.
        let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
        ldr.step(
            99,
            Message::RequestVote {
                term: 99,
                candidate_id: 99,
                last_log_index: 0,
                last_log_term: 0,
            },
        );
    }
    let li = node_idx(&cluster, leader);
    for _ in 0..10_000 {
        let mut solicited = false;
        for action in cluster.nodes[li].tick() {
            if let Action::Send { to, msg } = action {
                solicited = true;
                let ti = node_idx(&cluster, to);
                let resps = cluster.nodes[ti].step(leader, msg);
                for r in resps {
                    if let Action::Send { msg: m, .. } = r {
                        cluster.nodes[li].step(to, m);
                    }
                }
            }
        }
        if solicited && cluster.nodes[li].role() == crate::message::Role::Leader {
            break;
        }
    }
    let ldr = cluster.nodes.iter().find(|n| n.id() == leader).unwrap();
    assert_eq!(ldr.role(), crate::message::Role::Leader, "leader must win its new term");
    let term = ldr.current_term();
    assert!(term > 1);
    // Remove the election's no-op again: the rule under test concerns a
    // leader whose log holds nothing from its own term.
    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    ldr.replace_log_for_tests(vec![
        Entry { term: 1, index: 1, command: b"old".to_vec() },
        Entry { term: 1, index: 2, command: b"old".to_vec() },
    ]);

    // Phase 1: replicate the stale idx2 to two followers. {leader, f1, f2}
    // hold it — a majority — yet commit must not move. Two rounds: the first
    // is rejected past the followers' end and the hint walks `next` back.
    let others: Vec<u64> = [1, 2, 3, 4, 5].into_iter().filter(|p| *p != leader).collect();
    let (f1, f2) = (others[0], others[1]);
    round(&mut cluster, &[f1, f2]);
    round(&mut cluster, &[f1, f2]);
    let ldr = cluster.nodes.iter().find(|n| n.id() == leader).unwrap();
    assert_eq!(ldr.match_index_of(f1), Some(2));
    assert_eq!(ldr.match_index_of(f2), Some(2));
    assert_eq!(
        ldr.commit_index(),
        1,
        "stale-term idx2 on a majority must not advance the commit index"
    );

    // Phase 2: a current-term entry replicates; everything commits at once.
    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    assert_eq!(ldr.propose(b"new".to_vec()), Ok(3));
    round(&mut cluster, &[f1, f2]);
    round(&mut cluster, &[f1, f2]);
    let ldr = cluster.nodes.iter().find(|n| n.id() == leader).unwrap();
    assert_eq!(ldr.commit_index(), 3, "current-term commit carries the stale prefix with it");

    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    let applied: Vec<_> = ldr.ready().committed.into_iter().map(|e| e.command).collect();
    assert_eq!(applied, vec![b"old".to_vec(), b"old".to_vec(), b"new".to_vec()]);
}

#[test]
fn new_leader_appends_noop() {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);

    let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
    let ready = ldr.ready();
    let noop = ready.entries.iter().find(|e| e.command.is_empty());
    assert!(noop.is_some(), "new leader must append a no-op in its own term");
    let noop = noop.unwrap();
    assert_eq!(noop.term, ldr.current_term());
    assert_eq!(noop.index, 1);
}

#[test]
fn committed_entries_apply_in_order() {
    let mut cluster = Cluster::of_three();
    let leader = cluster.run_until_leader(500);
    {
        let ldr = cluster.nodes.iter_mut().find(|n| n.id() == leader).unwrap();
        ldr.propose(b"first".to_vec()).unwrap();
        ldr.propose(b"second".to_vec()).unwrap();
        ldr.propose(b"third".to_vec()).unwrap();
    }
    for _ in 0..50 {
        cluster.tick_all();
    }

    let mut seen_terms = None;
    for node in cluster.nodes.iter_mut() {
        let committed = node.ready().committed;
        assert_eq!(committed.len(), 4, "noop + 3 proposals, in order");
        let commands: Vec<_> = committed.iter().map(|e| e.command.clone()).collect();
        assert!(commands[0].is_empty(), "no-op applies first");
        assert_eq!(&commands[1..], &[b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]);
        for pair in committed.windows(2) {
            assert!(pair[0].index + 1 == pair[1].index);
        }
        let terms: Vec<_> = committed.iter().map(|e| e.term).collect();
        match &seen_terms {
            None => seen_terms = Some(terms),
            Some(t) => assert_eq!(t, &terms, "all replicas apply the same sequence"),
        }
    }
}
