use crate::message::{Config, Message, Ready};
use crate::types::{Entry, HardState};

fn sample_entry() -> Entry {
    Entry { term: 2, index: 9, command: b"cmd".to_vec() }
}

#[test]
fn every_message_variant_round_trips_through_bincode() {
    let messages = vec![
        Message::RequestVote { term: 3, candidate_id: 1, last_log_index: 7, last_log_term: 2 },
        Message::RequestVoteResp { term: 3, vote_granted: true },
        Message::AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 7,
            prev_log_term: 2,
            entries: vec![sample_entry()],
            leader_commit: 5,
            read_round: None,
        },
        Message::AppendEntriesResp {
            term: 3,
            success: false,
            match_index: 0,
            conflict_term: Some(2),
            conflict_index: Some(4),
            read_round: None,
        },
        Message::InstallSnapshot {
            term: 3,
            leader_id: 1,
            last_included_index: 50,
            last_included_term: 2,
            data: vec![7; 16],
        },
        Message::InstallSnapshotResp { term: 3, success: true },
    ];

    for msg in &messages {
        let decoded: Message = bincode::deserialize(&bincode::serialize(msg).unwrap()).unwrap();
        assert_eq!(&decoded, msg);
        assert_eq!(decoded.term(), 3);
    }
}

#[test]
fn ready_holds_entries_hard_state_and_messages_together() {
    let entry = sample_entry();
    let hs = HardState { term: 3, voted_for: None, commit_index: 9 };
    let ready = Ready {
        messages: vec![(2, Message::RequestVoteResp { term: 3, vote_granted: true })],
        entries: vec![entry.clone()],
        hard_state: Some(hs),
        committed: vec![entry],
        read_states: vec![],
    };

    assert!(!ready.is_empty());
    assert_eq!(ready.entries.len(), 1);
    assert_eq!(ready.hard_state, Some(hs));
    assert!(Ready::default().is_empty());
}

#[test]
fn single_node_config_has_quorum_one() {
    let config =
        Config { id: 1, peers: vec![], election_timeout: 10, heartbeat_interval: 2, seed: 0 };
    assert_eq!(config.cluster_size(), 1);
    assert_eq!(config.quorum(), 1);
    assert_eq!(config.all_nodes(), vec![1]);
}

#[test]
fn three_node_config_has_quorum_two() {
    let config =
        Config { id: 1, peers: vec![2, 3], election_timeout: 10, heartbeat_interval: 2, seed: 0 };
    assert_eq!(config.cluster_size(), 3);
    assert_eq!(config.quorum(), 2);
}
