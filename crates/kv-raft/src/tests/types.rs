use crate::membership::ClusterConfig;
use crate::types::{Entry, HardState, Snapshot};

#[test]
fn entry_round_trips_through_bincode() {
    let entry = Entry { term: 7, index: 42, command: vec![1, 2, 3] };
    let encoded = bincode::serialize(&entry).unwrap();
    let decoded: Entry = bincode::deserialize(&encoded).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn hard_state_round_trips_including_absent_vote() {
    for voted_for in [None, Some(3u64)] {
        let hs = HardState { term: 9, voted_for, commit_index: 12 };
        let decoded: HardState = bincode::deserialize(&bincode::serialize(&hs).unwrap()).unwrap();
        assert_eq!(hs, decoded);
    }
}

#[test]
fn default_hard_state_is_term_zero_no_vote() {
    let hs = HardState::default();
    assert_eq!(hs.term, 0);
    assert_eq!(hs.voted_for, None);
    assert_eq!(hs.commit_index, 0);
}

#[test]
fn snapshot_round_trips() {
    let snap = Snapshot {
        last_included_index: 100,
        last_included_term: 5,
        data: vec![9; 64],
        config: ClusterConfig::default(),
    };
    let decoded: Snapshot = bincode::deserialize(&bincode::serialize(&snap).unwrap()).unwrap();
    assert_eq!(snap, decoded);
}
