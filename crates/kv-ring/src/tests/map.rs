//! The versioned shard map (M10.3).

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use crate::map::{MapError, ShardMap};
use crate::ring::PlacementError;

const SHARDS: u16 = 256;
const RF: u8 = 3;
const VNODES: u32 = 128;

fn map_of(nodes: impl IntoIterator<Item = u64>) -> ShardMap {
    ShardMap::build(1, nodes, SHARDS, RF, VNODES).expect("buildable")
}

#[test]
fn a_map_has_one_replica_set_per_shard() {
    let map = map_of([1, 2, 3]);
    assert_eq!(map.shards.len(), SHARDS as usize);
    for shard in 0..SHARDS {
        assert_eq!(map.replicas(shard).len(), RF as usize);
    }
}

#[test]
fn build_refuses_a_replication_factor_the_cluster_cannot_meet() {
    assert_eq!(
        ShardMap::build(1, [1, 2], SHARDS, 3, VNODES),
        Err(MapError::Placement(PlacementError::NotEnoughNodes {
            replication_factor: 3,
            nodes: 2
        }))
    );
}

#[test]
fn build_refuses_a_cluster_with_no_shards() {
    assert_eq!(ShardMap::build(1, [1, 2, 3], 0, RF, VNODES), Err(MapError::ZeroShards));
}

#[test]
fn a_key_resolves_through_the_map_to_its_replicas() {
    let map = map_of([1, 2, 3, 4, 5]);
    let key = b"user:42";
    let shard = map.shard_for_key(key);
    assert_eq!(map.replicas_for_key(key), map.replicas(shard));
}

#[test]
fn the_map_records_its_own_node_set() {
    let map = map_of([1, 2, 3, 4, 5]);
    assert_eq!(map.nodes, BTreeSet::from([1, 2, 3, 4, 5]));
}

#[test]
fn rebuilding_for_a_new_node_set_bumps_the_version() {
    let first = map_of([1, 2, 3]);
    let second = first.with_nodes([1, 2, 3, 4]).expect("4 nodes hold 3 replicas");
    assert_eq!(second.version, first.version + 1);
    assert_eq!(second.nodes, BTreeSet::from([1, 2, 3, 4]));
    // The parameters are the map's, not the caller's — they are fixed at
    // bootstrap and a later rebuild must not quietly reinterpret them.
    assert_eq!(second.num_shards, first.num_shards);
    assert_eq!(second.replication_factor, first.replication_factor);
    assert_eq!(second.vnodes_per_node, first.vnodes_per_node);
}

/// The map is the `expected` side of a compare-and-swap, and `Cas` compares
/// **bytes**. An encoding that varied run to run — an unordered set, a hash
/// map, a float — would make every rebalance lose a race it should have won,
/// and the symptom would be a rebalance that mysteriously never lands.
#[test]
fn encoding_is_deterministic() {
    let built_once = map_of([3, 1, 2]);
    let built_again = map_of([1, 2, 3]);
    assert_eq!(built_once.encode(), built_again.encode());
}

#[test]
fn encode_decode_round_trips() {
    let map = map_of([1, 2, 3, 4, 5]);
    assert_eq!(ShardMap::decode(&map.encode()).expect("round trip"), map);
}

#[test]
fn decode_rejects_bytes_that_are_not_a_map() {
    assert!(ShardMap::decode(b"not a shard map at all").is_err());
}

/// M10's ✅ criterion, in its strong form. "Moves ≈1/N of shards **and no
/// more**" is usually read as a statistic, but consistent hashing promises
/// something much sharper: adding a node may only pull replicas *onto the new
/// node*. No shard may be reshuffled between two nodes that were both already
/// there. A placement that satisfied the statistic while shuffling existing
/// replicas would be a correctness bug — every shuffled replica is a Raft
/// membership change and a full data move for no reason at all.
fn assert_only_the_new_node_moved(before: &ShardMap, after: &ShardMap, added: u64) {
    let rf = before.replication_factor as usize;
    for shard in 0..before.num_shards {
        let old = before.replicas(shard);
        let new = after.replicas(shard);

        if !new.contains(&added) {
            assert_eq!(new, old, "shard {shard} was reshuffled without gaining node {added}");
            continue;
        }

        // The new node was inserted at some point in the clockwise walk, so
        // dropping it must leave exactly the old walk's first `rf - 1` nodes.
        let without: Vec<u64> = new.iter().copied().filter(|&n| n != added).collect();
        assert_eq!(
            without,
            old[..rf - 1].to_vec(),
            "shard {shard}: adding {added} disturbed the surviving replicas"
        );
    }
}

#[test]
fn adding_a_node_only_moves_replicas_onto_it() {
    let before = map_of([1, 2, 3, 4, 5]);
    let after = before.with_nodes([1, 2, 3, 4, 5, 6]).unwrap();
    assert_only_the_new_node_moved(&before, &after, 6);
}

#[test]
fn removing_a_node_only_moves_its_own_replicas() {
    let before = map_of([1, 2, 3, 4, 5, 6]);
    let after = before.with_nodes([1, 2, 3, 4, 5]).unwrap();

    for shard in 0..SHARDS {
        let old = before.replicas(shard);
        let new = after.replicas(shard);
        if !old.contains(&6) {
            assert_eq!(new, old, "shard {shard} moved although it never held node 6");
            continue;
        }
        // Everything that survived keeps its place and its order; the gap is
        // filled from further around the ring.
        let survivors: Vec<u64> = old.iter().copied().filter(|&n| n != 6).collect();
        assert_eq!(new[..survivors.len()].to_vec(), survivors, "shard {shard}");
    }
}

/// The statistic itself. A sixth node should end up holding roughly a sixth
/// of all shard-replicas — its fair share, no more.
#[test]
fn a_new_node_takes_about_its_fair_share() {
    let before = map_of([1, 2, 3, 4, 5]);
    let after = before.with_nodes([1, 2, 3, 4, 5, 6]).unwrap();

    let total = SHARDS as usize * RF as usize;
    let moved = (0..SHARDS).filter(|&s| after.replicas(s).contains(&6)).count();
    let fair = total as f64 / 6.0;

    assert!(
        (moved as f64) > fair * 0.5 && (moved as f64) < fair * 1.6,
        "node 6 took {moved} of {total} replica slots, fair share is {fair:.1}"
    );
}

/// Every replica slot that changed hands must have gone *to* the new node —
/// stated as a count rather than per-shard, which is the form that makes the
/// "and no more" bound quantitative.
#[test]
fn no_replica_moves_between_two_surviving_nodes() {
    let before = map_of([1, 2, 3, 4, 5]);
    let after = before.with_nodes([1, 2, 3, 4, 5, 6]).unwrap();

    let mut gained: BTreeMap<u64, usize> = BTreeMap::new();
    for shard in 0..SHARDS {
        let old: BTreeSet<_> = before.replicas(shard).iter().copied().collect();
        for node in after.replicas(shard) {
            if !old.contains(node) {
                *gained.entry(*node).or_default() += 1;
            }
        }
    }

    assert_eq!(
        gained.keys().copied().collect::<Vec<_>>(),
        vec![6],
        "nodes other than the new one gained replicas: {gained:?}"
    );
}

proptest! {
    /// The same invariant over arbitrary cluster sizes and an arbitrary
    /// incoming node, because the hand-written case above only exercises one
    /// ring.
    #[test]
    fn adding_any_node_to_any_cluster_only_moves_replicas_onto_it(
        existing in prop::collection::btree_set(1u64..64, 3..12),
        newcomer in 64u64..128,
    ) {
        let before = ShardMap::build(1, existing.iter().copied(), 64, RF, VNODES).unwrap();
        let mut grown = existing.clone();
        grown.insert(newcomer);
        let after = before.with_nodes(grown).unwrap();

        let rf = RF as usize;
        for shard in 0..before.num_shards {
            let old = before.replicas(shard);
            let new = after.replicas(shard);
            if new.contains(&newcomer) {
                let without: Vec<u64> =
                    new.iter().copied().filter(|&n| n != newcomer).collect();
                prop_assert_eq!(without, old[..rf - 1].to_vec());
            } else {
                prop_assert_eq!(new, old);
            }
        }
    }

    /// Placement depends on the node set and nothing else — not on how the
    /// set was reached. Two nodes replaying different conf-change orders must
    /// agree.
    #[test]
    fn the_map_is_a_function_of_the_node_set(
        nodes in prop::collection::btree_set(1u64..64, 3..12),
    ) {
        let forward = ShardMap::build(1, nodes.iter().copied(), 64, RF, VNODES).unwrap();
        let backward =
            ShardMap::build(1, nodes.iter().rev().copied(), 64, RF, VNODES).unwrap();
        prop_assert_eq!(forward.encode(), backward.encode());
    }
}
