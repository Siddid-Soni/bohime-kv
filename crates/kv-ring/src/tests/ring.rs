//! Placement: which nodes hold a shard (M10.2).

use std::collections::{BTreeMap, BTreeSet};

use crate::ring::{PlacementError, Ring};

/// The default in production. Small enough that the tests stay fast, large
/// enough that balance is meaningful.
const VNODES: u32 = 128;

fn ring_of(nodes: impl IntoIterator<Item = u64>) -> Ring {
    Ring::with_nodes(nodes, VNODES)
}

#[test]
fn place_returns_rf_distinct_nodes() {
    let ring = ring_of([1, 2, 3, 4, 5]);
    for shard in 0..256u16 {
        let replicas = ring.place(shard, 3).expect("5 nodes can hold 3 replicas");
        assert_eq!(replicas.len(), 3, "shard {shard}");
        let distinct: BTreeSet<_> = replicas.iter().collect();
        assert_eq!(distinct.len(), 3, "shard {shard} placed a node twice: {replicas:?}");
    }
}

#[test]
fn replication_factor_of_one_gives_one_node() {
    let ring = ring_of([1, 2, 3]);
    for shard in 0..256u16 {
        assert_eq!(ring.place(shard, 1).expect("rf 1").len(), 1);
    }
}

#[test]
fn every_node_may_hold_every_shard_when_rf_equals_the_cluster() {
    let ring = ring_of([1, 2, 3]);
    for shard in 0..256u16 {
        let replicas = ring.place(shard, 3).expect("rf 3 on 3 nodes");
        assert_eq!(replicas.iter().copied().collect::<BTreeSet<_>>(), BTreeSet::from([1, 2, 3]));
    }
}

/// The decision from the M10 design discussion: a replication factor the
/// cluster cannot satisfy is refused, not quietly under-replicated. A config
/// that cannot be met should fail where an operator will see it.
#[test]
fn place_refuses_when_rf_exceeds_the_node_count() {
    let ring = ring_of([1, 2]);
    assert_eq!(
        ring.place(0, 3),
        Err(PlacementError::NotEnoughNodes { replication_factor: 3, nodes: 2 })
    );
}

#[test]
fn place_refuses_an_empty_ring() {
    let ring = ring_of([]);
    assert_eq!(
        ring.place(0, 1),
        Err(PlacementError::NotEnoughNodes { replication_factor: 1, nodes: 0 })
    );
}

#[test]
fn place_refuses_a_replication_factor_of_zero() {
    let ring = ring_of([1, 2, 3]);
    assert_eq!(ring.place(0, 0), Err(PlacementError::ZeroReplicationFactor));
}

/// Placement must be a pure function of the node *set*. If it depended on
/// insertion order, two nodes that learned of the same membership through
/// different sequences of conf changes would compute different maps — the
/// exact divergence the meta group exists to prevent.
#[test]
fn insertion_order_does_not_change_placement() {
    let forward = ring_of([1, 2, 3, 4, 5]);
    let mut backward = Ring::new(VNODES);
    for id in [5, 4, 3, 2, 1] {
        backward.add_node(id);
    }
    let mut shuffled = Ring::new(VNODES);
    for id in [3, 1, 5, 2, 4] {
        shuffled.add_node(id);
    }

    for shard in 0..256u16 {
        let expected = forward.place(shard, 3).unwrap();
        assert_eq!(backward.place(shard, 3).unwrap(), expected, "shard {shard}");
        assert_eq!(shuffled.place(shard, 3).unwrap(), expected, "shard {shard}");
    }
}

/// Adding a node and removing it again must return the ring to where it
/// started — otherwise `remove` is leaving debris that will show up as a
/// spurious rebalance later.
#[test]
fn add_then_remove_restores_the_ring() {
    let before = ring_of([1, 2, 3]);
    let mut after = ring_of([1, 2, 3]);
    after.add_node(4);
    after.remove_node(4);

    assert_eq!(after.nodes(), before.nodes());
    for shard in 0..256u16 {
        assert_eq!(after.place(shard, 3).unwrap(), before.place(shard, 3).unwrap());
    }
}

#[test]
fn adding_a_node_twice_is_not_two_nodes() {
    let mut ring = ring_of([1, 2, 3]);
    ring.add_node(3);
    assert_eq!(ring.nodes(), &BTreeSet::from([1, 2, 3]));
}

#[test]
fn removing_an_absent_node_is_a_no_op() {
    let mut ring = ring_of([1, 2, 3]);
    ring.remove_node(9);
    assert_eq!(ring.nodes(), &BTreeSet::from([1, 2, 3]));
}

/// This is what virtual nodes are *for*. With one position per node the ring
/// is carved into N arcs of wildly uneven length, so one node ends up holding
/// several times its share — and a shard's whole Raft group lives on its
/// replicas' disks, so imbalance here is imbalance in load and capacity both.
#[test]
fn virtual_nodes_balance_shards_across_the_cluster() {
    const SHARDS: u16 = 256;
    let nodes = [1u64, 2, 3, 4, 5];
    let ring = ring_of(nodes);

    let mut load: BTreeMap<u64, usize> = nodes.iter().map(|&n| (n, 0)).collect();
    for shard in 0..SHARDS {
        for replica in ring.place(shard, 3).unwrap() {
            *load.get_mut(&replica).unwrap() += 1;
        }
    }

    // 256 shards x 3 replicas spread over 5 nodes: ~154 each.
    let mean = (SHARDS as usize * 3) as f64 / nodes.len() as f64;
    for (&node, &count) in &load {
        assert!(
            (count as f64) > mean * 0.6 && (count as f64) < mean * 1.4,
            "node {node} holds {count} shard-replicas, mean is {mean:.1}: {load:?}"
        );
    }
}

/// A single vnode per node is the degenerate ring, and it should still be
/// *correct* even though it is badly balanced. Worth pinning so the vnode
/// count stays a tuning knob rather than a correctness dependency.
#[test]
fn one_vnode_per_node_still_places_correctly() {
    let ring = Ring::with_nodes([1, 2, 3, 4, 5], 1);
    for shard in 0..256u16 {
        let replicas = ring.place(shard, 3).unwrap();
        assert_eq!(replicas.iter().collect::<BTreeSet<_>>().len(), 3);
    }
}
