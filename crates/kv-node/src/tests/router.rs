//! Key → shard → group, and what a key this node does not hold is told
//! (M11.6).

use kv_ring::ShardMap;

use crate::router::{Route, route};
use crate::transport::group;

fn map_of(nodes: [u64; 3], shards: u16) -> ShardMap {
    ShardMap::build(1, nodes, shards, 3, kv_ring::DEFAULT_VNODES).expect("placeable")
}

/// The shard a key belongs to comes from the hash alone — never from the ring
/// — so it is the same on every node and across every restart. The *group* is
/// then a pure function of the shard.
#[test]
fn a_key_routes_to_its_shards_group_on_a_node_that_holds_it() {
    let map = map_of([1, 2, 3], 64);
    // RF 3 on 3 nodes: every node holds every shard, so every key is local.
    for key in [b"a".as_slice(), b"zzz", b"", b"\xff\x00"] {
        let shard = map.shard_for_key(key);
        match route(Some(&map), 1, key) {
            Route::Local { shard: got, group } => {
                assert_eq!(got, shard);
                assert_eq!(group, group::shard(shard));
            }
            other => panic!("expected a local route for {key:?}, got {other:?}"),
        }
    }
}

/// A node that does not replicate a key's shard names the ones that do. The
/// hint is what turns a misdirected request into one redirect instead of a
/// scan of the whole cluster.
#[test]
fn a_key_this_node_does_not_hold_names_the_nodes_that_do() {
    // RF 3 on 3 nodes means every node holds everything, so a fourth node id
    // — in no replica set — is what stands in for "not mine".
    let map = map_of([1, 2, 3], 64);
    let key = b"some key";
    let shard = map.shard_for_key(key);

    match route(Some(&map), 9, key) {
        Route::Elsewhere { shard: got, replicas } => {
            assert_eq!(got, shard);
            assert_eq!(replicas, map.replicas(shard).to_vec());
            assert!(!replicas.contains(&9));
        }
        other => panic!("expected a redirect, got {other:?}"),
    }
}

/// Before the meta group has published a map there is no honest answer: the
/// node cannot say where the key belongs, and guessing would route the whole
/// keyspace by a placement nobody agreed to.
#[test]
fn a_node_with_no_map_yet_refuses_rather_than_guesses() {
    assert!(matches!(route(None, 1, b"k"), Route::NoMap));
}
