//! The hash is a *wire format*, not an implementation detail (M10.1).
//!
//! `hash(key) % num_shards` decides which shard — and therefore which Raft
//! group, and therefore which machines — a key lives on. If the function's
//! output ever changes, every key in the cluster silently moves to a
//! different shard while the data stays where it was. That is unrecoverable
//! corruption that looks like a bug in Raft, so these tests pin the bytes
//! rather than merely exercising the API.

use std::collections::BTreeMap;

use crate::hash::{fnv1a64, hash64};
use crate::shard_for;

/// The published FNV-1a 64 test vectors. These are not our numbers to choose:
/// they come from the FNV reference, and matching them is what makes the
/// unfinalized core recognisable to anyone auditing it.
#[test]
fn fnv1a_matches_the_published_vectors() {
    assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
}

/// The finalizer must be a bijection, or two distinct FNV outputs could
/// collapse onto one shard for reasons that have nothing to do with the keys.
/// Checking it over a large sample is enough to catch a dropped or duplicated
/// term in the mix.
#[test]
fn the_finalizer_loses_nothing() {
    let mut seen = BTreeMap::new();
    for i in 0..100_000u64 {
        let key = i.to_be_bytes();
        let h = hash64(&key);
        if let Some(previous) = seen.insert(h, i) {
            panic!("hash64 collided: {previous} and {i} both hash to {h:#x}");
        }
    }
}

/// Raw FNV-1a XORs each byte into the *low* bits and then multiplies, so the
/// low bits are the weakly mixed ones — and `% num_shards` reads exactly
/// those. This is the whole reason for the finalizer, so it gets a test that
/// fails if the finalizer is ever removed: consecutive keys must not land on
/// consecutive shards.
#[test]
fn consecutive_keys_do_not_land_on_consecutive_shards() {
    let shards: Vec<_> = (0..16u64).map(|i| shard_for(&i.to_be_bytes(), 256)).collect();
    let consecutive = shards.windows(2).filter(|w| w[1] == w[0].wrapping_add(1) % 256).count();
    assert!(consecutive < 4, "shards look like a counter, not a hash: {shards:?}");
}

#[test]
fn shard_for_stays_inside_the_shard_count() {
    for n in [1u16, 2, 3, 7, 256, 4096] {
        for i in 0..1000u64 {
            let shard = shard_for(&i.to_be_bytes(), n);
            assert!(shard < n, "shard {shard} out of range for {n} shards");
        }
    }
}

#[test]
fn shard_for_is_a_function_of_the_key_alone() {
    let key = b"the-same-key";
    let first = shard_for(key, 256);
    for _ in 0..100 {
        assert_eq!(shard_for(key, 256), first);
    }
}

/// M10's first ✅ criterion is "key→shard is stable across restarts". A
/// restart is only the cheapest way to break it; a toolchain upgrade that
/// changed the hash would break it just as thoroughly and far more quietly.
/// These constants are a change-detector: if one moves, the migration story
/// has to be deliberate.
///
/// They were computed by a second, independent implementation of FNV-1a +
/// fmix64 rather than copied out of this one's output, so they check the code
/// rather than merely recording it.
#[test]
fn key_to_shard_is_pinned() {
    assert_eq!(shard_for(b"", 256), 38);
    assert_eq!(shard_for(b"a", 256), 91);
    assert_eq!(shard_for(b"foobar", 256), 43);
    assert_eq!(shard_for(b"user:1", 256), 65);
    assert_eq!(shard_for(b"user:2", 256), 34);
}

/// Spreading well is the point of hashing onto shards at all: a lopsided
/// distribution puts a hot shard's whole Raft group on one node's disk.
#[test]
fn keys_spread_across_every_shard() {
    const SHARDS: u16 = 256;
    const KEYS: u64 = 100_000;

    let mut counts = vec![0u64; SHARDS as usize];
    for i in 0..KEYS {
        counts[shard_for(format!("key:{i}").as_bytes(), SHARDS) as usize] += 1;
    }

    let empty = counts.iter().filter(|&&c| c == 0).count();
    assert_eq!(empty, 0, "{empty} shards got no keys at all");

    // With 100k keys over 256 shards the mean is ~390 and the standard
    // deviation of a uniform assignment is ~20, so a shard 25% off the mean is
    // about 5 sigma. Wide enough not to flake, tight enough that a hash with a
    // real bias fails.
    let mean = KEYS as f64 / SHARDS as f64;
    let (&min, &max) = (counts.iter().min().unwrap(), counts.iter().max().unwrap());
    assert!(
        (min as f64) > mean * 0.75 && (max as f64) < mean * 1.25,
        "uneven spread: min {min}, max {max}, mean {mean:.1}"
    );
}
