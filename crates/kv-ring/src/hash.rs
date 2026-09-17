//! The placement hash (M10.1).
//!
//! **Why this is not `DefaultHasher`.** `std`'s hasher documents that its
//! output may change between Rust releases, and `HashMap` is free to seed it
//! randomly per process. Either property is fatal here: `hash(key) %
//! num_shards` decides which machines hold a key, so an output that changed
//! on a toolchain upgrade would move every key in the cluster to a different
//! shard while the data stayed where it was. Nothing would report an error —
//! reads would simply start missing, and the first place anyone would look is
//! Raft.
//!
//! So the function is ours, fully specified here, and pinned by golden
//! vectors in the tests.
//!
//! **Why FNV-1a and then a finalizer.** FNV-1a is four lines and has a
//! published set of test vectors, which makes the core auditable. What it is
//! not is well-avalanched in its *low* bits: each byte is XORed into the low
//! end and the multiply carries influence upward, so the high bits get mixed
//! far better than the low ones. `% num_shards` reads exactly the low bits.
//! MurmurHash3's `fmix64` finalizer fixes that, and it is a bijection, so it
//! cannot introduce a collision FNV-1a did not already have.

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64, unfinalized. Separate from [`hash64`] so the published vectors
/// can be asserted against the thing they actually describe.
pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// MurmurHash3's 64-bit finalizer. Every step is invertible — the shifts are
/// xor-shifts and both constants are odd, so they have multiplicative
/// inverses mod 2^64 — which is what makes the whole function a bijection.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// The project's one placement hash. Used for keys, for virtual node
/// positions, and for shard positions on the ring — one function, so there is
/// one thing to keep stable rather than three.
pub fn hash64(bytes: &[u8]) -> u64 {
    fmix64(fnv1a64(bytes))
}

/// Which shard a key belongs to.
///
/// Deliberately independent of the ring: the ring decides which *nodes* hold
/// a shard and changes whenever the cluster does, while this changes never.
/// That separation is what lets a node join without every key moving, and it
/// is M10's first ✅ criterion.
pub fn shard_for(key: &[u8], num_shards: u16) -> u16 {
    assert!(num_shards > 0, "a cluster has at least one shard");
    (hash64(key) % num_shards as u64) as u16
}
