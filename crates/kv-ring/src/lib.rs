//! Consistent hashing + shard map (plan §1.11). Keys hash onto a fixed
//! number of shards (256); the `shard -> [replica nodes]` map is versioned
//! and replicated by the meta Raft group, then published for reads via
//! `ArcSwap` (deliberately not `left-right` — see plan §1.15). Build begins
//! at M10.

#[cfg(test)]
mod tests;
