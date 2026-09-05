//! Deterministic simulator (plan §1.16 / M4): virtual clock, seeded RNG,
//! a lossy in-memory network (drop/delay/duplicate/reorder, partitions),
//! and node crash/restart. Drives `kv_raft`'s pure `Action`/`Ready`
//! interface so a failure is reproducible byte-for-byte from its seed.

#[cfg(test)]
mod tests {
    #[test]
    fn crate_wires_up() {
        assert_eq!(2 + 2, 4);
    }
}
