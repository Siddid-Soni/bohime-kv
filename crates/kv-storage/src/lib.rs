//! Bitcask-style log-structured storage engine (plan §1.13).
//!
//! Implementation begins at M1: record codec (M1.1), the append-only engine
//! (M1.2), keydir rebuild behind a `KeyDirIndex` trait (M1.3), segment
//! rotation (M1.4), compaction + hint files (M1.5), crash recovery (M1.6),
//! and configurable fsync policy (M1.7).

#[cfg(test)]
mod tests {
    #[test]
    fn crate_wires_up() {
        assert_eq!(2 + 2, 4);
    }
}
