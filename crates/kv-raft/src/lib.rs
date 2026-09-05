//! Pure Raft consensus core (plan §1.5). No I/O, no async runtime, no
//! wall-clock reads — `tick`/`step` take logical time and return `Action`s
//! for the caller (`kv-node` for real I/O, `kv-sim` for simulated I/O) to
//! execute. See plan Part 2 for the `Action`/`Ready` interface and M3 for
//! the build sequence (types, election, replication, commitment, safety).

#[cfg(test)]
mod tests {
    #[test]
    fn crate_wires_up() {
        assert_eq!(2 + 2, 4);
    }
}
