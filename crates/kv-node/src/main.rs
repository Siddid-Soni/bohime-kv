//! `kv-node`: the impure shell binding kv-storage + kv-raft + kv-ring to
//! real tokio/tonic I/O. Wiring begins at M6.

mod command;
mod config;
mod storage;
mod transport;

#[cfg(test)]
mod tests;

pub use storage::BitcaskStorage;

fn main() {
    println!("bohime kv-node placeholder — implementation begins at M6");
}
