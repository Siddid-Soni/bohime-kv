pub mod bitcask;
pub mod client;
pub mod node;
pub mod raft;

#[allow(clippy::result_large_err)] // tonic::Status, in generated code
pub mod pb {
    tonic::include_proto!("bohime");
}

#[cfg(test)]
mod tests;
