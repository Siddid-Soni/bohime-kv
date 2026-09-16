//! `kv-client`: the CLI front end. Everything real lives in the library, so
//! that kv-node's end-to-end test can drive the same code the CLI does.

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kv_client::cli::run(kv_client::cli::Args::parse()).await
}
