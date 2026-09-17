//! The `kv-client` CLI (M6). Keys and values are UTF-8 here and `Vec<u8>` in
//! the library — the store itself is byte-oriented.

use clap::{Parser, Subcommand};

use crate::{Client, NodeId};

#[derive(Debug, Parser)]
#[command(name = "kv-client", about = "A Bohime client")]
pub struct Args {
    /// A node, as `id=endpoint`. Repeat once per node.
    #[arg(long = "peer", value_parser = parse_peer, required = true)]
    pub peers: Vec<(NodeId, String)>,
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Debug, Subcommand)]
pub enum CliCommand {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Compare-and-swap. Omit `--expected` to mean "only if absent".
    Cas {
        key: String,
        new_value: String,
        /// The value the key must currently hold. Absent means the key must
        /// not exist — a distinct condition from holding an empty value.
        #[arg(long)]
        expected: Option<String>,
    },
}

fn parse_peer(s: &str) -> Result<(NodeId, String), String> {
    let (id, addr) = s.split_once('=').ok_or_else(|| format!("expected id=endpoint, got {s:?}"))?;
    let id: NodeId = id.parse().map_err(|_| format!("bad node id {id:?}"))?;
    Ok((id, addr.to_string()))
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let mut client = Client::new(args.peers);
    match args.command {
        CliCommand::Get { key } => match client.get(key.as_bytes()).await? {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => {
                eprintln!("(nil)");
                std::process::exit(1);
            }
        },
        CliCommand::Put { key, value } => {
            client.put(key.as_bytes(), value.as_bytes()).await?;
            println!("OK");
        }
        CliCommand::Delete { key } => {
            client.delete(key.as_bytes()).await?;
            println!("OK");
        }
        CliCommand::Cas { key, new_value, expected } => {
            let swapped = client
                .cas(key.as_bytes(), expected.as_deref().map(str::as_bytes), new_value.as_bytes())
                .await?;
            if swapped {
                println!("OK");
            } else {
                eprintln!("(not swapped)");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
