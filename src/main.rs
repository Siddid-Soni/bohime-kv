use bohime::node::{self, Config};
use bohime::pb::{Command, Op};
use clap::{Parser, Subcommand};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a node: one replica of one shard. Every node takes the same --node
    /// list and --rf; each run of rf nodes in id order is one shard.
    Serve {
        #[arg(long)]
        id: u64,
        /// `id=host:port`, once per node in the cluster, this one included.
        #[arg(long = "node", value_parser = parse_node, required = true)]
        nodes: Vec<(u64, String)>,
        #[arg(long, default_value_t = 3)]
        rf: usize,
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long, default_value_t = 50)]
        tick_ms: u64,
    },
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Del {
        key: String,
    },
}

fn parse_node(s: &str) -> Result<(u64, String), String> {
    let (id, addr) = s.split_once('=').ok_or("expected id=host:port")?;
    Ok((id.parse().map_err(|_| "bad node id")?, addr.to_string()))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let is_get = matches!(cli.cmd, Cmd::Get { .. });
    let cmd = match cli.cmd {
        Cmd::Serve { id, nodes, rf, data_dir, tick_ms } => {
            let nodes: BTreeMap<_, _> = nodes.into_iter().collect();
            let listener = tokio::net::TcpListener::bind(&nodes[&id]).await.expect("bind");
            let tick = Duration::from_millis(tick_ms);
            let cfg = Config { id, nodes, rf, data_dir, tick };
            return node::serve(cfg, listener).await.expect("server");
        }
        Cmd::Get { key } => Command { op: Op::Get as i32, key: key.into(), value: vec![] },
        Cmd::Put { key, value } => {
            Command { op: Op::Put as i32, key: key.into(), value: value.into() }
        }
        Cmd::Del { key } => Command { op: Op::Delete as i32, key: key.into(), value: vec![] },
    };
    let nodes: Vec<String> = std::env::var("BOHIME_NODES")
        .unwrap_or_else(|_| "127.0.0.1:7001".into())
        .split(',')
        .map(String::from)
        .collect();
    match bohime::client::call(&nodes, cmd).await {
        Some(reply) => match reply.value {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None if is_get => {
                eprintln!("(not found)");
                std::process::exit(1);
            }
            None => println!("OK"),
        },
        None => {
            eprintln!("no leader reachable");
            std::process::exit(2);
        }
    }
}
