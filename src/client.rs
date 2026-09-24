//! Sends a command to any node and follows redirects to the shard's leader.

use crate::pb::{Command, Reply, kv_client::KvClient};
use std::time::Duration;

/// Gives up after `ATTEMPTS` tries. A retried put or delete may apply twice,
/// which is harmless: both are idempotent.
const ATTEMPTS: usize = 50;

pub async fn call(nodes: &[String], cmd: Command) -> Option<Reply> {
    let mut addr = nodes[0].clone();
    for attempt in 1..=ATTEMPTS {
        let reply = tokio::time::timeout(Duration::from_secs(2), once(&addr, cmd.clone())).await;
        match reply.ok().flatten() {
            Some(reply) if reply.ok => return Some(reply),
            Some(reply) if !reply.leader.is_empty() => addr = reply.leader,
            _ => {
                // Unreachable, or no leader yet: wait for an election, try another node.
                addr = nodes[attempt % nodes.len()].clone();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    None
}

async fn once(addr: &str, cmd: Command) -> Option<Reply> {
    let mut client = KvClient::connect(format!("http://{addr}")).await.ok()?;
    Some(client.call(cmd).await.ok()?.into_inner())
}
