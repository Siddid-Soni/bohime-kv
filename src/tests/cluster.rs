//! Whole clusters over real gRPC on loopback. Each node runs on its own
//! runtime, so `stop` drops every socket it had open, as a crash would.

use crate::client;
use crate::node::{self, Config};
use crate::pb::{Command, Op};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

struct Cluster {
    dir: tempfile::TempDir,
    nodes: BTreeMap<u64, String>,
    running: BTreeMap<u64, Runtime>,
    shards: u32,
    rf: usize,
    client: Runtime,
}

impl Cluster {
    fn start(n: u64, shards: u32, rf: usize) -> Self {
        let mut c = Self {
            dir: tempfile::tempdir().unwrap(),
            nodes: BTreeMap::new(),
            running: BTreeMap::new(),
            shards,
            rf,
            client: Runtime::new().unwrap(),
        };
        let mut listeners = vec![];
        for id in 1..=n {
            let rt = Runtime::new().unwrap();
            let listener = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
            c.nodes.insert(id, listener.local_addr().unwrap().to_string());
            listeners.push((id, rt, listener));
        }
        for (id, rt, listener) in listeners {
            c.spawn(id, rt, listener);
        }
        c
    }

    fn spawn(&mut self, id: u64, rt: Runtime, listener: TcpListener) {
        let cfg = Config {
            id,
            nodes: self.nodes.clone(),
            shards: self.shards,
            rf: self.rf,
            data_dir: self.dir.path().join(id.to_string()),
            tick: Duration::from_millis(10),
        };
        rt.spawn(node::serve(cfg, listener));
        self.running.insert(id, rt);
    }

    fn stop(&mut self, id: u64) {
        self.running.remove(&id).unwrap().shutdown_timeout(Duration::from_secs(5));
    }

    fn restart(&mut self, id: u64) {
        let rt = Runtime::new().unwrap();
        let listener = rt.block_on(TcpListener::bind(&self.nodes[&id])).unwrap();
        self.spawn(id, rt, listener);
    }

    fn call(&self, op: Op, key: &str, value: &str) -> Option<Vec<u8>> {
        let nodes: Vec<String> = self.nodes.values().cloned().collect();
        let cmd = Command { op: op as i32, key: key.into(), value: value.into() };
        self.client.block_on(client::call(&nodes, cmd)).expect("no leader reachable").value
    }

    fn put(&self, key: &str, value: &str) {
        self.call(Op::Put, key, value);
    }

    fn get(&self, key: &str) -> Option<String> {
        self.call(Op::Get, key, "").map(|v| String::from_utf8(v).unwrap())
    }
}

#[test]
fn puts_gets_and_deletes_replicate() {
    let c = Cluster::start(3, 4, 3);
    for i in 0..20 {
        c.put(&format!("k{i}"), &format!("v{i}"));
    }
    for i in 0..20 {
        assert_eq!(c.get(&format!("k{i}")), Some(format!("v{i}")));
    }
    c.call(Op::Delete, "k3", "");
    assert_eq!(c.get("k3"), None);
}

#[test]
fn keys_reach_their_shard_from_a_node_that_does_not_hold_it() {
    // RF 1 over 3 nodes: every node holds a third of the shards.
    let c = Cluster::start(3, 8, 1);
    let first = vec![c.nodes[&1].clone()];
    for i in 0..20 {
        let cmd = Command { op: Op::Put as i32, key: format!("k{i}").into(), value: b"v".to_vec() };
        c.client.block_on(client::call(&first, cmd)).unwrap();
    }
    for i in 0..20 {
        assert_eq!(c.get(&format!("k{i}")), Some("v".into()));
    }
}

#[test]
fn a_killed_node_is_survived_and_catches_up_when_it_returns() {
    let mut c = Cluster::start(3, 4, 3);
    c.put("a", "1");
    c.stop(1);
    c.put("b", "2");
    c.restart(1);
    // Node 1 is now needed for every quorum, so these commit only once it has
    // caught up on "b".
    c.stop(2);
    c.put("c", "3");
    assert_eq!(c.get("a"), Some("1".into()));
    assert_eq!(c.get("b"), Some("2".into()));
    assert_eq!(c.get("c"), Some("3".into()));
}

#[test]
fn data_survives_restarting_every_node() {
    let mut c = Cluster::start(3, 4, 3);
    for i in 0..10 {
        c.put(&format!("k{i}"), &format!("v{i}"));
    }
    for id in 1..=3 {
        c.stop(id);
    }
    for id in 1..=3 {
        c.restart(id);
    }
    for i in 0..10 {
        assert_eq!(c.get(&format!("k{i}")), Some(format!("v{i}")));
    }
}
