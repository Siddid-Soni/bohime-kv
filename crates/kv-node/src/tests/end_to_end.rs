//! M6's gate: three real processes, real sockets, a real `kill -9`.
//!
//! The roadmap places this at `crates/kv-node/tests/end_to_end.rs`. It lives
//! in `src/tests/` instead, per the crate layout convention — there are no
//! `crates/*/tests/` directories here, and this test needs nothing public.
//!
//! Timeouts are generous on purpose. What is asserted is that a leader is
//! elected and that committed data survives, not how fast a loaded CI box gets
//! there. A tight bound would make this flaky, and a flaky gate is worse than
//! no gate.

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use kv_client::Client;

struct Node {
    id: u64,
    child: Child,
    _dir: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// The `kv-node` executable, rebuilt first.
///
/// **This rebuild is load-bearing.** `cargo nextest run` builds test binaries,
/// not the crate's ordinary `[[bin]]` target, so `target/debug/kv-node` is
/// whatever the last `cargo build` left there. Without this, the gate happily
/// spawns a stale binary and passes — it did exactly that across the M7 read
/// changes, reporting green while running M6 code, and only failed once the
/// binary was rebuilt by hand. A test that can silently validate the wrong
/// binary is worse than no test.
///
/// `cargo build` is a no-op in the common case. It is safe to call here
/// because nextest has finished building and released the target lock before
/// any test runs.
fn kv_node_binary() -> std::path::PathBuf {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--quiet", "-p", "kv-node", "--bin", "kv-node"])
        .status()
        .expect("cargo build runs");
    assert!(status.success(), "kv-node failed to build; the gate cannot test a stale binary");

    // `CARGO_BIN_EXE_*` is set for integration tests only, and this is a unit
    // test inside the binary crate; `cargo_bin` resolves from the test
    // executable's own target directory instead.
    assert_cmd::cargo::cargo_bin("kv-node")
}

fn spawn_cluster(ports: &[u16]) -> Vec<Node> {
    let bin = kv_node_binary();
    ports
        .iter()
        .enumerate()
        .map(|(i, &port)| {
            let id = i as u64 + 1;
            let dir = tempfile::tempdir().unwrap();
            let mut cmd = Command::new(&bin);
            cmd.arg("--id")
                .arg(id.to_string())
                .arg("--listen")
                .arg(format!("127.0.0.1:{port}"))
                .arg("--data-dir")
                .arg(dir.path())
                // Four shards, not the default 256. The gate is about three
                // real processes surviving a `kill -9`, not about how many
                // Raft groups one box can tick: 256 shards × RF 3 is 768
                // Bitcask pairs across three processes, which measures the
                // machine rather than the code.
                .arg("--shards")
                .arg("4");
            for (j, &peer_port) in ports.iter().enumerate() {
                if j != i {
                    cmd.arg("--peer").arg(format!("{}=http://127.0.0.1:{peer_port}", j + 1));
                }
            }
            Node { id, child: cmd.spawn().expect("kv-node starts"), _dir: dir }
        })
        .collect()
}

/// Polls the cluster until it serves `expected`, or fails after `within`.
///
/// Reads go through whichever node currently leads: since M7 a read takes a
/// leadership quorum, so asking one node in particular is no longer
/// meaningful, and the M6 property "get k on any node returns it" is
/// deliberately gone — it was a property of the *stale* read.
///
/// Still a poll rather than a single call, because a freshly elected leader
/// must commit its own no-op before it can serve any read at all.
async fn read_until(
    cluster: &[(u64, String)],
    key: &[u8],
    expected: &[u8],
    within: Duration,
) -> Vec<u8> {
    let mut client = Client::new(cluster.to_vec());
    let deadline = Instant::now() + within;
    let mut last = None;
    while Instant::now() < deadline {
        last = client.get(key).await.unwrap_or(None);
        if last.as_deref() == Some(expected) {
            return last.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "the cluster never served {:?} within {within:?}; last saw {last:?}",
        String::from_utf8_lossy(expected)
    );
}

fn endpoints(ports: &[u16]) -> Vec<(u64, String)> {
    ports.iter().enumerate().map(|(i, p)| (i as u64 + 1, format!("http://127.0.0.1:{p}"))).collect()
}

#[tokio::test]
async fn three_processes_replicate_and_survive_killing_the_leader() {
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let mut cluster = spawn_cluster(&ports);
    let mut client = Client::new(endpoints(&ports));

    // The client retries through NotLeader and connection refusals, so a
    // successful put is also the signal that an election finished.
    client.put(b"alpha", b"one").await.expect("the cluster elects a leader and accepts a write");

    // The write is readable back through the cluster.
    read_until(&endpoints(&ports), b"alpha", b"one", Duration::from_secs(10)).await;

    let leader = client.leader().expect("the accepted write named a leader");
    let position = cluster.iter().position(|n| n.id == leader).expect("the leader is one of ours");

    // kill -9: no shutdown hook, no flush, nothing but what reached the disk.
    let mut victim = cluster.remove(position);
    victim.child.kill().unwrap();
    victim.child.wait().unwrap();
    drop(victim);

    let survivors: Vec<_> = endpoints(&ports).into_iter().filter(|(id, _)| *id != leader).collect();
    let mut client = Client::new(survivors.clone());

    // The dead leader's committed write is still there.
    let value = client.get(b"alpha").await.expect("a read after the kill");
    assert_eq!(value, Some(b"one".to_vec()), "a committed write did not survive its leader");

    // And the survivors elect a new leader and keep taking writes.
    client.put(b"beta", b"two").await.expect("the survivors elect a new leader");
    read_until(&survivors, b"beta", b"two", Duration::from_secs(10)).await;
}
