use std::time::Duration;

use crate::{Client, ClientError};

/// A cluster that is not there must produce an error, not a hang. A client
/// that waits forever for a leader that cannot exist is indistinguishable
/// from a hung client.
#[tokio::test]
async fn an_unreachable_cluster_fails_rather_than_hanging() {
    let mut client = Client::new(vec![
        (1, "http://127.0.0.1:1".to_string()),
        (2, "http://127.0.0.1:2".to_string()),
    ]);
    let result = tokio::time::timeout(Duration::from_secs(60), client.put(b"k", b"v")).await;
    assert!(result.is_ok(), "it must give up, not hang");
    assert!(matches!(result.unwrap(), Err(ClientError::NoReachableNode { .. })));
}

#[tokio::test]
async fn a_fresh_client_knows_no_leader() {
    let client = Client::new(vec![(1, "http://127.0.0.1:1".to_string())]);
    assert_eq!(client.leader(), None);
}
