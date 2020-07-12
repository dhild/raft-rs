mod common;

use std::time::Duration;

#[tokio::test]
async fn test_consensus_replication() {
    common::setup();
    common::spawn_three_servers();

    tokio::time::delay_for(Duration::from_millis(1500)).await;

    let mut client = pontoon::client("localhost:8001").build_http_client();

    client.put("foo", b"bar").await.unwrap();
    assert_eq!(client.get("foo").await.unwrap().unwrap(), b"bar".to_vec());

    client.put("foo", b"baz").await.unwrap();
    assert_eq!(client.get("foo").await.unwrap().unwrap(), b"baz".to_vec());
}
