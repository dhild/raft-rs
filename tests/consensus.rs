mod common;

use pontoon::Client;
use std::time::Duration;

#[tokio::test]
async fn test_consensus_replication() {
    common::setup();
    common::state_machine(CONFIG_SERVER_1).await;
    common::state_machine(CONFIG_SERVER_2).await;
    common::state_machine(CONFIG_SERVER_3).await;

    tokio::time::delay_for(Duration::from_millis(1500)).await;

    let mut client = common::client(CONFIG_CLIENT);

    client.put("foo", b"bar").await.unwrap();
    assert_eq!(client.get("foo").await.unwrap().unwrap(), b"bar".to_vec());

    client.put("foo", b"baz").await.unwrap();
    assert_eq!(client.get("foo").await.unwrap().unwrap(), b"baz".to_vec());
}

pub const CONFIG_SERVER_1: &'static str = r#"
id = "peer1"
[storage.memory]
term = 0
[rpc.http]
address = "localhost:8001"
[[peer]]
id = "peer2"
address = "localhost:8002"
[[peer]]
id = "peer3"
address = "localhost:8003"
"#;
pub const CONFIG_SERVER_2: &'static str = r#"
id = "peer2"
[storage.memory]
term = 0
[rpc.http]
address = "localhost:8002"
[[peer]]
id = "peer1"
address = "localhost:8001"
[[peer]]
id = "peer3"
address = "localhost:8003"
"#;
pub const CONFIG_SERVER_3: &'static str = r#"
id = "peer3"
[storage.memory]
term = 0
[rpc.http]
address = "localhost:8003"
[[peer]]
id = "peer1"
address = "localhost:8001"
[[peer]]
id = "peer2"
address = "localhost:8002"
"#;

pub const CONFIG_CLIENT: &'static str = r#"
address = "localhost:8001"
[http]
"#;
