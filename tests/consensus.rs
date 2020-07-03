mod common;

#[tokio::test]
async fn test_consensus_replication() {
    common::setup();
    let mut sm1 = common::state_machine(CONFIG_1).await;
    let sm2 = common::state_machine(CONFIG_2).await;
    let sm3 = common::state_machine(CONFIG_3).await;

    sm1.put("foo", b"bar").await.unwrap();
    assert_eq!(sm2.get("foo").await.unwrap(), b"bar"[..]);
    assert_eq!(sm3.get("foo").await.unwrap(), b"bar"[..]);
}

pub const CONFIG_1: &'static str = r#"
id = "peer1"
[storage.memory]
[rpc.http]
address = "localhost:8001"
[[peer]]
id = "peer2"
address = "localhost:8002"
[[peer]]
id = "peer3"
address = "localhost:8003"
"#;
pub const CONFIG_2: &'static str = r#"
id = "peer2"
[storage.memory]
[rpc.http]
address = "localhost:8002"
[[peer]]
id = "peer1"
address = "localhost:8001"
[[peer]]
id = "peer3"
address = "localhost:8003"
"#;
pub const CONFIG_3: &'static str = r#"
id = "peer3"
[storage.memory]
[rpc.http]
address = "localhost:8003"
[[peer]]
id = "peer1"
address = "localhost:8001"
[[peer]]
id = "peer2"
address = "localhost:8002"
"#;
