extern crate pontoon_core;

use pontoon_core::fsm::KeyValueStore;
use pontoon_core::logs::InMemoryStorage;
use pontoon_core::rpc::LoopbackTransport;
use pontoon_core::Raft;

fn new_raft(id: &str, port: u16) -> Raft<KeyValueStore, InMemoryStorage, LoopbackTransport> {
    pontoon_core::new(id, ([127, 0, 0, 1], port)).build_defaults()
}

#[test]
fn three_cluster() {
    let r1 = new_raft("one", 8080);
    let r2 = new_raft("two", 8081);
    let r3 = new_raft("three", 8082);
}

// mod util;
//
// use pontoon_core::{Raft, RaftBuilder};
// use util::{FakeRPC, InMemoryStorage};
//
// const TEST_SERVER_ID: &str = "test-server";
//
// fn new_raft() -> Raft<InMemoryStorage> {
//     RaftBuilder::new(TEST_SERVER_ID, "127.0.0.1:8080".parse().unwrap())
//         .peer("127.0.0.1:8081".parse().unwrap())
//         .peer("127.0.0.1:8082".parse().unwrap())
//         .build(InMemoryStorage::default())
// }
//
// #[test]
// fn initial_state() {
//     let raft = new_raft();
//     let status = raft.status().unwrap();
//
//     assert_eq!(status.commit_index, 0);
//     assert_eq!(status.last_applied, 0);
//     assert_eq!(status.last_log_term, None);
//     assert_eq!(status.last_log_index, None);
//     assert_eq!(status.leader, None);
//
//     assert_eq!(raft.get("A"), None);
// }
//
// #[test]
// fn update_noop() {
//     let mut raft = new_raft();
//     let mut rpc = FakeRPC::default();
//
//     raft.update(&mut rpc);
//
//     let status = raft.status().unwrap();
//
//     assert_eq!(status.commit_index, 0);
//     assert_eq!(status.last_applied, 0);
//     assert_eq!(status.last_log_term, None);
//     assert_eq!(status.last_log_index, None);
//     assert_eq!(status.leader, None);
//
//     assert_eq!(raft.get("A"), None);
// }
