extern crate pontoon_core;
extern crate env_logger;

fn main() {
    env_logger::init();

    let fsm = pontoon_core::fsm::KeyValueStore::default();
    let storage = pontoon_core::logs::InMemoryStorage::default();
    let transport = pontoon_core::rpc::HttpTransport::default();
    let mut raft =
        pontoon_core::new("unique-raft-id", ([127, 0, 0, 1], 8080)).build(fsm, storage, transport);

    let (stop, stop_rx) = pontoon_core::crossbeam_channel::bounded(0);
    raft.run(stop_rx);
}
