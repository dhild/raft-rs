use crate::http::{Client, Server};
use crate::raft::{Raft, Storage};
use crate::storage;
use std::sync::{Arc, Mutex};

pub struct RaftConfig {}

impl RaftConfig {
    pub fn new(_args: &[String]) -> RaftConfig {
        RaftConfig {}
    }

    pub fn build_storage(&self) -> Box<dyn Storage<E = storage::Error> + Send> {
        Box::new(storage::InMemoryStorage::new())
    }

    pub fn build_raft(&self) -> Arc<Mutex<Raft<storage::Error>>> {
        let addr = ([127, 0, 0, 1], 8080).into();
        let raft = Raft::new(addr, self.build_storage());
        Arc::new(Mutex::new(raft))
    }

    pub fn build_rpc(&self) -> Client {
        Client::new()
    }

    pub fn build_server(&self, raft: &Arc<Mutex<Raft<storage::Error>>>) -> Server<storage::Error> {
        let addr = ([127, 0, 0, 1], 8080).into();
        Server::new(addr, raft)
    }
}
