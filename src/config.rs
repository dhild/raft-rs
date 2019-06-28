use crate::raft::{Server, Storage};
use crate::storage::{Error, InMemoryStorage};

pub struct RaftConfig {}

impl RaftConfig {
    pub fn new(args: &[String]) -> RaftConfig {
        RaftConfig{}
    }

    pub fn storage(&self) -> Box<dyn Storage<E = Error>> {
        Box::new(InMemoryStorage::new())
    }

    pub fn build_server(&self) -> Server<Error> {
        Server::new(self.storage())
    }
}
