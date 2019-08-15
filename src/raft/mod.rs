pub mod api;
pub use api::*;
mod error;
pub use error::{Error, Result};
mod storage;
use storage::{InMemoryStorage, Storage};

use crate::RaftConfig;
use std::sync::RwLock;
use std::time::Instant;

pub struct Raft {
    id: ServerId,
    majority: usize,
    state: RwLock<State>,
}

enum State {
    Follower {
        storage: Box<dyn Storage + Send + Sync>,
        commit_index: LogIndex,
        last_applied: LogIndex,
        known_peers: Vec<ServerId>,
        last_contact_time: Instant,
    },
    Candidate {
        storage: Box<dyn Storage + Send + Sync>,
        commit_index: LogIndex,
        last_applied: LogIndex,
        known_peers: Vec<ServerId>,
    },
    Leader {
        storage: Box<dyn Storage + Send + Sync>,
        commit_index: LogIndex,
        last_applied: LogIndex,
        known_peers: Vec<ServerId>,
    },
}

impl State {
    fn commit_index(&self) -> LogIndex {
        match *self {
            State::Follower {
                commit_index: c, ..
            } => c,
            State::Candidate {
                commit_index: c, ..
            } => c,
            State::Leader {
                commit_index: c, ..
            } => c,
        }
    }
    fn last_applied(&self) -> LogIndex {
        match *self {
            State::Follower {
                last_applied: c, ..
            } => c,
            State::Candidate {
                last_applied: c, ..
            } => c,
            State::Leader {
                last_applied: c, ..
            } => c,
        }
    }
}

impl Raft {
    pub fn new(config: RaftConfig) -> Result<Raft> {
        Ok(Raft {
            id: config.server_addr,
            state: RwLock::new(State::Follower {
                storage: Box::new(InMemoryStorage::default()),
                commit_index: 0,
                last_applied: 0,
                known_peers: config.peers,
                last_contact_time: Instant::now(),
            }),
            majority: 3,
        })
    }

    pub fn status(&self) -> Result<Status> {
        let state = self.state.read().expect("Poisoned read lock in status");
        Ok(Status {
            leader: None,
            peers: vec![],
            last_log_term: None,
            last_log_index: None,
            commit_index: state.commit_index(),
            last_applied: state.last_applied(),
        })
    }

    pub fn append_entries(&self, _req: AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        Ok(AppendEntriesResponse {
            success: false,
            term: 0,
        })
    }

    pub fn request_vote(&self, _req: RequestVoteRequest) -> Result<RequestVoteResponse> {
        Ok(RequestVoteResponse {
            vote_granted: false,
            term: 0,
        })
    }

    pub fn update(&self) -> Result<()> {
        debug!("Running update");
        Ok(())
    }
}
