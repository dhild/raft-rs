use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::time::Instant;

use crate::configuration::ServerId;
use crate::rpc::AppendEntriesRequest;

pub enum RaftState {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
    ShuttingDown,
}

impl Default for RaftState {
    fn default() -> Self {
        RaftState::Follower(Follower {
            last_contact_time: Instant::now(),
        })
    }
}

pub struct Follower {
    last_contact_time: Instant,
}

impl Follower {
    pub fn into_candidate(&self) -> Candidate {
        Candidate {}
    }
}

pub struct Candidate {}

impl Candidate {
    pub fn into_follower(&self) -> Follower {
        Follower {
            last_contact_time: Instant::now(),
        }
    }
    pub fn into_leader(&self) -> Leader {
        Leader {
            servers: HashMap::new(),
        }
    }
}

pub struct Leader {
    servers: HashMap<ServerId, Sender<AppendEntriesRequest>>,
}

impl Leader {
    pub fn into_follower(&self) -> Follower {
        Follower {
            last_contact_time: Instant::now(),
        }
    }

    pub fn add_server(&mut self, id: &str, sender: Sender<AppendEntriesRequest>) {
        self.servers.insert(id.to_string(), sender);
    }
}
