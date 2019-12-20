use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::time::{Duration, Instant};

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
    pub last_contact_time: Instant,
}

impl Follower {
    pub fn to_candidate(&self) -> Candidate {
        Candidate {
            election_start: Instant::now(),
        }
    }

    pub fn duration_since_last_contact(&self) -> Duration {
        Instant::now().duration_since(self.last_contact_time)
    }
}

pub struct Candidate {
    pub election_start: Instant,
}

impl Candidate {
    pub fn to_follower(&self) -> Follower {
        Follower {
            last_contact_time: Instant::now(),
        }
    }
    pub fn to_leader(&self) -> Leader {
        Leader {
            servers: HashMap::new(),
        }
    }
    pub fn new_election(&self) -> Candidate {
        Candidate {
            election_start: Instant::now(),
        }
    }

    pub fn duration_since_start(&self) -> Duration {
        Instant::now().duration_since(self.election_start)
    }
}

pub struct Leader {
    servers: HashMap<String, Sender<AppendEntriesRequest>>,
}

impl Leader {
    pub fn to_follower(&self) -> Follower {
        Follower {
            last_contact_time: Instant::now(),
        }
    }

    pub fn add_server(&mut self, id: &str, sender: Sender<AppendEntriesRequest>) {
        self.servers.insert(id.to_string(), sender);
    }
}
