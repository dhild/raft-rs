use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::net::SocketAddr;

pub type ServerId = SocketAddr;
pub type Term = u32;
pub type LogIndex = usize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StateCommand {
    Noop,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: StateCommand,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Status {
    pub leader: Option<ServerId>,
    pub peers: Vec<ServerId>,
    pub last_log_term: Option<Term>,
    pub last_log_index: Option<LogIndex>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: ServerId,
    pub prev_log_index: LogIndex,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: ServerId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

impl Ord for LogEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.index
            .cmp(&other.index)
            .then(self.term.cmp(&other.term))
    }
}

impl PartialOrd for LogEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LogEntry {}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.term == other.term
    }
}
