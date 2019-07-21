use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::net::SocketAddr;

pub type Term = u32;
pub type ServerId = SocketAddr;
pub type LogCommand = String;
pub type LogIndex = usize;

#[derive(Serialize, Deserialize, Debug, Eq, Clone)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: LogCommand,
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

pub type FutureAppendEntriesResponse =
    Box<dyn Send + Future<Item = (AppendEntriesRequest, AppendEntriesResponse), Error = ()>>;

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

pub type FutureRequestVoteResponse =
    Box<dyn Send + Future<Item = (RequestVoteRequest, RequestVoteResponse), Error = ()>>;

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

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.term == other.term
    }
}
