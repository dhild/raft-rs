use std::cmp::Ordering;
use std::net::SocketAddr;

pub type Term = u32;
pub type LogIndex = usize;

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: ServerId,
    pub address: SocketAddr,
}

#[derive(Debug, Clone)]
pub enum StateCommand {
    Put(String, Vec<u8>),
    Delete(String),
}

#[derive(Debug, Clone)]
pub struct Status {
    pub leader: Option<ServerId>,
    pub peers: Vec<Peer>,
    pub last_log_term: Option<Term>,
    pub last_log_index: Option<LogIndex>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}
