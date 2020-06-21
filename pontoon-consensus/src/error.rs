#[derive(Debug)]
pub enum Error {
    NotLeader,
    RaftProtocolTerminated,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotLeader => write!(f, "This raft instance is not the leader"),
            Error::RaftProtocolTerminated => write!(f, "The raft protocol has been terminated"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
