use crate::config;
use crate::raft::{LogEntry, ServerId, Storage, Term};
use std::error;
use std::fmt;

pub fn new(cfg: &config::RaftConfig) -> Box<dyn Storage<E = Error>> {
    let s = InMemoryStorage::new();
    Box::new(s)
}

#[derive(Debug)]
pub enum Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {}
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {}
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {}
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct InMemoryStorage {
    currentTerm: Term,
    votedFor: Option<ServerId>,
    logEntries: Vec<LogEntry>,
}

impl InMemoryStorage {
    fn new() -> InMemoryStorage {
        InMemoryStorage {
            currentTerm: 0,
            votedFor: None,
            logEntries: Vec::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    type E = Error;
    fn current_term(&self) -> Result<Term> {
        Ok(self.currentTerm)
    }
    fn set_current_term(&mut self, t: Term) -> Result<()> {
        self.currentTerm = t;
        Ok(())
    }

    fn voted_for(&self) -> Result<Option<ServerId>> {
        Ok(self.votedFor.clone())
    }
    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> Result<()> {
        self.votedFor = candidate;
        Ok(())
    }

    fn logs(&self) -> Result<std::slice::Iter<LogEntry>> {
        Ok(self.logEntries.iter())
    }
    fn apppend(&mut self, logs: Vec<LogEntry>) -> Result<()> {
        self.logEntries.extend(logs);
        Ok(())
    }
}
