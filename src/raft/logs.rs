use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::error;
use std::result;
use std::slice;

pub type Term = u32;
pub type ServerId = String;
pub type LogCommand = String;
pub type LogIndex = usize;

#[derive(Serialize, Deserialize, Debug, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: LogCommand,
}

pub trait Storage {
    type E: error::Error;

    fn current_term(&self) -> result::Result<Term, Self::E>;
    fn set_current_term(&mut self, t: Term) -> result::Result<(), Self::E>;

    fn voted_for(&self) -> result::Result<Option<ServerId>, Self::E>;
    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> result::Result<(), Self::E>;

    fn contains(&self, term: Term, index: LogIndex) -> result::Result<bool, Self::E>;
    fn append(&mut self, logs: Vec<LogEntry>) -> result::Result<(), Self::E>;
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

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.term == other.term
    }
}
