use crate::raft::*;
use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Error {}

impl fmt::Display for Error {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
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
    current_term: Term,
    voted_for: Option<ServerId>,
    log_entries: Vec<LogEntry>,
}

impl InMemoryStorage {
    pub fn new() -> InMemoryStorage {
        InMemoryStorage {
            current_term: 0,
            voted_for: None,
            log_entries: Vec::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    type E = Error;
    fn current_term(&self) -> Result<Term> {
        Ok(self.current_term)
    }
    fn set_current_term(&mut self, t: Term) -> Result<()> {
        self.current_term = t;
        Ok(())
    }

    fn voted_for(&self) -> Result<Option<ServerId>> {
        Ok(self.voted_for)
    }
    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> Result<()> {
        self.voted_for = candidate;
        Ok(())
    }

    fn contains(&self, term: Term, index: LogIndex) -> Result<bool> {
        Ok(self
            .log_entries
            .iter()
            .any(|x| x.index == index && x.term == term))
    }
    fn append(&mut self, mut logs: Vec<LogEntry>) -> Result<()> {
        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it.
        if let Some(min_conflict) = logs
            .iter()
            .filter(|x| {
                self.log_entries
                    .iter()
                    .any(|y| y.index == x.index && y.term != x.term)
            })
            .min()
        {
            self.log_entries.retain(|x| x.index < min_conflict.index);
        }
        // Append any new entries not already in the log
        self.log_entries.append(&mut logs);
        self.log_entries.sort();
        self.log_entries.dedup();
        Ok(())
    }

    fn last_log_entry(&self) -> Result<Option<(LogIndex, Term)>> {
        Ok(self
            .log_entries
            .iter()
            .max()
            .map(|last| (last.index, last.term)))
    }

    fn get_entries_from(&self, index: LogIndex) -> Result<Vec<LogEntry>> {
        Ok(self
            .log_entries
            .iter()
            .filter(|entry| entry.index >= index)
            .cloned()
            .collect())
    }

    fn get_term(&self, index: LogIndex) -> Result<Option<Term>> {
        Ok(self
            .log_entries
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term))
    }
}
