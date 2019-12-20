//! Types for working with log data that raft uses to represent the current state.
use std::cmp::Ordering;

#[cfg(feature = "http-transport")]
use serde::{Deserialize, Serialize};

use crate::configuration::ConfigurationChange;

pub type Term = u32;
pub type LogIndex = usize;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: LogCommand,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub enum LogCommand {
    Noop,
    ConfigurationChange(ConfigurationChange),
    StateMachine(Vec<u8>),
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

/// A form of long-term storage for raft log entries.
///
/// For the specification to work correctly, modification functions should only return once the
/// write is persistent on the backing storage.
pub trait Storage: Sized + Send {
    type Error: ::std::error::Error;

    fn current_term(&self) -> Term;
    fn set_current_term(&mut self, t: Term) -> Result<(), Self::Error>;

    fn voted_for(&self) -> Option<String>;
    fn set_voted_for(&mut self, candidate: Option<String>) -> Result<(), Self::Error>;

    fn last_log_entry(&self) -> Option<(LogIndex, Term)>;

    fn get(&self, index: LogIndex) -> Result<Option<LogEntry>, Self::Error>;

    fn delete_logs(&mut self, start: LogIndex) -> Result<(), Self::Error>;
    fn store_logs(&mut self, logs: Vec<LogEntry>) -> Result<(), Self::Error>;
}

/// Storage without true persistence; all data is lost on process restart.
pub struct InMemoryStorage {
    term: Term,
    voted_for: Option<String>,
    log_entries: Vec<LogEntry>,
}

impl Storage for InMemoryStorage {
    type Error = !;

    fn current_term(&self) -> Term {
        self.term
    }
    fn set_current_term(&mut self, t: Term) -> Result<(), Self::Error> {
        self.term = t;
        Ok(())
    }

    fn voted_for(&self) -> Option<String> {
        self.voted_for.clone()
    }
    fn set_voted_for(&mut self, candidate: Option<String>) -> Result<(), Self::Error> {
        self.voted_for = candidate;
        Ok(())
    }

    fn last_log_entry(&self) -> Option<(LogIndex, Term)> {
        self.log_entries.last().map(|x| (x.index, x.term))
    }

    fn get(&self, index: LogIndex) -> Result<Option<LogEntry>, Self::Error> {
        Ok(self.log_entries.get(index).cloned())
    }

    fn delete_logs(&mut self, start: LogIndex) -> Result<(), Self::Error> {
        self.log_entries.retain(|x| x.index < start);
        Ok(())
    }
    fn store_logs(&mut self, mut logs: Vec<LogEntry>) -> Result<(), Self::Error> {
        self.log_entries.append(&mut logs);
        Ok(())
    }
}

impl Default for InMemoryStorage {
    fn default() -> InMemoryStorage {
        InMemoryStorage {
            term: 0,
            voted_for: None,
            log_entries: Vec::default(),
        }
    }
}
