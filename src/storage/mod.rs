use crate::state::Command;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub command: LogCommand,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum LogCommand {
    Command(Command),
    Noop,
}

pub trait Storage: Send + Sync {
    fn current_term(&self) -> std::io::Result<usize>;
    fn set_current_term(&mut self, current_term: usize) -> std::io::Result<()>;
    fn voted_for(&self) -> std::io::Result<Option<String>>;
    fn set_voted_for(&mut self, candidate_id: Option<String>) -> std::io::Result<()>;
    fn last_index(&self) -> std::io::Result<usize>;
    fn last_term(&self) -> std::io::Result<usize>;
    fn append_entry(&mut self, term: usize, command: LogCommand) -> std::io::Result<usize>;
    fn get_term(&self, log_index: usize) -> std::io::Result<Option<usize>>;
    fn get_command(&self, log_index: usize) -> std::io::Result<LogCommand>;
    fn remove_entries_starting_at(&mut self, log_index: usize) -> std::io::Result<()>;
}

pub use memory::{MemoryConfig, MemoryStorage};

mod memory;
