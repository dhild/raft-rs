use crate::error::Result;

pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub command: LogCommand,
}

pub enum LogCommand {
    Command(Vec<u8>),
    Noop,
}

pub trait Storage: Clone + Send + Sync + 'static {
    fn current_term(&self) -> Result<usize>;
    fn set_current_term(&mut self, current_term: usize) -> Result<()>;
    fn voted_for(&self) -> Result<Option<String>>;
    fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<()>;
    fn last_index(&self) -> Result<usize>;
    fn last_term(&self) -> Result<usize>;
    fn append_entry(&mut self, term: usize, command: LogCommand) -> Result<usize>;
    fn get_term(&self, log_index: usize) -> Result<Option<usize>>;
    fn get_command(&self, log_index: usize) -> Result<LogCommand>;
    fn remove_entries_starting_at(&self, log_index: usize) -> Result<()>;
}
