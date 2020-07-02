use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub command: LogCommand,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LogCommand {
    Command(Vec<u8>),
    Noop,
}

pub trait Storage: Sized + Send + Sync + 'static {
    type Config: Serialize + DeserializeOwned + Clone;

    fn new(config: Self::Config) -> std::io::Result<Self>;

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

#[cfg(feature = "memory-storage")]
pub use memory::{MemoryConfig, MemoryStorage};

#[cfg(feature = "memory-storage")]
mod memory {
    use crate::storage::{LogCommand, LogEntry, Storage};
    use serde::{Deserialize, Serialize};
    use std::io::Result;

    pub struct MemoryStorage {
        current_term: usize,
        voted_for: Option<String>,
        entries: Vec<LogEntry>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct MemoryConfig {}

    impl Storage for MemoryStorage {
        type Config = MemoryConfig;

        fn new(config: MemoryConfig) -> Result<MemoryStorage> {
            Ok(MemoryStorage {
                current_term: 0,
                voted_for: None,
                entries: Vec::new(),
            })
        }

        fn current_term(&self) -> Result<usize> {
            Ok(self.current_term)
        }

        fn set_current_term(&mut self, current_term: usize) -> Result<()> {
            self.current_term = current_term;
            Ok(())
        }

        fn voted_for(&self) -> Result<Option<String>> {
            Ok(self.voted_for.clone())
        }

        fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<()> {
            self.voted_for = candidate_id;
            Ok(())
        }

        fn last_index(&self) -> Result<usize> {
            Ok(self.entries.last().map(|e| e.index).unwrap_or(0))
        }

        fn last_term(&self) -> Result<usize> {
            Ok(self.entries.last().map(|e| e.term).unwrap_or(0))
        }

        fn append_entry(&mut self, term: usize, command: LogCommand) -> Result<usize> {
            let index = self.entries.len() + 1;
            self.entries.push(LogEntry {
                index,
                term,
                command,
            });
            Ok(index)
        }

        fn get_term(&self, log_index: usize) -> Result<Option<usize>> {
            Ok(self.entries.get(log_index - 1).map(|e| e.term))
        }

        fn get_command(&self, log_index: usize) -> Result<LogCommand> {
            Ok(self
                .entries
                .get(log_index - 1)
                .map(|e| e.command.clone())
                .unwrap())
        }

        fn remove_entries_starting_at(&mut self, log_index: usize) -> Result<()> {
            self.entries.retain(|l| l.index < log_index);
            Ok(())
        }
    }
}
