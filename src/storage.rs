use crate::error::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct LogEntry<C: Command> {
    pub term: usize,
    pub index: usize,
    #[serde(bound(deserialize = "C: Command"))]
    pub command: LogCommand<C>,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum LogCommand<C: Command> {
    #[serde(bound(deserialize = "C: Command"))]
    Command(C),
    Noop,
}

pub trait Command: Serialize + DeserializeOwned + Clone + Send + Sync {}

pub trait Storage<C: Command>: Send + Sync + 'static {
    fn current_term(&self) -> Result<usize>;
    fn set_current_term(&mut self, current_term: usize) -> Result<()>;
    fn voted_for(&self) -> Result<Option<String>>;
    fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<()>;
    fn last_index(&self) -> Result<usize>;
    fn last_term(&self) -> Result<usize>;
    fn append_entry(&mut self, term: usize, command: LogCommand<C>) -> Result<usize>;
    fn get_term(&self, log_index: usize) -> Result<Option<usize>>;
    fn get_command(&self, log_index: usize) -> Result<LogCommand<C>>;
    fn remove_entries_starting_at(&mut self, log_index: usize) -> Result<()>;
}

#[cfg(feature = "memory-storage")]
mod memory {
    use crate::error::Result;
    use crate::storage::{Command, LogCommand, LogEntry, Storage};

    pub struct MemoryStorage<C: Command> {
        current_term: usize,
        voted_for: Option<String>,
        entries: Vec<LogEntry<C>>,
    }

    impl<C: Command + 'static> Storage<C> for MemoryStorage<C> {
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

        fn append_entry(&mut self, term: usize, command: LogCommand<C>) -> Result<usize> {
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

        fn get_command(&self, log_index: usize) -> Result<LogCommand<C>> {
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
