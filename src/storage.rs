#[derive(Clone)]
pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub command: LogCommand,
}

#[derive(Clone)]
pub enum LogCommand {
    Command(Vec<u8>),
    Noop,
}

pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + Sized + 'static;
    fn current_term(&self) -> Result<usize, Self::Error>;
    fn set_current_term(&mut self, current_term: usize) -> Result<(), Self::Error>;
    fn voted_for(&self) -> Result<Option<String>, Self::Error>;
    fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<(), Self::Error>;
    fn last_index(&self) -> Result<usize, Self::Error>;
    fn last_term(&self) -> Result<usize, Self::Error>;
    fn append_entry(&mut self, term: usize, command: LogCommand) -> Result<usize, Self::Error>;
    fn get_term(&self, log_index: usize) -> Result<Option<usize>, Self::Error>;
    fn get_command(&self, log_index: usize) -> Result<LogCommand, Self::Error>;
    fn remove_entries_starting_at(&mut self, log_index: usize) -> Result<(), Self::Error>;
}

#[cfg(feature = "memory-storage")]
mod memory {
    use crate::storage::{LogCommand, LogEntry, Storage};
    use std::convert::Infallible;

    pub struct MemoryStorage {
        current_term: usize,
        voted_for: Option<String>,
        entries: Vec<LogEntry>,
    }

    impl Storage for MemoryStorage {
        type Error = Infallible;

        fn current_term(&self) -> Result<usize, Infallible> {
            Ok(self.current_term)
        }

        fn set_current_term(&mut self, current_term: usize) -> Result<(), Infallible> {
            self.current_term = current_term;
            Ok(())
        }

        fn voted_for(&self) -> Result<Option<String>, Infallible> {
            Ok(self.voted_for.clone())
        }

        fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<(), Infallible> {
            self.voted_for = candidate_id;
            Ok(())
        }

        fn last_index(&self) -> Result<usize, Infallible> {
            Ok(self.entries.last().map(|e| e.index).unwrap_or(0))
        }

        fn last_term(&self) -> Result<usize, Infallible> {
            Ok(self.entries.last().map(|e| e.term).unwrap_or(0))
        }

        fn append_entry(&mut self, term: usize, command: LogCommand) -> Result<usize, Infallible> {
            let index = self.entries.len() + 1;
            self.entries.push(LogEntry {
                index,
                term,
                command,
            });
            Ok(index)
        }

        fn get_term(&self, log_index: usize) -> Result<Option<usize>, Infallible> {
            Ok(self.entries.get(log_index - 1).map(|e| e.term))
        }

        fn get_command(&self, log_index: usize) -> Result<LogCommand, Infallible> {
            Ok(self
                .entries
                .get(log_index - 1)
                .map(|e| e.command.clone())
                .unwrap())
        }

        fn remove_entries_starting_at(&mut self, log_index: usize) -> Result<(), Infallible> {
            self.entries.retain(|l| l.index < log_index);
            Ok(())
        }
    }
}
