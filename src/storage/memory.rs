use crate::storage::{LogCommand, LogEntry, Storage};
use serde::{Deserialize, Serialize};
use std::io::Result;

pub struct MemoryStorage {
    current_term: usize,
    voted_for: Option<String>,
    entries: Vec<LogEntry>,
}

impl MemoryStorage {
    pub fn new() -> MemoryStorage {
        MemoryStorage {
            current_term: 0,
            voted_for: None,
            entries: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MemoryConfig {}

impl From<&MemoryConfig> for MemoryStorage {
    fn from(_config: &MemoryConfig) -> Self {
        MemoryStorage::new()
    }
}

impl Storage for MemoryStorage {
    fn current_term(&self) -> Result<usize> {
        Ok(self.current_term)
    }

    fn set_current_term(&mut self, current_term: usize) -> Result<()> {
        if current_term != self.current_term {
            self.voted_for = None;
        }
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
        if log_index == 0 {
            return Ok(Some(0));
        }
        Ok(self.entries.get(log_index - 1).map(|e| e.term))
    }

    fn get_command(&self, log_index: usize) -> Result<LogCommand> {
        if log_index == 0 {
            return Ok(LogCommand::Noop);
        }
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

#[cfg(test)]
mod tests {
    use crate::storage::{LogCommand, MemoryStorage};
    use crate::Storage;

    #[test]
    fn append_entries() {
        let mut storage = MemoryStorage::new();
        storage.append_entry(1, LogCommand::Noop).unwrap();
        storage.append_entry(1, LogCommand::Noop).unwrap();
        storage.append_entry(1, LogCommand::Noop).unwrap();

        assert_eq!(storage.last_index().unwrap(), 3);
        assert_eq!(storage.last_term().unwrap(), 1);
        assert_eq!(storage.get_command(1).unwrap(), LogCommand::Noop);
        assert_eq!(storage.get_command(2).unwrap(), LogCommand::Noop);
        assert_eq!(storage.get_command(3).unwrap(), LogCommand::Noop);
    }
}
