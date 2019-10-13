use crate::error::{Error, Result};
use pontoon_core::*;

#[derive(Debug, Clone)]
pub struct Metadata {
    current_term: Term,
    voted_for: Option<ServerId>,
    log_entries: Vec<LogEntry>,
}

pub struct InMemoryStorage {
    meta: Metadata,
}

impl InMemoryStorage {
    fn read(&self) -> Result<Metadata> {
        Ok(self.meta.clone())
    }
    fn write(&mut self, meta: Metadata) -> Result<()> {
        self.meta = meta;
        Ok(())
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        InMemoryStorage {
            meta: Metadata {
                current_term: 0,
                voted_for: None,
                log_entries: Vec::new(),
            },
        }
    }
}

impl Storage for InMemoryStorage {
    type Error = Error;

    fn current_term(&self) -> Result<Term> {
        self.read().map(|m| m.current_term)
    }

    fn set_current_term(&mut self, t: Term) -> Result<()> {
        let mut meta = self.read()?;
        meta.current_term = t;
        self.write(meta)
    }

    fn voted_for(&self) -> Result<Option<ServerId>> {
        self.read().map(|m| m.voted_for)
    }

    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> Result<()> {
        let mut meta = self.read()?;
        meta.voted_for = candidate;
        self.write(meta)
    }

    fn contains(&self, term: Term, index: LogIndex) -> Result<bool> {
        self.read().map(|m| {
            m.log_entries
                .iter()
                .any(|x| x.index == index && x.term == term)
        })
    }

    fn append(&mut self, mut logs: Vec<LogEntry>) -> Result<()> {
        let mut meta = self.read()?;
        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it.
        let conflict = logs
            .iter()
            .filter(|x| {
                meta.log_entries
                    .iter()
                    .any(|y| y.index == x.index && y.term != x.term)
            })
            .min();
        if let Some(min_conflict) = conflict {
            meta.log_entries = meta
                .log_entries
                .into_iter()
                .filter(|x| x.index < min_conflict.index)
                .collect();
        }

        // Append any new entries not already in the log
        meta.log_entries.append(&mut logs);
        meta.log_entries.sort();
        meta.log_entries.dedup();
        self.write(meta)
    }

    fn last_log_entry(&self) -> Result<Option<(LogIndex, Term)>> {
        self.read().map(|m| {
            m.log_entries
                .iter()
                .max()
                .map(|last| (last.index, last.term))
        })
    }

    fn get_entries_from(&self, index: LogIndex) -> Result<Vec<LogEntry>> {
        self.read().map(|m| {
            m.log_entries
                .iter()
                .filter(|entry| entry.index >= index)
                .cloned()
                .collect()
        })
    }

    fn get_term(&self, index: LogIndex) -> Result<Option<Term>> {
        self.read().map(|m| {
            m.log_entries
                .iter()
                .find(|entry| entry.index == index)
                .map(|entry| entry.term)
        })
    }
}
