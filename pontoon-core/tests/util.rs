use std::fmt;
use std::net::SocketAddr;
use std::sync::mpsc::Receiver;

use pontoon_core::*;

#[derive(Debug, Eq, PartialEq)]
pub struct TestError;

impl ::std::error::Error for TestError {
    fn description(&self) -> &str {
        "test error"
    }
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "test error")
    }
}

type Result<T> = ::std::result::Result<T, TestError>;

#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    term: Term,
    vote: Option<ServerId>,
    logs: Vec<LogEntry>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        InMemoryStorage {
            term: 0,
            vote: None,
            logs: Vec::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    type Error = TestError;

    fn current_term(&self) -> Result<Term> {
        Ok(self.term)
    }

    fn set_current_term(&mut self, t: Term) -> Result<()> {
        self.term = t;
        Ok(())
    }

    fn voted_for(&self) -> Result<Option<ServerId>> {
        match &self.vote {
            None => Ok(None),
            Some(x) => Ok(Some(x.clone())),
        }
    }

    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> Result<()> {
        self.vote = candidate;
        Ok(())
    }

    fn append(&mut self, mut logs: Vec<LogEntry>) -> Result<()> {
        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it.
        let conflict = logs
            .iter()
            .filter(|x| {
                self.logs
                    .iter()
                    .any(|y| y.index == x.index && y.term != x.term)
            })
            .min();
        if let Some(min_conflict) = conflict {
            self.logs.retain(|x| x.index < min_conflict.index);
        }

        // Append any new entries not already in the log
        self.logs.append(&mut logs);
        self.logs.sort();
        self.logs.dedup();
        Ok(())
    }

    fn last_log_entry(&self) -> Result<Option<(LogIndex, Term)>> {
        Ok(self.logs.iter().max().map(|last| (last.index, last.term)))
    }

    fn get(&self, index: LogIndex) -> Result<&LogEntry> {
        Ok(&self.logs[index])
    }

    fn get_term(&self, index: LogIndex) -> Result<Option<Term>> {
        Ok(self
            .logs
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term))
    }
}

pub struct FakeRPC {}

impl Default for FakeRPC {
    fn default() -> Self {
        FakeRPC {}
    }
}

impl RPC for FakeRPC {
    type Error = TestError;

    fn append_entries(
        &mut self,
        peer: SocketAddr,
        req: AppendEntriesRequest,
    ) -> Receiver<std::result::Result<AppendEntriesResponse, TestError>> {
        unimplemented!();
    }

    fn request_vote(
        &mut self,
        peer: SocketAddr,
        req: RequestVoteRequest,
    ) -> Receiver<std::result::Result<RequestVoteResponse, TestError>> {
        unimplemented!();
    }
}
