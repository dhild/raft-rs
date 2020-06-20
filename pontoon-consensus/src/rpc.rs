use async_trait::async_trait;
use log::{debug, error};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::Result;
use crate::storage::{LogEntry, Storage};
use std::sync::mpsc::Sender;

pub struct AppendEntriesRequest {
    pub leader_id: String,
    pub term: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub leader_commit: usize,
    pub entries: Vec<LogEntry>,
}

pub struct AppendEntriesResponse {
    pub success: bool,
    pub term: usize,
}

impl AppendEntriesResponse {
    pub fn failed(term: usize) -> AppendEntriesResponse {
        AppendEntriesResponse {
            success: false,
            term,
        }
    }
    pub fn success(term: usize) -> AppendEntriesResponse {
        AppendEntriesResponse {
            success: true,
            term,
        }
    }
}

#[derive(Clone)]
pub struct RequestVoteRequest {
    pub term: usize,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

pub struct RequestVoteResponse {
    pub success: bool,
    pub term: usize,
}

impl RequestVoteResponse {
    pub fn failed(term: usize) -> RequestVoteResponse {
        RequestVoteResponse {
            success: false,
            term,
        }
    }
    pub fn success(term: usize) -> RequestVoteResponse {
        RequestVoteResponse {
            success: true,
            term,
        }
    }
}

#[async_trait]
pub trait RPC: Clone + Send + 'static {
    async fn append_entries(
        &self,
        peer_address: String,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse>;

    async fn request_vote(
        &self,
        peer_address: String,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse>;
}

pub struct RaftServer<S: Storage> {
    storage: Arc<RwLock<S>>,
    term_updates: Sender<usize>,
    commit_updates: Sender<usize>,
}

impl<S: Storage> RaftServer<S> {
    pub fn append_entries(&mut self, req: AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        let mut storage = self.storage.write().expect("poisoned lock");

        let current_term = storage.current_term()?;
        // Ensure we are on the latest term
        if req.term > current_term {
            self.term_updates
                .send(req.term)
                .unwrap_or_else(|_| error!("Processing AppendEntries after protocol has stopped"));
            storage.set_current_term(req.term)?;
            return Ok(AppendEntriesResponse::failed(current_term));
        }
        // Ensure the leader is on the latest term
        if req.term < current_term {
            return Ok(AppendEntriesResponse::failed(current_term));
        }
        // Ensure our previous entries match
        if let Some(term) = storage.get_term(req.prev_log_index)? {
            if term != req.prev_log_term {
                return Ok(AppendEntriesResponse::failed(current_term));
            }
        }
        // Remove any conflicting log entries
        for e in req.entries.iter() {
            match storage.get_term(e.index)? {
                None => break,
                Some(x) if x != e.term => {}
                _ => {
                    storage.remove_entries_starting_at(e.index)?;
                    break;
                }
            }
        }
        // Append any new entries
        let mut index = storage.last_index()?;
        for e in req.entries {
            if e.index > index {
                storage.append_entry(e.term, e.command)?;
                index = e.index;
            }
        }

        // Update the commit index with the latest committed value in our logs
        self.commit_updates
            .send(req.leader_commit.min(index))
            .unwrap_or_else(|_| error!("Processing AppendEntries after protocol has stopped"));

        Ok(AppendEntriesResponse::success(current_term))
    }

    pub fn request_vote(&mut self, req: RequestVoteRequest) -> Result<RequestVoteResponse> {
        let mut storage = self.storage.write().expect("poisoned lock");

        // Ensure we are on the latest term - if we are not, update and continue processing the request.
        let current_term = {
            let current_term = storage.current_term()?;
            if req.term > current_term {
                self.term_updates.send(req.term).unwrap_or_else(|_| {
                    error!("Processing AppendEntries after protocol has stopped")
                });
                storage.set_current_term(req.term)?;
                req.term
            } else {
                current_term
            }
        };
        // Ensure the candidate is on the latest term
        if req.term < current_term {
            return Ok(RequestVoteResponse::failed(current_term));
        }
        // Make sure we didn't vote for a different candidate already
        if storage
            .voted_for()?
            .map(|c| &c == &req.candidate_id)
            .unwrap_or(true)
        {
            // Grant the vote as long as their log is up to date.
            // Raft determines which of two logs is more up-to-date by comparing the index
            // and term of the last entries in the logs. If the logs have last entries with
            // different terms, then the log with the later term is more up-to-date. If the
            // logs end with the same term, then whichever log is longer is more up-to-date.
            let term = storage.last_term()?;
            let index = storage.last_index()?;
            if term <= req.last_log_term && index <= req.last_log_index {
                debug!("Voting for {} in term {}", &req.candidate_id, req.term);
                storage.set_voted_for(Some(req.candidate_id))?;
                return Ok(RequestVoteResponse::success(current_term));
            }
        }
        Ok(RequestVoteResponse::failed(current_term))
    }
}
