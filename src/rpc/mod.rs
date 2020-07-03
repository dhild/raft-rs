use crate::storage::{LogEntry, Storage};
use async_channel::Sender;
use async_lock::Lock;
use async_trait::async_trait;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppendEntriesRequest {
    pub leader_id: String,
    pub term: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub leader_commit: usize,
    pub entries: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RequestVoteRequest {
    pub term: usize,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

pub trait RPCBuilder {
    type RPC: RPC;
    fn build<S: Storage>(&self, server: RaftServer<S>) -> std::io::Result<Self::RPC>;
}

#[async_trait]
pub trait RPC: Clone + Send + Sync + 'static {
    async fn append_entries(
        &self,
        peer_address: String,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn StdError + Send + Sync>>;

    async fn request_vote(
        &self,
        peer_address: String,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn StdError + Send + Sync>>;
}

#[derive(Clone)]
pub struct RaftServer<S: Storage> {
    storage: Lock<S>,
    term_updates: Sender<()>,
    append_entries_tx: Sender<usize>,
}

impl<S: Storage> RaftServer<S> {
    pub fn new(
        storage: Lock<S>,
        term_updates: Sender<()>,
        append_entries_tx: Sender<usize>,
    ) -> RaftServer<S> {
        RaftServer {
            storage,
            term_updates,
            append_entries_tx,
        }
    }

    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> std::io::Result<AppendEntriesResponse> {
        let mut storage = self.storage.lock().await;

        let current_term = storage.current_term()?;
        // Ensure we are on the latest term
        if req.term > current_term {
            storage.set_current_term(req.term)?;
            storage.set_voted_for(None)?;
            if self.term_updates.send(()).await.is_err() {
                error!("Raft protocol has been shut down");
            }
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
        if self
            .append_entries_tx
            .send(req.leader_commit.min(index))
            .await
            .is_err()
        {
            error!("Raft protocol has been shut down");
            return Ok(AppendEntriesResponse::failed(current_term));
        }

        Ok(AppendEntriesResponse::success(current_term))
    }

    pub async fn request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> std::io::Result<RequestVoteResponse> {
        let mut storage = self.storage.lock().await;

        // Ensure we are on the latest term - if we are not, update and continue processing the request.
        let current_term = {
            let current_term = storage.current_term()?;
            if req.term > current_term {
                storage.set_current_term(req.term)?;
                if self.term_updates.send(()).await.is_err() {
                    error!("Raft protocol has been shut down");
                    return Ok(RequestVoteResponse::failed(current_term));
                }
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

#[cfg(feature = "http-rpc")]
mod http;

#[cfg(feature = "http-rpc")]
pub use http::{HttpConfig, HttpRPC};
