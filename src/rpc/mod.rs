use crate::protocol::{ProtocolState, RaftConfiguration};
use crate::state::{Command, Query, QueryResponse, StateMachine};
use crate::storage::{LogCommand, LogEntry, Storage};
use async_channel::Sender;
use async_lock::Lock;
use async_trait::async_trait;
use log::{debug, error, info};
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientApplyResponse {
    pub leader_address: Option<String>,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientQueryResponse {
    pub leader_address: Option<String>,
    pub response: Option<QueryResponse>,
}

pub trait RPCBuilder {
    type RPC: RPC;
    fn build(&self, server: RaftServer) -> std::io::Result<Arc<dyn RPC>>;
}

#[async_trait]
pub trait RPC: Send + Sync {
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
pub struct RaftServer {
    configuration: Lock<RaftConfiguration>,
    storage: Lock<Box<dyn Storage>>,
    term_updates: Sender<()>,
    append_entries_tx: Sender<usize>,
    current_state: Lock<ProtocolState>,
    new_logs_tx: Sender<usize>,
    last_applied_tx: Lock<Vec<Sender<usize>>>,
    state_machine: Lock<StateMachine>,
}

impl RaftServer {
    pub fn new(
        configuration: Lock<RaftConfiguration>,
        storage: Lock<Box<dyn Storage>>,
        term_updates: Sender<()>,
        append_entries_tx: Sender<usize>,
        current_state: Lock<ProtocolState>,
        new_logs_tx: Sender<usize>,
        last_applied_tx: Lock<Vec<Sender<usize>>>,
        state_machine: Lock<StateMachine>,
    ) -> RaftServer {
        RaftServer {
            configuration,
            storage,
            term_updates,
            append_entries_tx,
            current_state,
            new_logs_tx,
            last_applied_tx,
            state_machine,
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
        // Update the current leader:
        {
            let mut configuration = self.configuration.lock().await;
            if !configuration.is_current_leader(&req.leader_id) {
                info!(
                    "Updating local leader to point to the current leader {}",
                    req.leader_id
                );

                configuration.set_current_leader(&req.leader_id);
            }
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
        drop(storage);

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
            .map(|c| c == req.candidate_id)
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

    pub async fn apply(&self, cmd: Command) -> Result<ClientApplyResponse, RaftServerError> {
        {
            match *self.current_state.lock().await {
                ProtocolState::Leader => (),
                ProtocolState::Shutdown => return Err(RaftServerError::RaftProtocolTerminated),
                ProtocolState::Candidate | ProtocolState::Follower => {
                    let leader_address = self.configuration.lock().await.current_leader_address();
                    debug!("Not leader, responding to apply with {:?}", leader_address);
                    return Ok(ClientApplyResponse {
                        leader_address,
                        success: false,
                    });
                }
            }
        }
        let command = LogCommand::Command(cmd);

        let index = {
            let mut storage = self.storage.lock().await;

            let term = storage.current_term()?;

            storage.append_entry(term, command)?
        };

        let (tx, rx) = async_channel::bounded(1);
        {
            let mut vec = self.last_applied_tx.lock().await;
            vec.push(tx);
        }

        self.new_logs_tx
            .send(index)
            .await
            .map_err(|_| RaftServerError::RaftProtocolTerminated)?;

        while let Ok(last_applied) = rx.recv().await {
            if last_applied >= index {
                let leader_address = self.configuration.lock().await.current_leader_address();
                return Ok(ClientApplyResponse {
                    leader_address,
                    success: true,
                });
            }
        }
        Err(RaftServerError::RaftProtocolTerminated)
    }

    pub async fn query(&self, query: Query) -> Result<ClientQueryResponse, RaftServerError> {
        {
            match *self.current_state.lock().await {
                ProtocolState::Leader => (),
                ProtocolState::Shutdown => return Err(RaftServerError::RaftProtocolTerminated),
                ProtocolState::Candidate | ProtocolState::Follower => {
                    let leader_address = self.configuration.lock().await.current_leader_address();
                    return Ok(ClientQueryResponse {
                        leader_address,
                        response: None,
                    });
                }
            }
        }
        // TODO: Send heartbeat, wait for majority to respond
        // After majority responds, query state machine

        let leader_address = { self.configuration.lock().await.current_leader_address() };
        let state_machine = self.state_machine.lock().await;
        let response = state_machine.query(query);
        Ok(ClientQueryResponse {
            leader_address,
            response: Some(response),
        })
    }
}

#[derive(Debug)]
pub enum RaftServerError {
    RaftProtocolTerminated,
    StorageError(std::io::Error),
}

impl std::fmt::Display for RaftServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftServerError::RaftProtocolTerminated => {
                write!(f, "The raft protocol has been terminated")
            }
            RaftServerError::StorageError(e) => {
                write!(f, "Error while interacting with stable storage: {}", e)
            }
        }
    }
}

impl std::error::Error for RaftServerError {}

impl From<std::io::Error> for RaftServerError {
    fn from(e: std::io::Error) -> Self {
        RaftServerError::StorageError(e)
    }
}

#[cfg(feature = "http-rpc")]
mod http;

#[cfg(feature = "http-rpc")]
pub use http::{HttpClient, HttpClientConfig, HttpRPC};
use std::sync::Arc;
