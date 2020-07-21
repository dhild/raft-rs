use crate::state::{Command, KVCommand, KVQuery, KVQueryResponse, Query, QueryResponse};
use crate::storage::LogEntry;
use async_trait::async_trait;
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

#[async_trait]
pub trait RaftServerRPC: Send + Sync {
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

#[async_trait::async_trait]
pub trait RaftClientRPC {
    async fn apply(&mut self, cmd: Command) -> Result<(), ClientError>;
    async fn query(&mut self, query: Query) -> Result<QueryResponse, ClientError>;
}

pub struct Client {
    client: Box<dyn RaftClientRPC>,
}

impl Client {
    pub(crate) fn new(client: Box<dyn RaftClientRPC>) -> Client {
        Client { client }
    }
    pub async fn apply(&mut self, cmd: Command) -> Result<(), ClientError> {
        self.client.apply(cmd).await
    }
    pub async fn query(&mut self, query: Query) -> Result<QueryResponse, ClientError> {
        self.client.query(query).await
    }

    pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), ClientError> {
        self.apply(
            KVCommand::Put {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
            }
            .into(),
        )
        .await
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, ClientError> {
        match self
            .query(
                KVQuery::Get {
                    key: key.to_string(),
                }
                .into(),
            )
            .await?
        {
            QueryResponse::KV(KVQueryResponse::Get { value }) => Ok(value),
            _ => unreachable!("Invalid response from the server"),
        }
    }
}

#[derive(Debug)]
pub enum ClientError {
    NoLeader,
    RaftProtocolTerminated,
    MaxRetriesReached,
    IOError(std::io::Error),
    SerializationError(serde_json::Error),
    InvalidRequestError(hyper::http::Error),
    HttpError(hyper::Error),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::NoLeader => write!(f, "The raft cluster does not have a leader"),
            ClientError::RaftProtocolTerminated => {
                write!(f, "The raft protocol has been terminated")
            }
            ClientError::MaxRetriesReached => {
                write!(f, "The maximum number of retries has been reached")
            }
            ClientError::IOError(e) => write!(f, "{}", e),
            ClientError::SerializationError(e) => write!(f, "{}", e),
            ClientError::InvalidRequestError(e) => write!(f, "{}", e),
            ClientError::HttpError(e) => write!(f, "{}", e),
        }
    }
}

impl From<hyper::Error> for ClientError {
    fn from(e: hyper::Error) -> Self {
        ClientError::HttpError(e)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(e: serde_json::Error) -> Self {
        ClientError::SerializationError(e)
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::IOError(e)
    }
}

impl From<hyper::http::Error> for ClientError {
    fn from(e: hyper::http::Error) -> Self {
        ClientError::InvalidRequestError(e)
    }
}

impl std::error::Error for ClientError {}

#[cfg(feature = "http-rpc")]
mod http;

use bytes::Bytes;
#[cfg(feature = "http-rpc")]
pub use http::{HttpClient, HttpClientConfig, HttpRPC};
