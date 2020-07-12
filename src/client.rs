use crate::rpc::{HttpClient, HttpClientConfig};
pub use crate::state::{Command, Query, QueryResponse};
use crate::state::{KVCommand, KVQuery, KVQueryResponse};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[async_trait::async_trait]
pub trait RaftClient {
    async fn apply(&mut self, cmd: Command) -> Result<(), ClientError>;
    async fn query(&mut self, query: Query) -> Result<QueryResponse, ClientError>;

    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), ClientError> {
        self.apply(
            KVCommand::Put {
                key: key.to_string(),
                value: Bytes::copy_from_slice(value),
            }
            .into(),
        )
        .await
    }

    async fn get(&mut self, key: &str) -> Result<Option<Bytes>, ClientError> {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientConfig {
    pub address: String,
    pub max_retries: Option<usize>,
    #[cfg(feature = "http-rpc")]
    pub http: Option<HttpClientConfig>,
}

impl ClientConfig {
    pub fn build(&self) -> Result<impl RaftClient, std::io::Error> {
        #[cfg(feature = "http-rpc")]
        if let Some(ref _cfg) = self.http {
            let client = HttpClient::new(self.address.clone(), self.max_retries.unwrap_or(5));
            return Ok(client);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No valid client configuration",
        ))
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

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::IOError(e)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(e: serde_json::Error) -> Self {
        ClientError::SerializationError(e)
    }
}

impl From<hyper::http::Error> for ClientError {
    fn from(e: hyper::http::Error) -> Self {
        ClientError::InvalidRequestError(e)
    }
}

impl From<hyper::Error> for ClientError {
    fn from(e: hyper::Error) -> Self {
        ClientError::HttpError(e)
    }
}
