pub use crate::state::{Command, Query, QueryResponse};
use crate::state::{KVCommand, KVQuery, KVQueryResponse};
use bytes::Bytes;

pub fn client(address: &str) -> ClientBuilder {
    ClientBuilder::new(address)
}

pub struct ClientBuilder {
    address: String,
    max_retries: Option<usize>,
}

impl ClientBuilder {
    pub fn new(address: &str) -> ClientBuilder {
        ClientBuilder {
            address: address.to_string(),
            max_retries: None,
        }
    }

    pub fn max_retries(&mut self, max_retries: usize) -> &mut Self {
        self.max_retries = Some(max_retries);
        self
    }

    #[cfg(feature = "http-rpc")]
    pub fn build_http_client(&mut self) -> Client {
        use crate::rpc::HttpClient;
        let client = HttpClient::new(self.address.clone(), self.max_retries.unwrap_or(5));
        Client::new(Box::new(client) as Box<dyn RaftClient>)
    }
}

#[async_trait::async_trait]
pub trait RaftClient {
    async fn apply(&mut self, cmd: Command) -> Result<(), ClientError>;
    async fn query(&mut self, query: Query) -> Result<QueryResponse, ClientError>;
}

pub struct Client {
    client: Box<dyn RaftClient>,
}

impl Client {
    pub(crate) fn new(client: Box<dyn RaftClient>) -> Client {
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
    #[cfg(feature = "http-rpc")]
    SerializationError(serde_json::Error),
    #[cfg(feature = "http-rpc")]
    InvalidRequestError(hyper::http::Error),
    #[cfg(feature = "http-rpc")]
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
            #[cfg(feature = "http-rpc")]
            ClientError::SerializationError(e) => write!(f, "{}", e),
            #[cfg(feature = "http-rpc")]
            ClientError::InvalidRequestError(e) => write!(f, "{}", e),
            #[cfg(feature = "http-rpc")]
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

#[cfg(feature = "http-rpc")]
impl From<serde_json::Error> for ClientError {
    fn from(e: serde_json::Error) -> Self {
        ClientError::SerializationError(e)
    }
}

#[cfg(feature = "http-rpc")]
impl From<hyper::http::Error> for ClientError {
    fn from(e: hyper::http::Error) -> Self {
        ClientError::InvalidRequestError(e)
    }
}

#[cfg(feature = "http-rpc")]
impl From<hyper::Error> for ClientError {
    fn from(e: hyper::Error) -> Self {
        ClientError::HttpError(e)
    }
}
