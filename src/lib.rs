//! # Pontoon Consensus
//!
//! `pontoon_consensus` provides the core consensus module.

mod protocol;
mod rpc;
mod state;
mod storage;

pub use crate::rpc::Client;

use crate::protocol::ServerConfig;
use crate::rpc::{HttpClient, RaftClientRPC};

pub fn server(id: &str, address: &str) -> ServerConfig {
    ServerConfig::new(id, address)
}

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
        let client = HttpClient::new(self.address.clone(), self.max_retries.unwrap_or(5));
        Client::new(Box::new(client) as Box<dyn RaftClientRPC>)
    }
}
