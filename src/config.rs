use crate::protocol::Peer;
use crate::rpc::HttpConfig;
use crate::state::StateMachine;
use crate::storage::{MemoryConfig, MemoryStorage, Storage};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub id: String,
    pub peer: Option<Vec<PeerConfig>>,
    pub timeout: Option<Duration>,
    pub rpc: RPCConfig,
    pub storage: StorageConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerConfig {
    pub id: String,
    pub address: String,
    pub voting: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPCConfig {
    #[cfg(feature = "http-rpc")]
    pub http: Option<HttpConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StorageConfig {
    #[cfg(feature = "memory-storage")]
    pub memory: Option<MemoryConfig>,
}

impl Config {
    pub async fn spawn_server(&self) -> Result<(), std::io::Error> {
        let id = self.id.clone();
        let peers = self.peers();
        let timeout = self.timeout.unwrap_or(Duration::from_millis(500));

        let storage = self.storage.build()?;

        self.rpc.build(id, peers, timeout, storage).await
    }

    fn peers(&self) -> Vec<Peer> {
        if let Some(ref peers) = self.peer {
            peers
                .iter()
                .map(|p| Peer {
                    id: p.id.clone(),
                    address: p.address.clone(),
                    voting: p.voting.unwrap_or(true),
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}

impl RPCConfig {
    pub async fn build<S: Storage>(
        &self,
        id: String,
        peers: Vec<Peer>,
        timeout: Duration,
        storage: S,
    ) -> Result<(), std::io::Error> {
        #[cfg(feature = "http-rpc")]
        if let Some(ref cfg) = self.http {
            crate::protocol::start(id, peers, timeout, cfg.clone(), storage).await
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No valid storage configuration",
        ))
    }
}

impl StorageConfig {
    pub fn build(&self) -> Result<impl Storage, std::io::Error> {
        #[cfg(feature = "memory-storage")]
        if let Some(ref cfg) = self.memory {
            let storage: MemoryStorage = cfg.into();
            return Ok(storage);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No valid storage configuration",
        ))
    }
}
