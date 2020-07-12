//! # Pontoon Consensus
//!
//! `pontoon_consensus` provides the core consensus module.
#![recursion_limit = "512"]

mod client;
mod protocol;
mod rpc;
mod state;
mod storage;

use crate::protocol::{LogCommitter, Peer, ProtocolState, ProtocolTasks, RaftConfiguration};
use crate::rpc::{HttpRPC, RaftServer};
use crate::state::StateMachine;
use crate::storage::{MemoryStorage, Storage};
use async_lock::Lock;
pub use client::{ClientConfig, RaftClient as Client};
use log::debug;
use std::sync::Arc;
use std::time::Duration;

pub fn server(id: &str, address: &str) -> ServerBuilder {
    ServerBuilder::new(id, address)
}

pub struct ServerBuilder {
    id: String,
    address: String,
    peers: Vec<Peer>,
    timeout: Option<Duration>,
    storage: Option<Lock<Box<dyn Storage>>>,
}

impl ServerBuilder {
    pub fn new(id: &str, address: &str) -> ServerBuilder {
        ServerBuilder {
            id: id.to_string(),
            address: address.to_string(),
            peers: Vec::new(),
            timeout: None,
            storage: None,
        }
    }

    pub fn peer(&mut self, id: &str, address: &str) -> &mut Self {
        self.peers.push(Peer {
            id: id.to_string(),
            address: address.to_string(),
            voting: true,
        });
        self
    }

    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn in_memory_storage(&mut self) -> &mut Self {
        self.storage = Some(Lock::new(Box::new(MemoryStorage::new())));
        self
    }

    #[cfg(feature = "http-rpc")]
    pub fn spawn_http(&mut self) -> Result<(), std::io::Error> {
        let id = self.id.clone();
        let storage = self.storage.clone().expect("no storage device configured");
        self.storage = None;
        let timeout = self.timeout.unwrap_or_else(|| Duration::from_millis(150));
        let configuration = Lock::new(RaftConfiguration::new(
            id.clone(),
            self.address.clone(),
            self.peers.clone(),
        ));
        let state_machine = Lock::new(StateMachine::default());
        let current_state = Lock::new(ProtocolState::Follower);
        let last_applied_tx = Lock::new(Vec::new());
        let (commits_to_apply_tx, commits_to_apply_rx) = async_channel::bounded(1);
        let (append_entries_tx, append_entries_rx) = async_channel::bounded(1);
        let (term_updates_tx, term_updates_rx) = async_channel::bounded(1);
        let (new_logs_tx, new_logs_rx) = async_channel::bounded(1);

        let raft_server = RaftServer::new(
            configuration.clone(),
            storage.clone(),
            term_updates_tx.clone(),
            append_entries_tx,
            current_state.clone(),
            new_logs_tx,
            last_applied_tx.clone(),
            state_machine.clone(),
        );

        let rpc = HttpRPC::spawn_server(&self.address, raft_server)?;

        tokio::spawn(async move {
            debug!("Starting log committer task");
            let mut lc = LogCommitter::new(state_machine, commits_to_apply_rx, last_applied_tx);
            lc.run().await;
            debug!("Closing log committer task");
        });
        tokio::spawn(async move {
            let mut tasks = ProtocolTasks::new(
                id,
                configuration,
                timeout,
                storage,
                current_state,
                Arc::new(rpc),
                term_updates_tx,
                term_updates_rx,
                commits_to_apply_tx,
                append_entries_rx,
                new_logs_rx,
            );
            debug!("Starting raft protocol task");
            tasks.run().await;
            debug!("Closing raft protocol task");
        });
        Ok(())
    }
}
