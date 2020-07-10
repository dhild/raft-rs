//! # Pontoon Consensus
//!
//! `pontoon_consensus` provides the core consensus module.
#![recursion_limit = "512"]

mod client;
mod config;
mod protocol;
mod rpc;
mod state;
mod storage;

pub use client::{ClientConfig, RaftClient as Client};
pub use config::ServerConfig;
