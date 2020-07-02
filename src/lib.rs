//! # Pontoon Consensus
//!
//! `pontoon_consensus` provides the core consensus module.
#![recursion_limit = "512"]

pub mod error;
mod protocol;
pub mod rpc;
pub mod state;
pub mod storage;

pub use protocol::{start, Consensus};
pub use state::StateMachine;
