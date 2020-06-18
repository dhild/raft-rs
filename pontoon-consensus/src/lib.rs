//! # Pontoon Consensus
//!
//! `pontoon_consensus` provides the core consensus module.
//!

pub mod raft;

pub use raft::{Error, Raft, Result};
