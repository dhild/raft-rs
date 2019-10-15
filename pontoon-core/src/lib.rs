//! # Pontoon Core
//!
//! `pontoon_core` provides a basic set of raft protocol functionality.
//!
//!
//! # Examples
//!
//! Create a raft server by supplying some form of state machine and backing storage:
//! ```
//! let fsm = pontoon_core::fsm::KeyValueStore::default();
//! let storage = pontoon_core::logs::InMemoryStorage::default();
//! let transport = pontoon_core::rpc::HttpTransport::default();
//! let raft = pontoon_core::new("unique-raft-id", ([127, 0, 0, 1], 8080))
//!              .build(fsm, storage, transport);
//! ```
#![feature(never_type)]

pub extern crate crossbeam_channel;
pub extern crate futures;
extern crate log;
#[cfg(any(feature = "http-transport", feature = "key-value-store"))]
extern crate serde;
#[cfg(any(feature = "http-transport", feature = "key-value-store"))]
extern crate serde_json;
#[cfg(feature = "http-transport")]
extern crate hyper;

mod configuration;
pub mod fsm;
pub mod logs;
mod raft;
pub mod rpc;
mod state;

pub use fsm::FiniteStateMachine;
pub use logs::{LogCommand, LogEntry, LogIndex, Storage, Term};
pub use raft::{new, Raft};
pub use rpc::Transport;
