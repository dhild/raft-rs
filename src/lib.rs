#[macro_use]
extern crate log;
extern crate futures;
extern crate hyper;
extern crate serde;
extern crate tokio_timer;

mod client;
mod config;
pub mod raft;
mod server;

pub use client::{Client, Health};
pub use config::RaftConfig;
pub use server::{new_server, ServerFuture, Error as ServerError};
