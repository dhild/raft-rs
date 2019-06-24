#[macro_use]
extern crate log;
extern crate env_logger;

use std::env;

mod config;
mod errors;
mod raft;
mod storage;

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let cfg = config::RaftConfig::new(&args);

    let store = storage::new(&cfg);

    raft::run(store);
}
