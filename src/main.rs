#[macro_use]
extern crate log;
extern crate env_logger;
extern crate serde;

use std::env;

mod config;
mod raft;
mod storage;

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let cfg = config::RaftConfig::new(&args);

    let server = cfg.build_server();

    server.run();
}
