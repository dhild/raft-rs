#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate serde;

use futures::future::{lazy, loop_fn, ok, Loop};
use std::env;
use std::thread;
use std::time;

mod config;
mod http;
mod raft;
mod storage;

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    let cfg = config::RaftConfig::new(&args);

    let raft = cfg.build_raft();
    let rpc = cfg.build_rpc();
    let server = cfg.build_server(&raft);

    let update_loop = loop_fn((raft, rpc), |(arc, mut rpc)| {
        {
            let mut raft = arc.lock().unwrap();
            raft.update(&mut rpc);
        }
        thread::sleep(time::Duration::from_millis(50));
        Ok(Loop::Continue((arc, rpc)))
    });

    server.run(lazy(|| {
        hyper::rt::spawn(update_loop);
        ok::<(), ()>(())
    }));
}
