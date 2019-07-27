extern crate env_logger;
#[macro_use]
extern crate log;
extern crate hyper;

mod common;

use futures::future::{loop_fn, Future, Loop};
use raft_rs::client;
use raft_rs::raft::Status;
use std::time::{Duration, Instant};

#[test]
fn three_servers_leader_matches() {
    env_logger::builder()
        .filter(Some("raft_rs"), log::LevelFilter::Info)
        .init();
    let (addr1, addr2, addr3, server, stop) = common::three_servers();

    let start = Instant::now();

    let check_single_leader = loop_fn(
        (addr1, addr2, addr3, start),
        |(addr1, addr2, addr3, start)| {
            client::health(addr1)
                .join3(client::health(addr2), client::health(addr3))
                .then(move |result| {
                    if let Ok((h1, h2, h3)) = result {
                        if let (
                            Some(Status {
                                leader: Some(l1), ..
                            }),
                            Some(Status {
                                leader: Some(l2), ..
                            }),
                            Some(Status {
                                leader: Some(l3), ..
                            }),
                        ) = (h1.status, h2.status, h3.status)
                        {
                            if l1 == l2 && l2 == l3 {
                                return Ok(Loop::Break(()));
                            }
                            warn!("Leaders do not match: {:?}, {:?}, {:?}", l1, l2, l3);
                        }
                    };
                    if start.elapsed() > Duration::from_secs(5) {
                        return Err("Timeout reached without quorum");
                    }

                    Ok(Loop::Continue((addr1, addr2, addr3, start)))
                })
        },
    );

    common::run_server_test(server, stop, check_single_leader);
}
