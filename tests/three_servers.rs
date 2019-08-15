extern crate env_logger;
#[macro_use]
extern crate log;
extern crate hyper;
extern crate tokio_timer;

mod common;

use futures::future::{loop_fn, Future, Loop};
use raft_rs::*;
use std::time::{Duration, Instant};
use tokio_timer::Delay;

#[test]
fn three_servers_leader_matches() {
    env_logger::builder()
        .filter(Some("raft_rs"), log::LevelFilter::Debug)
        .init();
    let servers = common::three_servers();

    let start = Instant::now();

    let mut client = Client::default();

    let check_single_leader = loop_fn(
        (servers.addr1(), servers.addr2(), servers.addr3(), start),
        move |(addr1, addr2, addr3, start)| {
            client
                .health(addr1)
                .join3(client.health(addr2), client.health(addr3))
                .then(move |result| {
                    if let Ok((
                        Health {
                            raft:
                                raft::Status {
                                    leader: Some(l1), ..
                                },
                        },
                        Health {
                            raft:
                                raft::Status {
                                    leader: Some(l2), ..
                                },
                        },
                        Health {
                            raft:
                                raft::Status {
                                    leader: Some(l3), ..
                                },
                        },
                    )) = result
                    {
                        if l1 == l2 && l2 == l3 {
                            return Ok(Loop::Break(()));
                        }
                        warn!("Leaders do not match: {:?}, {:?}, {:?}", l1, l2, l3);
                    };
                    if start.elapsed() > Duration::from_millis(500) {
                        return Err("Timeout reached without quorum");
                    }

                    Ok(Loop::Continue((addr1, addr2, addr3, start)))
                })
                .then(|x| Delay::new(Instant::now() + Duration::from_millis(25)).then(|_| x))
        },
    );

    common::run_server_test(servers, check_single_leader);
}
