use raft_rs::*;
use std::net::SocketAddr;
use crossbeam_channel::{bounded, Sender};

pub fn three_servers() -> ThreeServers {
    let addr1 = ([127, 0, 0, 1], 8080).into();
    let addr2 = ([127, 0, 0, 1], 8081).into();
    let addr3 = ([127, 0, 0, 1], 8082).into();
    let (stop, rx) = bounded(1);
    let rx1 = rx.shared();
    let rx2 = rx.shared();
    let rx3 = rx.shared();
    let server1 = new_server(
        RaftConfig {
            server_addr: addr1,
            peers: vec![addr2, addr3],
        },
        rx.clone().and_then(|_| Ok(())),
    );
    let server2 = new_server(
        RaftConfig {
            server_addr: addr2,
            peers: vec![addr1, addr3],
        },
        rx.clone().and_then(|_| Ok(())),
    );
    let server3 = new_server(
        RaftConfig {
            server_addr: addr3,
            peers: vec![addr1, addr2],
        },
        rx.clone().and_then(|_| Ok(())),
    );
    let servers = rx.then(|_| Ok(())).join4(server1, server2, server3);
    ThreeServers {
        addr1,
        addr2,
        addr3,
        stop,
        future: Box::new(servers),
    }
}

pub struct ThreeServers {
    pub addr1: SocketAddr,
    pub addr2: SocketAddr,
    pub addr3: SocketAddr,
    stop: Sender<()>,
}

impl ThreeServers {
}

pub fn run_server_test<E, F>(server: ThreeServers, test: F)
where
    E: std::fmt::Debug + Send + 'static,
    F: IntoFuture<Item = (), Error = E>,
    F::Future: Send + 'static,
{
    let (tx, rx) = futures::sync::oneshot::channel::<Option<E>>();
    let ThreeServers {
        stop,
        future: servers,
        ..
    } = server;
    let test = test.into_future().then(|res| {
        stop.send(()).unwrap();
        tx.send(res.err()).unwrap();
        ok(())
    });
    let fut = servers.join(test).then(|_| ok(()));
    hyper::rt::run(fut);
    if let Ok(Some(e)) = rx.wait() {
        panic!("Failed test: {:?}", e)
    }
}
