use futures::future::{ok, Future, IntoFuture};
use futures::sync::oneshot::{channel, Sender};
use raft_rs::*;
use std::net::SocketAddr;

pub fn three_servers() -> (SocketAddr, SocketAddr, SocketAddr, ServerFuture, Sender<()>) {
    let addr1 = ([0, 0, 0, 0], 8080).into();
    let addr2 = ([0, 0, 0, 0], 8081).into();
    let addr3 = ([0, 0, 0, 0], 8082).into();
    let (stop1, rx1) = channel();
    let (stop2, rx2) = channel();
    let (stop3, rx3) = channel();
    let (stop, rx) = channel();
    let server1 = serve(
        RaftConfig {
            server_addr: addr1,
            peers: vec![addr2, addr3],
        },
        rx1,
    );
    let server2 = serve(
        RaftConfig {
            server_addr: addr2,
            peers: vec![addr1, addr3],
        },
        rx2,
    );
    let server3 = serve(
        RaftConfig {
            server_addr: addr3,
            peers: vec![addr1, addr2],
        },
        rx3,
    );
    let servers = rx
        .then(|_| {
            stop1.send(()).unwrap();
            stop2.send(()).unwrap();
            stop3.send(()).unwrap();
            ok(())
        })
        .join4(server1, server2, server3)
        .and_then(|_| ok(()));
    (
        ([127, 0, 0, 1], addr1.port()).into(),
        ([127, 0, 0, 1], addr2.port()).into(),
        ([127, 0, 0, 1], addr3.port()).into(),
        Box::new(servers),
        stop,
    )
}

pub fn run_server_test<E, F>(server: ServerFuture, stop: Sender<()>, test: F)
where
    E: std::fmt::Debug + Send + 'static,
    F: IntoFuture<Item = (), Error = E>,
    F::Future: Send + 'static,
{
    let (tx, rx) = futures::sync::oneshot::channel::<Option<E>>();
    let test = test.into_future().then(|res| {
        stop.send(()).unwrap();
        tx.send(res.err()).unwrap();
        ok(())
    });
    let fut = server.join(test).then(|_| ok(()));
    hyper::rt::run(fut);
    if let Ok(Some(e)) = rx.wait() {
        panic!("Failed test: {:?}", e)
    }
}
