use crate::client;
use crate::config::RaftConfig;
use crate::raft::*;
use crate::storage;
use futures::future::*;
use hyper;
use hyper::rt::{Future, Stream};
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_timer::Delay;

pub type ServerFuture = Box<dyn Future<Item = (), Error = Error> + Send>;

#[derive(Debug)]
pub enum Error {
    Server(hyper::Error),
    Storage(storage::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Server(ref e) => f.write_fmt(format_args!("Server error: {}", e)),
            Error::Storage(ref e) => f.write_fmt(format_args!("Storage error: {}", e)),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Server(_) => "Server error",
            Error::Storage(_) => "Storage error",
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Server(ref e) => Some(e),
            Error::Storage(ref e) => Some(e),
        }
    }
}

pub fn run_serve<C: Into<RaftConfig>>(config: C) {
    let cfg = config.into();

    hyper::rt::run(lazy(|| {
        if let Err(e) = serve(cfg, futures::empty::<(), ()>()).wait() {
            eprintln!("Error from server: {}", e);
        }
        ok(())
    }))
}

pub fn serve<F>(cfg: RaftConfig, stop: F) -> ServerFuture
where
    F: Future<Item = ()> + Send + 'static,
{
    let storage = Box::new(storage::InMemoryStorage::new());
    let raft = Raft::new(cfg.server_addr, storage, cfg.peers.clone());
    let raft = Arc::new(Mutex::new(raft));
    let server = Server::new(cfg.server_addr, raft.clone());

    Box::new(
        update_loop(raft)
            .select(server.run(stop))
            .then(|res| match res {
                Ok(_) => Ok(()),
                Err((e, _)) => Err(e),
            }),
    )
}

fn update_loop<E: error::Error + 'static>(
    raft: Arc<Mutex<Raft<E>>>,
) -> Box<dyn Future<Item = (), Error = Error> + Send> {
    let rpc = client::Client::default();
    let fut = loop_fn((raft, rpc), |(arc, mut rpc)| {
        match arc.lock() {
            Ok(mut raft) => raft.update(&mut rpc),
            Err(e) => error!("Failed to execute raft update: {}", e),
        };

        Delay::new(Instant::now() + Duration::from_millis(50))
            .map_err(|e| {
                error!("Failed to execute delay: {}", e);
            })
            .then(|res| match res {
                Ok(_) => ok(Loop::Continue((arc, rpc))),
                Err(_) => ok(Loop::Break((arc, rpc))),
            })
    });
    Box::new(fut.and_then(|_| ok(())))
}

pub struct Server<E> {
    addr: SocketAddr,
    raft: Arc<Mutex<Raft<E>>>,
}

type ResponseFut = Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send>;

impl<E: error::Error + 'static> Server<E> {
    pub fn new(addr: SocketAddr, raft: Arc<Mutex<Raft<E>>>) -> Server<E> {
        Server { addr, raft }
    }

    fn raft_mut_post<X, Y, F>(
        raft: &Arc<Mutex<Raft<E>>>,
        req: Request<Body>,
        func: F,
    ) -> ResponseFut
    where
        X: serde::de::DeserializeOwned,
        Y: serde::Serialize,
        F: FnOnce(&mut Raft<E>, X) -> Result<Y, E> + Send + 'static,
    {
        let raft = Arc::clone(raft);
        let resp = req.into_body().concat2().map(move |chunk| {
            let mut resp = Response::builder();
            let parsed = serde_json::from_slice(&chunk);
            if let Err(_e) = parsed {
                return resp
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::empty())
                    .unwrap();
            }
            let res = {
                let mut raft = raft.lock().unwrap();
                func(&mut raft, parsed.unwrap())
            };
            match res {
                Err(_e) => resp
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap(),
                Ok(res) => match serde_json::to_string(&res) {
                    Err(_e) => resp
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap(),
                    Ok(body) => resp.status(StatusCode::OK).body(Body::from(body)).unwrap(),
                },
            }
        });
        Box::new(resp)
    }

    fn raft_get<Y, F>(raft: &Arc<Mutex<Raft<E>>>, func: F) -> ResponseFut
    where
        Y: serde::Serialize,
        F: FnOnce(&mut Raft<E>) -> Result<Y, E> + Send + 'static,
    {
        let raft = Arc::clone(raft);
        let resp = lazy(move || {
            let res = {
                let mut raft = raft.lock().unwrap();
                func(&mut raft)
            };
            let mut resp = Response::builder();
            let resp = match res {
                Err(_e) => resp
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap(),
                Ok(res) => match serde_json::to_string(&res) {
                    Err(_e) => resp
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap(),
                    Ok(body) => resp.status(StatusCode::OK).body(Body::from(body)).unwrap(),
                },
            };
            ok(resp)
        });
        Box::new(resp)
    }

    fn route(raft: &Arc<Mutex<Raft<E>>>, req: Request<Body>) -> ResponseFut {
        trace!("{} {}", req.method(), req.uri().path());
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/append_entries") => {
                Self::raft_mut_post(raft, req, |raft, x| raft.append_entries(x))
            }
            (&Method::POST, "/request_vote") => {
                Self::raft_mut_post(raft, req, |raft, x| raft.request_vote(x))
            }
            (&Method::GET, "/status") => Self::raft_get(raft, |raft| raft.status()),
            _ => Box::new(ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap())),
        }
    }

    pub fn run<F: Future<Item = ()> + Send + 'static>(self, stop: F) -> ServerFuture {
        info!("Listening on {}", &self.addr);
        let server = hyper::Server::bind(&self.addr)
            .serve(move || {
                let raft = self.raft.clone();
                service_fn(move |req: Request<Body>| -> ResponseFut { Self::route(&raft, req) })
            })
            .with_graceful_shutdown(stop)
            .map_err(Error::Server);

        Box::new(server)
    }
}
