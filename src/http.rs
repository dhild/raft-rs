use crate::raft::*;
use futures::future::*;
use hyper::rt::{Future, Stream};
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, Server as HyperServer, StatusCode};
use std::error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum Error {}

impl fmt::Display for Error {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        match *self {}
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {}
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {}
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct Server<E> {
    addr: SocketAddr,
    raft: Arc<Mutex<Raft<E>>>,
}

type BoxFut = Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send>;

impl<E: error::Error + 'static> Server<E> {
    pub fn new(addr: SocketAddr, raft: &Arc<Mutex<Raft<E>>>) -> Server<E> {
        Server {
            addr: addr,
            raft: Arc::clone(raft),
        }
    }

    fn append_entries(raft: &Arc<Mutex<Raft<E>>>, req: Request<Body>) -> BoxFut {
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
                raft.append_entries(parsed.unwrap())
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

    fn route(raft: &Arc<Mutex<Raft<E>>>, req: Request<Body>) -> BoxFut {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/append_entries") => Self::append_entries(raft, req),
            _ => Box::new(ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap())),
        }
    }

    pub fn run<F: Future<Item = (), Error = ()> + Send + 'static>(self, init: F) {
        let server = HyperServer::bind(&self.addr)
            .serve(move || {
                let raft = self.raft.clone();
                service_fn(move |req: Request<Body>| -> BoxFut { Self::route(&raft, req) })
            })
            .map_err(|e| eprintln!("server error: {}", e));

        // Run this server for... forever!
        hyper::rt::run(init.and_then(|()| server));
    }
}

pub struct Client {}
impl Client {
    pub fn new() -> Client {
        Client {}
    }
}

impl RPC for Client {
    type E = Error;

    fn append_entries(
        &mut self,
        server: &ServerId,
        req: AppendEntriesRequest,
    ) -> Box<dyn Future<Item = AppendEntriesResponse, Error = Self::E>> {
        panic!("not implemented yet!")
    }

    fn request_vote(
        &mut self,
        server: &ServerId,
        req: RequestVoteRequest,
    ) -> Box<dyn Future<Item = RequestVoteResponse, Error = Self::E>> {
        panic!("not implemented yet!")
    }
}
