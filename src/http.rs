use crate::raft::*;
use futures::future::*;
use hyper::rt::{Future, Stream};
use hyper::service::service_fn;
use hyper::{
    Body, Client as HyperClient, Method, Request, Response, Server as HyperServer, StatusCode,
};
use std::error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

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

    fn request_vote(raft: &Arc<Mutex<Raft<E>>>, req: Request<Body>) -> BoxFut {
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
                raft.request_vote(parsed.unwrap())
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
            (&Method::POST, "/request_vote") => Self::request_vote(raft, req),
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
    fn append_entries(
        &mut self,
        id: &ServerId,
        req: AppendEntriesRequest,
    ) -> FutureAppendEntriesResponse {
        let failure = || Box::new(err::<(AppendEntriesRequest, AppendEntriesResponse), ()>(()));
        let client = HyperClient::new();
        let uri: hyper::Uri = format!("http://{}/append_entries", id).parse().unwrap();

        let mut request = Request::post(uri);
        let request = match serde_json::to_string(&req) {
            Err(e) => {
                error!("Failed to marshal append_entries request body: {}", e);
                return failure();
            }
            Ok(body) => match request.body(Body::from(body)) {
                Err(e) => {
                    error!("Failed to create request: {}", e);
                    return failure();
                }
                Ok(r) => r,
            },
        };
        let response = client.request(request)
            .map_err(|e| {
                error!("Failed to get append_entries response: {}", e);
                ()
            }).and_then(|resp| {
                return resp.into_body().concat2().map_err(|e| {
                    error!("Failed to read full append_entries response: {}", e);
                    ()
                })
            })
            .and_then(move |chunk| {
                serde_json::from_slice(&chunk).map_err(|e| {
                    error!("Failed to unmarshal append_entries response: {}", e);
                    ()
                })
            }).map(|resp| (req, resp));
        Box::new(response)
    }

    fn request_vote(
        &mut self,
        id: &ServerId,
        req: RequestVoteRequest,
    ) -> FutureRequestVoteResponse {
        let failure = || Box::new(err::<(RequestVoteRequest, RequestVoteResponse), ()>(()));
        let client = HyperClient::new();
        let uri: hyper::Uri = format!("http://{}/request_vote", id).parse().unwrap();

        let mut request = Request::post(uri);
        let request = match serde_json::to_string(&req) {
            Err(e) => {
                error!("Failed to marshal request_vote request body: {}", e);
                return failure();
            }
            Ok(body) => match request.body(Body::from(body)) {
                Err(e) => {
                    error!("Failed to create request: {}", e);
                    return failure();
                }
                Ok(r) => r,
            },
        };
        let response = client.request(request)
            .map_err(|e| {
                error!("Failed to get request_vote response: {}", e);
                ()
            }).and_then(|resp| {
                return resp.into_body().concat2().map_err(|e| {
                    error!("Failed to read full request_vote response: {}", e);
                    ()
                })
            })
            .and_then(move |chunk| {
                serde_json::from_slice(&chunk).map_err(|e| {
                    error!("Failed to unmarshal request_vote response: {}", e);
                    ()
                })
            }).map(|resp| (req, resp));
        Box::new(response)
    }
}
