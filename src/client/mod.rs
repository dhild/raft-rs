mod types;

use crate::raft::*;
use futures::future::*;
use hyper::rt::{Future, Stream};
use hyper::{client, Body, Request};
use std::error;
use std::fmt;
use std::net::SocketAddr;
pub use types::*;

pub type ClientFuture<Item> = Box<dyn Future<Item = Item, Error = Error> + Send>;

#[derive(Debug)]
pub enum Error {
    REST(hyper::Error),
    JSON(serde_json::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::REST(ref e) => f.write_fmt(format_args!("REST error: {}", e)),
            Error::JSON(ref e) => f.write_fmt(format_args!("JSON error: {}", e)),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::REST(_) => "REST error",
            Error::JSON(_) => "JSON error",
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::REST(ref e) => Some(e),
            Error::JSON(ref e) => Some(e),
        }
    }
}

pub fn health<S: Into<SocketAddr>>(addr: S) -> ClientFuture<HealthStatus> {
    let addr = addr.into();

    Client::new().health(&addr)
}

pub struct Client {
    client: client::Client<client::HttpConnector, Body>,
}

impl Client {
    pub fn new() -> Client {
        Client {
            client: client::Client::new(),
        }
    }

    fn health(&mut self, id: &ServerId) -> ClientFuture<HealthStatus> {
        let uri: hyper::Uri = format!("http://{}/status", id).parse().unwrap();
        trace!("Sending GET to {}", uri);

        let response = self
            .client
            .get(uri)
            .map_err(|e| {
                error!("Failed to get health response: {}", e);
                Error::REST(e)
            })
            .and_then(|resp| {
                return resp.into_body().concat2().map_err(|e| {
                    error!("Failed to read full health response: {}", e);
                    Error::REST(e)
                });
            })
            .and_then(move |chunk| match serde_json::from_slice(&chunk) {
                Ok(status) => Ok(HealthStatus {
                    status: Some(status),
                }),
                Err(e) => {
                    error!("Failed to unmarshal health response: {}", e);
                    Err(Error::JSON(e))
                }
            });
        Box::new(response)
    }
}

impl RPC for Client {
    fn append_entries(
        &mut self,
        id: &ServerId,
        req: AppendEntriesRequest,
    ) -> FutureAppendEntriesResponse {
        let failure = || Box::new(err::<(AppendEntriesRequest, AppendEntriesResponse), ()>(()));
        let uri: hyper::Uri = format!("http://{}/append_entries", id).parse().unwrap();
        trace!("Sending POST to {}", uri);

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
        let response = self
            .client
            .request(request)
            .map_err(|e| {
                error!("Failed to get append_entries response: {}", e);
                ()
            })
            .and_then(|resp| {
                return resp.into_body().concat2().map_err(|e| {
                    error!("Failed to read full append_entries response: {}", e);
                    ()
                });
            })
            .and_then(move |chunk| {
                serde_json::from_slice(&chunk).map_err(|e| {
                    error!("Failed to unmarshal append_entries response: {}", e);
                    ()
                })
            })
            .map(|resp| (req, resp));
        Box::new(response)
    }

    fn request_vote(
        &mut self,
        id: &ServerId,
        req: RequestVoteRequest,
    ) -> FutureRequestVoteResponse {
        let failure = || Box::new(err::<(RequestVoteRequest, RequestVoteResponse), ()>(()));
        let uri: hyper::Uri = format!("http://{}/request_vote", id).parse().unwrap();
        trace!("Sending POST to {}", uri);

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
        let response = self
            .client
            .request(request)
            .map_err(|e| {
                error!("Failed to get request_vote response: {}", e);
                ()
            })
            .and_then(|resp| {
                return resp.into_body().concat2().map_err(|e| {
                    error!("Failed to read full request_vote response: {}", e);
                    ()
                });
            })
            .and_then(move |chunk| {
                serde_json::from_slice(&chunk).map_err(|e| {
                    error!("Failed to unmarshal request_vote response: {}", e);
                    ()
                })
            })
            .map(|resp| (req, resp));
        Box::new(response)
    }
}
