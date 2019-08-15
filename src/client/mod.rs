mod error;
pub use crate::server::api::*;
pub use error::Error;

use crate::raft::ServerId;
use futures::{Future, Poll, Stream};
use hyper::client::{self, HttpConnector};
use hyper::Body;

pub struct Client {
    client: client::Client<HttpConnector, Body>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            client: client::Client::new(),
        }
    }
}

pub struct ClientFuture<T> {
    inner: Box<dyn Future<Item = T, Error = Error> + Send>,
}

impl Client {
    pub fn health(&mut self, id: ServerId) -> ClientFuture<Health> {
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
                resp.into_body().concat2().map_err(|e| {
                    error!("Failed to read full health response: {}", e);
                    Error::REST(e)
                })
            })
            .and_then(move |chunk| match serde_json::from_slice(&chunk) {
                Ok(status) => Ok(status),
                Err(e) => {
                    error!("Failed to unmarshal health response: {}", e);
                    Err(Error::JSON(e))
                }
            });
        ClientFuture {
            inner: Box::new(response),
        }
    }
}

impl<T> Future for ClientFuture<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}
