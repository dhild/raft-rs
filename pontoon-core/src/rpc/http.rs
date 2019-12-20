use crossbeam_channel::{Receiver, Sender};
use futures::prelude::*;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Uri};
use log::{debug, error};
use std::net::SocketAddr;

use super::{
    AppendEntriesRequest, AppendEntriesResult, RequestVoteRequest, RequestVoteResult, Transport,
    RPC,
};

/// Transport implemented using the HTTP protocol.
pub struct HttpTransport {
    client: Client<HttpConnector, Body>,
    server_tx: Sender<RPC>,
    server_rx: Receiver<RPC>,
}

impl Transport for HttpTransport {
    fn incoming(&mut self) -> Receiver<RPC> {
        self.server_rx.clone()
    }

    fn append_entries(
        &mut self,
        id: String,
        target: SocketAddr,
        req: &AppendEntriesRequest,
        results: Sender<AppendEntriesResult>,
    ) {
        self.send_request("Append Entries", "append-entries", id, target, req, results)
    }

    fn request_vote(
        &mut self,
        id: String,
        target: SocketAddr,
        req: &RequestVoteRequest,
        results: Sender<RequestVoteResult>,
    ) {
        self.send_request("Request Vote", "request-vote", id, target, req, results)
    }
}

impl Default for HttpTransport {
    fn default() -> Self {
        let (server_tx, server_rx) = crossbeam_channel::bounded(100);
        HttpTransport {
            client: Client::new(),
            server_tx,
            server_rx,
        }
    }
}

impl HttpTransport {
    fn send_request<S, D>(
        &mut self,
        name: &str,
        url_path: &str,
        id: String,
        target: SocketAddr,
        req: &S,
        results: Sender<D>,
    ) where
        S: serde::Serialize,
        D: serde::de::DeserializeOwned + Send + 'static,
    {
        debug!("Processing {} request to {}", name, id);
        let uri: Uri = match format!("http://{}/v1/{}", target, url_path).parse() {
            Ok(x) => x,
            Err(e) => {
                error!("failed to create {} request URI: {}", name, e);
                return;
            }
        };
        let body = match serde_json::to_vec(req) {
            Ok(x) => Body::from(x),
            Err(e) => {
                error!("failed to serialize {} request: {}", name, e);
                return;
            }
        };
        let req = match Request::builder().uri(uri).method("POST").body(body) {
            Ok(x) => x,
            Err(e) => {
                error!("failed to create http request: {}", e);
                return;
            }
        };
        let request = self
            .client
            .request(req)
            .map_err(|e| {
                error!("failed to send request: {}", e);
            })
            .and_then(|res| {
                res.into_body().concat2().map_err(|e| {
                    error!("failed to receive response: {}", e);
                })
            })
            .and_then(|body| {
                serde_json::from_slice(&body).map_err(|e| {
                    error!("failed to parse response: {}", e);
                })
            })
            .and_then(move |res| {
                results.send(res).map_err(|e| {
                    error!("failed to record response: {}", e);
                })
            });
        hyper::rt::spawn(request);
    }
}
