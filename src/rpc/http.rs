use crate::client::{ClientError, RaftClient};
use crate::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, ClientApplyResponse, ClientQueryResponse,
    RPCBuilder, RaftServer, RequestVoteRequest, RequestVoteResponse, RPC,
};
use crate::state::{Command, Query, QueryResponse};
use crate::storage::Storage;
use bytes::buf::BufExt;
use futures::TryFutureExt;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};
use log::{debug, error, info, trace};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::convert::Infallible;
use std::error::Error;
use std::net::ToSocketAddrs;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HttpConfig {
    pub address: String,
}

impl RPCBuilder for HttpConfig {
    type RPC = HttpRPC;

    fn build<S: Storage>(&self, server: RaftServer<S>) -> std::io::Result<Self::RPC> {
        start_server(self.address.clone(), Arc::new(server))?;
        Ok(HttpRPC::new())
    }
}

pub type HttpRPC = hyper::Client<hyper::client::HttpConnector>;

#[async_trait::async_trait]
impl RPC for HttpRPC {
    async fn append_entries(
        &self,
        peer_address: String,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn Error + Send + Sync>> {
        Ok(send_request(self, &peer_address, "append_entries", request).await?)
    }

    async fn request_vote(
        &self,
        peer_address: String,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn Error + Send + Sync>> {
        Ok(send_request(self, &peer_address, "request_vote", request).await?)
    }
}

fn start_server<A: ToSocketAddrs, S: Storage>(
    addr: A,
    server: Arc<RaftServer<S>>,
) -> Result<(), std::io::Error> {
    let addrs = addr.to_socket_addrs()?;
    for addr in addrs {
        info!("Serving Raft at {}", &addr);
        let server = server.clone();
        let make_svc = make_service_fn(move |socket: &AddrStream| {
            trace!(
                "Serving new connection to {} from {}",
                &addr,
                socket.remote_addr()
            );
            let server = server.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    trace!("Serving request {} {}", req.method(), req.uri());
                    let server = server.clone();
                    async move { Ok::<_, Infallible>(serve_request(server, req).await) }
                }))
            }
        });
        let server = Server::bind(&addr).serve(make_svc);
        tokio::spawn(async move {
            if let Err(e) = server.await {
                error!("Server error: {}", e);
            }
        });
    }
    Ok(())
}

async fn serve_request<S: Storage>(
    server: Arc<RaftServer<S>>,
    req: Request<Body>,
) -> Response<Body> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::POST, "/v1/append_entries") => {
            parse_request_body(req)
                .and_then(|req| async move {
                    let response = server.append_entries(req).await?;
                    make_response_body(response)
                })
                .await
        }
        (&Method::POST, "/v1/request_vote") => {
            parse_request_body(req)
                .and_then(|req| async move {
                    let response = server.request_vote(req).await?;
                    make_response_body(response)
                })
                .await
        }
        (&Method::POST, "/v1/apply") => {
            parse_request_body(req)
                .and_then(|req| async move {
                    let response = server.apply(req).await?;
                    make_response_body(response)
                })
                .await
        }
        (&Method::POST, "/v1/query") => {
            parse_request_body(req)
                .and_then(|req| async move {
                    let response = server.query(req).await?;
                    make_response_body(response)
                })
                .await
        }
        _ => Ok::<_, Box<dyn std::error::Error>>(
            Response::builder().status(404).body(Body::empty()).unwrap(),
        ),
    };
    match response {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to process request: {}", e);
            Response::builder().status(500).body(Body::empty()).unwrap()
        }
    }
}

async fn parse_request_body<T>(req: Request<Body>) -> Result<T, Box<dyn std::error::Error>>
where
    T: DeserializeOwned,
{
    let data = hyper::body::to_bytes(req.into_body()).await?;
    let body = serde_json::from_slice(&data)?;
    Ok(body)
}

fn make_response_body<T>(body: T) -> Result<Response<Body>, Box<dyn std::error::Error>>
where
    T: Serialize,
{
    let body = serde_json::to_vec(&body)?;
    Ok(Response::new(Body::from(body)))
}

async fn send_request<'de, S, D>(
    client: &hyper::Client<hyper::client::HttpConnector>,
    peer_address: &str,
    method_name: &str,
    request: S,
) -> Result<D, ClientError>
where
    S: Serialize,
    D: DeserializeOwned,
{
    trace!("{}: sending request to {}", method_name, peer_address);
    let body = serde_json::to_vec(&request)?;
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{}/v1/{}", peer_address, method_name))
        .header("content-type", "application/json")
        .body(Body::from(body))?;
    let resp = client.request(req).await?;
    trace!("{}: response status {}", method_name, resp.status());
    if !resp.status().is_success() {
        debug!(
            "{}: server {} failed with status {}",
            method_name,
            peer_address,
            resp.status()
        );
    }
    let bytes = hyper::body::to_bytes(resp.into_body()).await?;
    trace!(
        "{}: server responded with: {}",
        method_name,
        String::from_utf8(bytes.to_vec()).unwrap(),
    );
    let resp = serde_json::from_reader(bytes.reader())?;
    Ok(resp)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HttpClientConfig {}

pub struct HttpClient {
    leader_address: String,
    max_retries: usize,
    http: hyper::Client<hyper::client::HttpConnector>,
}

impl HttpClient {
    pub fn new(leader_address: String, max_retries: usize) -> HttpClient {
        HttpClient {
            leader_address,
            max_retries,
            http: hyper::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl RaftClient for HttpClient {
    async fn apply(&mut self, cmd: Command) -> Result<(), ClientError> {
        for _attempt in 0..self.max_retries {
            let response: Result<ClientApplyResponse, ClientError> =
                send_request(&self.http, &self.leader_address, "apply", cmd.clone()).await;
            match response {
                Ok(ClientApplyResponse { success: true, .. }) => {
                    return Ok(());
                }
                Ok(ClientApplyResponse {
                    leader_address: leader_id,
                    success: false,
                }) => {
                    if let Some(leader_id) = leader_id {
                        self.leader_address = leader_id;
                    }
                }
                Err(e) => error!("Error while communicating with raft server: {}", e),
            }
        }
        Err(ClientError::MaxRetriesReached)
    }

    async fn query(&mut self, query: Query) -> Result<QueryResponse, ClientError> {
        for _attempt in 0..self.max_retries {
            let response: Result<ClientQueryResponse, ClientError> =
                send_request(&self.http, &self.leader_address, "query", query.clone()).await;
            match response {
                Ok(ClientQueryResponse {
                    response: Some(response),
                    ..
                }) => {
                    return Ok(response);
                }
                Ok(ClientQueryResponse {
                    leader_address: leader_id,
                    response: None,
                    ..
                }) => {
                    if let Some(leader_id) = leader_id {
                        self.leader_address = leader_id;
                    }
                }
                Err(e) => error!("Error while communicating with raft server: {}", e),
            }
        }
        Err(ClientError::MaxRetriesReached)
    }
}
