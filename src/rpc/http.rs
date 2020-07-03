use crate::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RPCBuilder, RaftServer, RequestVoteRequest,
    RequestVoteResponse, RPC,
};
use crate::storage::Storage;
use bytes::buf::BufExt;
use futures::TryFutureExt;
use hyper::server::conn::AddrStream;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server};
use log::{error, info, trace};
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
        send_request(self, &peer_address, "append_entries", request).await
    }

    async fn request_vote(
        &self,
        peer_address: String,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn Error + Send + Sync>> {
        send_request(self, &peer_address, "request_vote", request).await
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
) -> Result<D, Box<dyn Error + Send + Sync>>
where
    S: Serialize,
    D: DeserializeOwned,
{
    let body = serde_json::to_vec(&request)?;
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{}/v1/{}", peer_address, method_name))
        .header("content-type", "application/json")
        .body(Body::from(body))?;
    let resp = client.request(req).await?;
    // if !resp.status().is_success() {
    //     error!(
    //         "{} to peer {} failed with status {}",
    //         method_name,
    //         peer_address,
    //         resp.status()
    //     );
    //     return Err(Box::new("Failed request"));
    // }
    let bytes = hyper::body::to_bytes(resp.into_body()).await?;
    let resp = serde_json::from_reader(bytes.reader())?;
    Ok(resp)
}
