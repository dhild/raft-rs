use crate::storage::{LogEntry, Storage};
use async_channel::Sender;
use async_lock::Lock;
use async_trait::async_trait;
use log::{debug, error};
use std::error::Error as StdError;

#[derive(Clone)]
pub struct AppendEntriesRequest {
    pub leader_id: String,
    pub term: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub leader_commit: usize,
    pub entries: Vec<LogEntry>,
}

#[derive(Clone)]
pub struct AppendEntriesResponse {
    pub success: bool,
    pub term: usize,
}

impl AppendEntriesResponse {
    pub fn failed(term: usize) -> AppendEntriesResponse {
        AppendEntriesResponse {
            success: false,
            term,
        }
    }
    pub fn success(term: usize) -> AppendEntriesResponse {
        AppendEntriesResponse {
            success: true,
            term,
        }
    }
}

#[derive(Clone)]
pub struct RequestVoteRequest {
    pub term: usize,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Clone)]
pub struct RequestVoteResponse {
    pub success: bool,
    pub term: usize,
}

impl RequestVoteResponse {
    pub fn failed(term: usize) -> RequestVoteResponse {
        RequestVoteResponse {
            success: false,
            term,
        }
    }
    pub fn success(term: usize) -> RequestVoteResponse {
        RequestVoteResponse {
            success: true,
            term,
        }
    }
}

#[async_trait]
pub trait RPC: Clone + Send + Sync + 'static {
    async fn append_entries(
        &self,
        peer_address: String,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn std::error::Error + Send + Sync>>;

    async fn request_vote(
        &self,
        peer_address: String,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone)]
pub struct RaftServer<S: Storage> {
    storage: Lock<S>,
    term_updates: Sender<usize>,
    commit_updates: Sender<usize>,
}

impl<S: Storage> RaftServer<S> {
    pub fn new(
        storage: Lock<S>,
        term_updates: Sender<usize>,
        commit_updates: Sender<usize>,
    ) -> RaftServer<S> {
        RaftServer {
            storage,
            term_updates,
            commit_updates,
        }
    }

    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, S::Error> {
        let mut storage = self.storage.lock().await;

        let current_term = storage.current_term()?;
        // Ensure we are on the latest term
        if req.term > current_term {
            storage.set_current_term(req.term)?;
            if self.term_updates.send(req.term).await.is_err() {
                error!("Raft protocol has been shut down");
            }
            return Ok(AppendEntriesResponse::failed(current_term));
        }
        // Ensure the leader is on the latest term
        if req.term < current_term {
            return Ok(AppendEntriesResponse::failed(current_term));
        }
        // Ensure our previous entries match
        if let Some(term) = storage.get_term(req.prev_log_index)? {
            if term != req.prev_log_term {
                return Ok(AppendEntriesResponse::failed(current_term));
            }
        }
        // Remove any conflicting log entries
        for e in req.entries.iter() {
            match storage.get_term(e.index)? {
                None => break,
                Some(x) if x != e.term => {}
                _ => {
                    storage.remove_entries_starting_at(e.index)?;
                    break;
                }
            }
        }
        // Append any new entries
        let mut index = storage.last_index()?;
        for e in req.entries {
            if e.index > index {
                storage.append_entry(e.term, e.command)?;
                index = e.index;
            }
        }

        // Update the commit index with the latest committed value in our logs
        if self
            .commit_updates
            .send(req.leader_commit.min(index))
            .await
            .is_err()
        {
            error!("Raft protocol has been shut down");
            return Ok(AppendEntriesResponse::failed(current_term));
        }

        Ok(AppendEntriesResponse::success(current_term))
    }

    pub async fn request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn StdError + Send + Sync + 'static>> {
        let mut storage = self.storage.lock().await;

        // Ensure we are on the latest term - if we are not, update and continue processing the request.
        let current_term = {
            let current_term = storage.current_term()?;
            if req.term > current_term {
                storage.set_current_term(req.term)?;
                self.term_updates.send(req.term).await?;
                req.term
            } else {
                current_term
            }
        };
        // Ensure the candidate is on the latest term
        if req.term < current_term {
            return Ok(RequestVoteResponse::failed(current_term));
        }
        // Make sure we didn't vote for a different candidate already
        if storage
            .voted_for()?
            .map(|c| &c == &req.candidate_id)
            .unwrap_or(true)
        {
            // Grant the vote as long as their log is up to date.
            // Raft determines which of two logs is more up-to-date by comparing the index
            // and term of the last entries in the logs. If the logs have last entries with
            // different terms, then the log with the later term is more up-to-date. If the
            // logs end with the same term, then whichever log is longer is more up-to-date.
            let term = storage.last_term()?;
            let index = storage.last_index()?;
            if term <= req.last_log_term && index <= req.last_log_index {
                debug!("Voting for {} in term {}", &req.candidate_id, req.term);
                storage.set_voted_for(Some(req.candidate_id))?;
                return Ok(RequestVoteResponse::success(current_term));
            }
        }
        Ok(RequestVoteResponse::failed(current_term))
    }
}
//
// #[cfg(feature = "http-rpc")]
// mod http {
//     use crate::rpc::{
//         AppendEntriesRequest, AppendEntriesResponse, RaftServer, RequestVoteRequest,
//         RequestVoteResponse, RPC,
//     };
//     use crate::state::Command;
//     use crate::storage::Storage;
//     use bytes::buf::BufExt;
//     use futures::{StreamExt, TryFutureExt};
//     use hyper::service::{make_service_fn, service_fn};
//     use hyper::{Body, Method, Request, Response, Server};
//     use log::error;
//     use serde::de::DeserializeOwned;
//     use serde::{Deserialize, Serialize};
//     use std::convert::Infallible;
//     use std::error::Error;
//     use std::net::SocketAddr;
//
//     fn error_response() -> Response<Body> {
//         Response::builder().status(500).body(Body::empty()).unwrap()
//     }
//
//     async fn append_entries<S: Storage + 'static>(
//         server: RaftServer<S>,
//         req: Request<Body>,
//     ) -> Response<Body> {
//         let data = match hyper::body::to_bytes(req.into_body()).await {
//             Err(e) => {
//                 error!("Failed to read request: {}", e);
//                 return error_response();
//             }
//             Ok(data) => data,
//         };
//         let req = match serde_json::from_slice(&data) {
//             Err(e) => {
//                 error!("Failed to parse request: {}", e);
//                 return error_response();
//             }
//             Ok(req) => req,
//         };
//         let resp = match server.append_entries(req).await {
//             Err(e) => {
//                 error!("Failed to process request: {}", e);
//                 return error_response();
//             }
//             Ok(resp) => resp,
//         };
//         match serde_json::to_vec(&resp) {
//             Err(e) => {
//                 error!("Failed to write response: {}", e);
//                 error_response()
//             }
//             Ok(bytes) => Response::new(Body::from(bytes)),
//         }
//     }
//
//     async fn request_vote<S: Storage + 'static>(
//         server: RaftServer<S>,
//         req: Request<Body>,
//     ) -> Response<Body> {
//         let data = match hyper::body::to_bytes(req.into_body()).await {
//             Err(e) => {
//                 error!("Failed to read request: {}", e);
//                 return error_response();
//             }
//             Ok(data) => data,
//         };
//         let req = match serde_json::from_slice(&data) {
//             Err(e) => {
//                 error!("Failed to parse request: {}", e);
//                 return error_response();
//             }
//             Ok(req) => req,
//         };
//         let resp = match server.request_vote(req).await {
//             Err(e) => {
//                 error!("Failed to process request: {}", e);
//                 return error_response();
//             }
//             Ok(resp) => resp,
//         };
//         match serde_json::to_vec(&resp) {
//             Err(e) => {
//                 error!("Failed to write response: {}", e);
//                 error_response()
//             }
//             Ok(bytes) => Response::new(Body::from(bytes)),
//         }
//     }
//
//     async fn serve<S: Storage + 'static>(port: u16, server: RaftServer<S>) {
//         let addr = SocketAddr::from(([127, 0, 0, 1], port));
//         let make_svc = make_service_fn(|_socket| {
//             let server = server.clone();
//             async move {
//                 Ok::<_, Infallible>(service_fn(move |req: Request<Body>| async move {
//                     let res = match (req.method(), req.uri().path()) {
//                         (&Method::POST, "/v1/append_entries") => append_entries(server, req).await,
//                         (&Method::POST, "/v1/request_vote") => request_vote(server, req).await,
//                         _ => Response::builder().status(404).body(Body::empty()).unwrap(),
//                     };
//                     Ok::<_, Infallible>(res)
//                 }))
//             }
//         });
//         let server = Server::bind(&addr).serve(make_svc);
//         if let Err(e) = server.await {
//             error!("Server error: {}", e);
//         }
//     }
//
//     async fn send_request<'de, S, D>(
//         client: &hyper::Client<hyper::client::HttpConnector>,
//         peer_address: &str,
//         method_name: &str,
//         request: S,
//     ) -> Result<D, Box<dyn Error + Send + Sync>>
//     where
//         S: Serialize,
//         D: 'de + Deserialize<'de>,
//     {
//         let body = serde_json::to_vec(&request)?;
//         let req = Request::builder()
//             .method(Method::POST)
//             .uri(format!("http://{}/v1/{}", peer_address, method_name))
//             .header("content-type", "application/json")
//             .body(Body::from(body))?;
//         let mut resp = client.request(req).await?;
//         // if !resp.status().is_success() {
//         //     error!(
//         //         "{} to peer {} failed with status {}",
//         //         method_name,
//         //         peer_address,
//         //         resp.status()
//         //     );
//         //     return Err(Box::new("Failed request"));
//         // }
//         let bytes = hyper::body::to_bytes(resp.into_body()).await?;
//         let resp = serde_json::from_slice(&bytes)?;
//         Ok(resp)
//     }
//
//     #[async_trait::async_trait]
//     impl RPC for hyper::Client<hyper::client::HttpConnector> {
//         async fn append_entries(
//             &self,
//             peer_address: String,
//             request: AppendEntriesRequest,
//         ) -> Result<AppendEntriesResponse, Box<dyn Error + Send + Sync>> {
//             send_request(self, &peer_address, "append_entries", request).await
//         }
//
//         async fn request_vote(
//             &self,
//             peer_address: String,
//             request: RequestVoteRequest,
//         ) -> Result<RequestVoteResponse, Box<dyn Error + Send + Sync>> {
//             send_request(self, &peer_address, "request_vote", request).await
//         }
//     }
// }
