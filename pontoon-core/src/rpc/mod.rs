//! Types for working with the raft protocol's Remote Procedure Calls.

#[cfg(feature = "http-transport")]
mod http;
#[cfg(feature = "http-transport")]
pub use http::HttpTransport;

use std::net::SocketAddr;

use crate::logs::{LogEntry, LogIndex, Term};

#[cfg(feature = "http-transport")]
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: String,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub last_log: LogIndex,
    pub success: bool,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: String,
    pub candidate_addr: SocketAddr,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct AppendEntriesResult {
    pub id: String,
    pub result: AppendEntriesResponse,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "http-transport", derive(Serialize, Deserialize))]
pub struct RequestVoteResult {
    pub id: String,
    pub result: RequestVoteResponse,
}

/// Represents an invocation for a Remote Procedure Call.
pub enum RPC {
    AppendEntries(
        AppendEntriesRequest,
        crossbeam_channel::Sender<AppendEntriesResult>,
    ),
    RequestVote(
        RequestVoteRequest,
        crossbeam_channel::Sender<RequestVoteResult>,
    ),
}

/// An interface for making and responding to raft RPCs.
pub trait Transport: Send {
    fn incoming(&mut self) -> crossbeam_channel::Receiver<RPC>;

    /// Sends an `AppendEntries` RPC, registering a channel that should receive the response.
    /// Errors should be handled by the `Transport`; the raft protocol only needs to know if there
    /// was a result that made it back to this process.
    fn append_entries(
        &mut self,
        id: String,
        target: SocketAddr,
        req: &AppendEntriesRequest,
        results: crossbeam_channel::Sender<AppendEntriesResult>,
    );

    /// Sends a `RequestVote` RPC, registering a channel that should receive the response.
    /// Errors should be handled by the `Transport`; the raft protocol only needs to know if there
    /// was a result that made it back to this process.
    fn request_vote(
        &mut self,
        id: String,
        target: SocketAddr,
        req: &RequestVoteRequest,
        results: crossbeam_channel::Sender<RequestVoteResult>,
    );
}
