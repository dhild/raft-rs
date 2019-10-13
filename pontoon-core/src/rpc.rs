//! Types for working with the raft protocol's Remote Procedure Calls.

use crossbeam_channel::{Receiver, Sender};
use std::net::SocketAddr;

use crate::configuration::ServerId;
use crate::logs::{LogEntry, LogIndex, Term};

#[derive(Debug, Clone)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: ServerId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub last_log: LogIndex,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: ServerId,
    pub candidate_addr: SocketAddr,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

pub struct AppendEntriesResult {
    pub id: ServerId,
    pub result: AppendEntriesResponse,
}

pub struct RequestVoteResult {
    pub id: ServerId,
    pub result: RequestVoteResponse,
}

/// Represents an invocation for a Remote Procedure Call.
pub enum RPC {
    AppendEntries(AppendEntriesRequest, Sender<AppendEntriesResult>),
    RequestVote(RequestVoteRequest, Sender<RequestVoteResult>),
}

/// An interface for making and responding to raft RPCs.
pub trait Transport: Sized + Send {
    /// Returns a channel for processing incoming RPC requests.
    fn requests(&mut self) -> Receiver<RPC>;

    /// Sends an `AppendEntries` RPC, registering a channel that should receive the response.
    /// Errors should be handled by the `Transport`; the raft protocol only needs to know if there
    /// was a result that made it back to this process.
    fn append_entries(
        &mut self,
        id: ServerId,
        target: SocketAddr,
        req: &AppendEntriesRequest,
        result: Sender<AppendEntriesResult>,
    );

    /// Sends a `RequestVote` RPC, registering a channel that should receive the response.
    /// Errors should be handled by the `Transport`; the raft protocol only needs to know if there
    /// was a result that made it back to this process.
    fn request_vote(
        &mut self,
        id: ServerId,
        target: SocketAddr,
        req: &RequestVoteRequest,
        result: Sender<RequestVoteResult>,
    );
}

pub struct HttpTransport {}

impl Transport for HttpTransport {
    /// Returns a channel for processing incoming RPC requests.
    fn requests(&mut self) -> Receiver<RPC> {
        unimplemented!();
    }

    /// Sends an `AppendEntries` RPC, registering a channel that should receive the response.
    /// Errors should be handled by the `Transport`; the raft protocol only needs to know if there
    /// was a result that made it back to this process.
    fn append_entries(
        &mut self,
        id: ServerId,
        target: SocketAddr,
        req: &AppendEntriesRequest,
        result: Sender<AppendEntriesResult>,
    ) {
        unimplemented!();
    }

    /// Sends a `RequestVote` RPC, registering a channel that should receive the response.
    /// Errors should be handled by the `Transport`; the raft protocol only needs to know if there
    /// was a result that made it back to this process.
    fn request_vote(
        &mut self,
        id: ServerId,
        target: SocketAddr,
        req: &RequestVoteRequest,
        result: Sender<RequestVoteResult>,
    ) {
        unimplemented!();
    }
}

impl Default for HttpTransport {
    fn default() -> Self {
        HttpTransport {}
    }
}
