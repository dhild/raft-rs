use crossbeam_channel::{select, Receiver, Sender};
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::result::Result;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::configuration::{ConfigurationState, Server};
use crate::fsm::FiniteStateMachine;
use crate::logs::{LogIndex, Storage, Term};
use crate::rpc::*;
use crate::state::*;

/// Used to construct a `Raft` object.
pub struct RaftBuilder {
    pub id: String,
    pub address: SocketAddr,
    pub election_timeout: Duration,
    pub heartbeat_timeout: Duration,
}

/// Creates a new `RaftBuilder` with sensible default values.
pub fn new<S: Into<SocketAddr>>(id: &str, address: S) -> RaftBuilder {
    RaftBuilder {
        id: String::from(id),
        address: address.into(),
        election_timeout: Duration::from_secs(1),
        heartbeat_timeout: Duration::from_secs(1),
    }
}

impl RaftBuilder {
    /// Initializes a new `Raft` object.
    pub fn build<F, S, T>(&self, state_machine: F, storage: S, transport: T) -> Raft<F, S, T>
    where
        F: FiniteStateMachine,
        S: Storage,
        T: Transport + 'static,
    {
        Raft {
            id: self.id.clone(),
            address: self.address,
            election_timeout: self.election_timeout,
            heartbeat_timeout: self.heartbeat_timeout,
            configuration: ConfigurationState::new(self.id.clone(), self.address),
            commit_index: 0,
            last_applied: 0,
            state: RaftState::default(),
            storage,
            state_machine: Arc::new(Mutex::new(state_machine)),
            transport: Arc::new(Mutex::new(transport)),
        }
    }
}

pub struct Raft<F: FiniteStateMachine, S: Storage, T: Transport> {
    id: String,
    address: SocketAddr,
    election_timeout: Duration,
    heartbeat_timeout: Duration,
    configuration: ConfigurationState,
    commit_index: LogIndex,
    last_applied: LogIndex,
    state: RaftState,
    storage: S,
    state_machine: Arc<Mutex<F>>,
    transport: Arc<Mutex<T>>,
}

impl<F, S, T> Raft<F, S, T>
where
    F: FiniteStateMachine,
    S: Storage,
    T: Transport + 'static,
{
    pub fn run(&mut self, shutdown: Receiver<()>) {
        loop {
            match self.state {
                RaftState::Follower(_) => self.run_follower(&shutdown),
                RaftState::Candidate(_) => self.run_candidate(&shutdown),
                RaftState::Leader(_) => self.run_leader(&shutdown),
                RaftState::ShuttingDown => return,
            }
        }
    }

    fn run_follower(&mut self, shutdown: &Receiver<()>) {
        let timeout = crossbeam_channel::after(self.election_timeout);
        while let RaftState::Follower(ref follower) = self.state {
            select! {
                recv(shutdown) -> _ => {
                    debug!("Shutdown signal received");
                    self.state = RaftState::ShuttingDown;
                }
                recv(timeout) -> _ => {
                    info!("Election timeout reached, starting election");
                    self.state = RaftState::Candidate(follower.to_candidate());
                }
            };
        }
    }

    fn run_candidate(&mut self, shutdown: &Receiver<()>) {
        let votes = match self.start_election() {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to start election: {}", e);
                return;
            }
        };
        let timeout = crossbeam_channel::after(self.election_timeout);
        let votes_needed = self.configuration.latest().quorum_size();
        let mut votes_granted = 0;
        let current_term = self.storage.current_term();

        while let RaftState::Candidate(candidate) = &self.state {
            select! {
                recv(votes) -> voter => {
                    match voter {
                        Err(e) => {
                            error!("All voters hung up! {}", e);
                            return
                        }
                        Ok(RequestVoteResult{id, result}) => {
                            if result.term > current_term {
                                info!("Newer term discovered as candidate, switching to follower");
                                self.state = RaftState::Follower(candidate.to_follower());
                                if let Err(e) = self.storage.set_current_term(result.term) {
                                    error!("Failed to set current term to {}: {}", result.term, e);
                                }
                            } else if result.vote_granted  {
                                votes_granted += 1;
                                debug!("Vote granted from {}; tally: {}", id, votes_granted);
                                if votes_granted >= votes_needed {
                                    info!("Won election {} with {} votes", current_term, votes_granted);
                                    self.state = RaftState::Leader(candidate.to_leader());
                                }
                            }
                        }
                    }
                },
                recv(shutdown) -> _ => {
                    debug!("Shutdown signal received");
                    self.state = RaftState::ShuttingDown;
                    return
                },
                recv(timeout) -> _ => {
                    warn!("Election timeout reached, restarting election");
                    return
                },
            };
        }
    }

    fn start_election(&mut self) -> Result<Receiver<RequestVoteResult>, S::Error> {
        let term = 1 + self.storage.current_term();
        self.storage.set_current_term(term)?;

        let (last_log_index, last_log_term) = self.storage.last_log_entry().unwrap_or((0, 0));
        let req = RequestVoteRequest {
            candidate_id: self.id.clone(),
            candidate_addr: self.address,
            term,
            last_log_index,
            last_log_term,
        };

        let (tx, rx) = crossbeam_channel::bounded(self.configuration.latest().count_voters());
        for server in self.configuration.latest().voters() {
            if server.id == self.id {
                self.storage.set_voted_for(Some(self.id.clone()))?;
                tx.send(RequestVoteResult {
                    id: self.id.clone(),
                    result: RequestVoteResponse {
                        term,
                        vote_granted: true,
                    },
                })
                .expect("Failed to record vote for self");
            } else {
                let mut rpc = self.transport.lock().unwrap();
                rpc.request_vote(server.id.clone(), server.address, &req, tx.clone());
            }
        }

        Ok(rx)
    }

    fn run_leader(&mut self, shutdown: &Receiver<()>) {
        let (tx, append_entries_responses) = crossbeam_channel::unbounded();
        for server in self.configuration.latest().servers() {
            let server_tx = self.heartbeat_for_server(server.clone(), tx.clone());
            if let RaftState::Leader(ref mut leader) = self.state {
                leader.add_server(&server.id, server_tx);
            }
        }
        let current_term = self.storage.current_term();

        while let RaftState::Leader(leader) = &self.state {
            select! {
                recv(append_entries_responses) -> resp => {
                    let resp = resp.unwrap();
                    if resp.term > current_term {
                        info!("Newer term discovered as leader, switching to follower");
                        self.state = RaftState::Follower(leader.to_follower());
                        if let Err(e) = self.storage.set_current_term(resp.term) {
                            error!("Failed to set current term to {}: {}", resp.term, e);
                        }
                    }
                }
                recv(shutdown) -> _ => {
                    debug!("Shutdown signal received");
                    self.state = RaftState::ShuttingDown;
                    return
                },
            }
        }
    }

    fn heartbeat_for_server(
        &self,
        server: Server,
        rpc_results: Sender<AppendEntriesResponse>,
    ) -> Sender<AppendEntriesRequest> {
        let heartbeat = crossbeam_channel::after(self.heartbeat_timeout);

        let next_index = self.storage.last_log_entry().unwrap_or((0, 0)).0 + 1;
        let rpc = self.transport.clone();
        let (tx, rx) = crossbeam_channel::unbounded();

        std::thread::spawn(move || {
            let mut next_index = next_index;
            let mut match_index = 0;

            let (tx2, rx2) = crossbeam_channel::bounded(1);

            loop {
                let next = match rx.recv() {
                    Ok(next) => next,
                    Err(e) => {
                        warn!(
                            "Sender closed, stopping replication for {}: {}",
                            server.id, e
                        );
                        return;
                    }
                };
                {
                    rpc.lock().unwrap().append_entries(
                        server.id.clone(),
                        server.address,
                        &next,
                        tx2.clone(),
                    );
                }
                match rx2.recv() {
                    Err(e) => {
                        warn!("RPC hung up sender for replication to {}: {}", server.id, e);
                        return;
                    }
                    Ok(res) => {
                        debug!("AppendEntriesResponse from {}: {:?}", res.id, res.result);
                        if rpc_results.send(res.result).is_err() {
                            debug!("RPC results closed, we must be a follower now. Stopping replication for {}", server.id);
                            return;
                        }
                    }
                }
            }
        });
        tx
    }

    fn process_rpc(&mut self, rpc: RPC) {
        match rpc {
            RPC::AppendEntries(req, rx) => match self.append_entries(req) {
                Err(e) => error!("Failed to process AppendEntries: {}", e),
                Ok(resp) => {
                    if let Err(e) = rx.send(AppendEntriesResult {
                        id: self.id.clone(),
                        result: resp,
                    }) {
                        warn!("Sender is hung up when responding to AppendEntries: {}", e);
                    }
                }
            },
            RPC::RequestVote(req, rx) => match self.request_vote(req) {
                Err(e) => error!("Failed to process RequestVote: {}", e),
                Ok(resp) => {
                    if let Err(e) = rx.send(RequestVoteResult {
                        id: self.id.clone(),
                        result: resp,
                    }) {
                        warn!("Sender is hung up when responding to RequestVote: {}", e);
                    }
                }
            },
        }
    }

    fn check_request_term(&mut self, req: Term, current_term: Term) -> Result<(), S::Error> {
        if req > current_term {
            info!("updating term from {} to {}", current_term, req);
            self.storage.set_current_term(req)?;

            match &self.state {
                RaftState::Candidate(c) => {
                    info!("converting from candidate to follower");
                    self.state = RaftState::Follower(c.to_follower())
                }
                RaftState::Leader(l) => {
                    info!("converting from leader to follower");
                    self.state = RaftState::Follower(l.to_follower())
                }
                _ => (),
            };
        }
        Ok(())
    }

    pub fn append_entries(
        &mut self,
        mut req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, S::Error> {
        let current_term = self.storage.current_term();
        let last_log = self.storage.last_log_entry().unwrap_or((0, 0)).0;
        let failed = Ok(AppendEntriesResponse {
            term: current_term,
            last_log,
            success: false,
        });
        debug!(
            "received append_entries on term {}: {:?}",
            current_term, req
        );

        // Reply false if term < current_term
        if req.term < current_term {
            debug!(
                "term {} is less than current term {}",
                req.term, current_term
            );
            return failed;
        }

        // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm.
        match self.storage.get(req.prev_log_index)? {
            Some(l) => {
                if l.term != req.prev_log_term {
                    debug!(
                        "log index {} with term {} doesn't match our term {}",
                        req.prev_log_index, req.prev_log_term, l.term
                    );
                    return failed;
                }
            }
            None => {
                debug!("log index {} is too new", req.prev_log_index);
                return failed;
            }
        }

        req.entries.sort();

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it.
        for i in 0..req.entries.len() {
            match self.storage.get(req.entries[i].index)? {
                None => {
                    req.entries = req.entries[i..].to_vec();
                    break;
                }
                Some(l) => {
                    if l.term != req.entries[i].term {
                        self.storage.delete_logs(l.index)?;
                        req.entries = req.entries[i..].to_vec();
                        break;
                    }
                }
            }
        }

        // Append any new entries not already in the log.
        let latest_index = req.entries.last().map(|x| x.index);
        self.storage.store_logs(req.entries)?;

        // If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry)
        if let Some(latest_index) = latest_index {
            if req.leader_commit > self.commit_index {
                if req.leader_commit > latest_index {
                    debug!(
                        "setting commit_index using log index {} from last commit",
                        latest_index
                    );
                    self.commit_index = latest_index;
                } else {
                    debug!(
                        "setting commit_index using log index {} from leader",
                        req.leader_commit
                    );
                    self.commit_index = req.leader_commit;
                }
            }
        }

        // Check the request term to make sure we are correct:
        self.check_request_term(req.term, current_term)?;

        Ok(AppendEntriesResponse {
            term: current_term,
            last_log: self.storage.last_log_entry().unwrap_or((0, 0)).0,
            success: true,
        })
    }

    pub fn request_vote(
        &mut self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, S::Error> {
        let current_term = self.storage.current_term();
        let failed = Ok(RequestVoteResponse {
            term: current_term,
            vote_granted: false,
        });
        debug!("received request_vote on term {}: {:?}", current_term, req);

        // Reply false if term < current_term
        if req.term < current_term {
            return failed;
        }

        // If voted_for is None or candidate_id, and candidate's log is at least as up-to-date
        // as the receiver's, grant vote.
        if let Some(id) = self.storage.voted_for() {
            if req.candidate_id != id {
                return failed;
            }
        }
        match self.storage.last_log_entry() {
            Some((_, term)) if req.last_log_term < term => {
                debug!(
                    "candidate's term {} is not up to date with our term {}",
                    req.last_log_term, term
                );
                return failed;
            }
            Some((index, term)) if req.last_log_term == term && req.last_log_index < index => {
                debug!(
                    "candidate's index {} is not up to date with our index {}",
                    req.last_log_index, index
                );
                return failed;
            }
            _ => (),
        }

        info!("voting for candidate {}", req.candidate_id);
        self.storage.set_voted_for(Some(req.candidate_id))?;

        // Make sure we are on the correct term:
        self.check_request_term(req.term, current_term)?;

        Ok(RequestVoteResponse {
            term: current_term,
            vote_granted: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsm::KeyValueStore;
    use crate::logs::InMemoryStorage;
    use crate::rpc::HttpTransport;

    fn basic_raft() -> Raft<KeyValueStore, InMemoryStorage, HttpTransport> {
        new("unique-raft-id", ([127, 0, 0, 1], 8080)).build(
            KeyValueStore::default(),
            InMemoryStorage::default(),
            HttpTransport::default(),
        )
    }

    #[test]
    fn raft_is_self_voter_at_start() {
        let raft = basic_raft();

        assert_eq!(1, raft.configuration.latest().count_voters());
    }
}
