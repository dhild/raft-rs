use crate::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, ClientApplyResponse, ClientQueryResponse, HttpRPC,
    RaftServerRPC, RequestVoteRequest, RequestVoteResponse,
};
use crate::state::{Command, Query, StateMachine};
use crate::storage::{LogCommand, LogEntry, MemoryStorage, Storage};
use async_channel::{Receiver, RecvError, Sender, TryRecvError, TrySendError};
use async_lock::Lock;
use log::{debug, error, info, trace};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct RaftConfiguration {
    id: String,
    address: String,
    peers: Vec<Peer>,
    current_leader: Option<String>,
}

impl RaftConfiguration {
    pub fn new(id: String, address: String, peers: Vec<Peer>) -> RaftConfiguration {
        RaftConfiguration {
            id,
            address,
            peers,
            current_leader: None,
        }
    }

    pub fn peers(&self) -> Vec<Peer> {
        self.peers.clone()
    }

    pub fn current_leader_address(&self) -> Option<String> {
        self.current_leader
            .as_ref()
            .map(|id| {
                self.peers
                    .iter()
                    .find(|peer| &peer.id == id)
                    .map(|peer| peer.address.clone())
            })
            .unwrap_or(None)
    }

    pub fn is_current_leader(&self, leader_id: &str) -> bool {
        self.current_leader
            .as_ref()
            .map(|id| id == leader_id)
            .unwrap_or(false)
    }

    pub fn set_current_leader(&mut self, leader_id: &str) {
        self.current_leader = Some(leader_id.to_string());
    }

    pub fn voting_majority(&self) -> Option<usize> {
        match self.peers.iter().filter(|p| p.voting).count() {
            0 => {
                error!("Must have at least one voting peer to create a voting majority");
                None
            }
            1 | 2 => Some(2),
            3 | 4 => Some(3),
            x => {
                error!("Too many voting peers (found {})", x);
                None
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub address: String,
    pub voting: bool,
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}: {})", self.id, self.address)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProtocolState {
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

impl fmt::Display for ProtocolState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolState::Follower => write!(f, "Follower"),
            ProtocolState::Candidate => write!(f, "Candidate"),
            ProtocolState::Leader => write!(f, "Leader"),
            ProtocolState::Shutdown => write!(f, "Shutdown"),
        }
    }
}

pub struct LogCommitter {
    commits: Receiver<(usize, LogCommand)>,
    state_machine: Lock<StateMachine>,
    last_applied_tx: Lock<Vec<Sender<usize>>>,
}

impl LogCommitter {
    pub fn new(
        state_machine: Lock<StateMachine>,
        commits: Receiver<(usize, LogCommand)>,
        last_applied_tx: Lock<Vec<Sender<usize>>>,
    ) -> LogCommitter {
        LogCommitter {
            commits,
            state_machine,
            last_applied_tx,
        }
    }

    pub async fn run(&mut self) {
        let mut last_applied = 0;
        while let Ok((commit_index, cmd)) = self.commits.recv().await {
            if commit_index != (last_applied + 1) {
                error!(
                    "Out of order commit transmission, expected {} and got {}",
                    (last_applied + 1),
                    commit_index
                );
                panic!()
            }
            last_applied = commit_index;
            match cmd {
                LogCommand::Command(cmd) => self.state_machine.lock().await.apply(cmd),
                LogCommand::Noop => {}
            }
            let mut last_applied = self.last_applied_tx.lock().await;
            let mut preserved = Vec::new();
            for tx in last_applied.iter() {
                if tx.send(commit_index).await.is_ok() {
                    preserved.push(tx.clone());
                }
            }
            *last_applied = preserved;
        }
    }
}

pub struct ProtocolTasks {
    id: String,
    configuration: Lock<RaftConfiguration>,
    timeout: Duration,
    storage: Lock<Box<dyn Storage>>,
    rpc: Arc<dyn RaftServerRPC>,
    commit_index: Lock<usize>,
    current_state: Lock<ProtocolState>,
    term_updates_tx: Sender<()>,
    term_updates_rx: Receiver<()>,
    append_entries_rx: Receiver<usize>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
    new_logs_rx: Receiver<usize>,
}

impl ProtocolTasks {
    pub fn new(
        id: String,
        configuration: Lock<RaftConfiguration>,
        timeout: Duration,
        storage: Lock<Box<dyn Storage>>,
        current_state: Lock<ProtocolState>,
        rpc: Arc<dyn RaftServerRPC>,
        term_updates_tx: Sender<()>,
        term_updates_rx: Receiver<()>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        append_entries_rx: Receiver<usize>,
        new_logs_rx: Receiver<usize>,
    ) -> ProtocolTasks {
        let commit_index = Lock::new(0);

        ProtocolTasks {
            id,
            configuration,
            timeout,
            storage,
            rpc,
            commit_index,
            current_state,
            term_updates_tx,
            term_updates_rx,
            append_entries_rx,
            commits_to_apply_tx,
            new_logs_rx,
        }
    }

    pub async fn run(&mut self) {
        loop {
            // Grab the current state and run the appropriate protocol logic.
            let state = { *self.current_state.lock().await };
            trace!("Running raft protocol as {}", state);
            let state = match state {
                ProtocolState::Follower { .. } => self.run_follower().await,
                ProtocolState::Candidate { .. } => self.run_candidate().await,
                ProtocolState::Leader { .. } => self.run_leader().await,
                ProtocolState::Shutdown => return,
            };
            // Update the state with any changes
            {
                *self.current_state.lock().await = state;
            }
        }
    }

    async fn run_follower(&mut self) -> ProtocolState {
        let follower = Follower::new(
            self.timeout,
            self.storage.clone(),
            self.commit_index.clone(),
            self.term_updates_rx.clone(),
            self.append_entries_rx.clone(),
            self.commits_to_apply_tx.clone(),
        );
        follower.run().await
    }

    async fn run_candidate(&mut self) -> ProtocolState {
        let candidate = Candidate::new(
            self.id.clone(),
            self.configuration.clone(),
            self.timeout,
            self.storage.clone(),
            self.rpc.clone(),
            self.term_updates_rx.clone(),
        );
        candidate.run().await
    }

    async fn run_leader(&mut self) -> ProtocolState {
        let mut leader = match Leader::start(
            self.id.clone(),
            self.configuration.clone(),
            self.timeout,
            self.storage.clone(),
            self.rpc.clone(),
            self.commit_index.clone(),
            self.term_updates_tx.clone(),
            self.term_updates_rx.clone(),
            self.commits_to_apply_tx.clone(),
            self.new_logs_rx.clone(),
        )
        .await
        {
            Ok(leader) => leader,
            Err(e) => {
                error!("Storage error, converting back to follower: {}", e);
                return ProtocolState::Follower;
            }
        };
        leader.run().await
    }
}

pub struct Follower {
    timeout: Duration,
    storage: Lock<Box<dyn Storage>>,
    commit_index: Lock<usize>,
    term_updates_rx: Receiver<()>,
    append_entries_rx: Receiver<usize>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
}

impl Follower {
    pub fn new(
        timeout: Duration,
        storage: Lock<Box<dyn Storage>>,
        commit_index: Lock<usize>,
        term_updates_rx: Receiver<()>,
        append_entries_rx: Receiver<usize>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
    ) -> Follower {
        let timeout = timeout.mul_f32(1. + 2. * rand::thread_rng().gen::<f32>());
        Follower {
            timeout,
            storage,
            commit_index,
            term_updates_rx,
            append_entries_rx,
            commits_to_apply_tx,
        }
    }

    pub async fn run(&self) -> ProtocolState {
        loop {
            match self.run_once().await {
                ProtocolState::Follower { .. } => {}
                other => return other,
            }
        }
    }

    pub async fn run_once(&self) -> ProtocolState {
        let election_timeout = tokio::time::delay_for(self.timeout);
        select! {
            // Consume the term updates; sender is responsible for updating the storage:
            _ = self.term_updates_rx.recv() => ProtocolState::Follower,
            // Actually process commits as they come in
            event = self.append_entries_rx.recv() => match event {
                Ok(new_index) => self.process_append_entries(new_index).await,
                // Server hung up, no more RPCs:
                Err(_) => {
                    trace!("State machine processing disconnected; shutting down");
                    ProtocolState::Shutdown
                },
            },
            _ = election_timeout => {
                info!("Election timeout reached; transitioning to candidate state");
                ProtocolState::Candidate
            }
        }
    }

    async fn process_append_entries(&self, new_commit_index: usize) -> ProtocolState {
        let storage = self.storage.lock().await;
        let mut commit_index = self.commit_index.lock().await;
        while new_commit_index > *commit_index {
            *commit_index += 1;
            let cmd = storage.get_command(*commit_index).unwrap();
            if self
                .commits_to_apply_tx
                .send((*commit_index, cmd))
                .await
                .is_err()
            {
                debug!("State machine processing has been disconnected; shutting down");
                return ProtocolState::Shutdown;
            }
        }
        ProtocolState::Follower
    }
}

pub struct Candidate {
    id: String,
    configuration: Lock<RaftConfiguration>,
    timeout: Duration,
    storage: Lock<Box<dyn Storage>>,
    rpc: Arc<dyn RaftServerRPC>,
    term_updates_rx: Receiver<()>,
}

impl Candidate {
    pub fn new(
        id: String,
        configuration: Lock<RaftConfiguration>,
        timeout: Duration,
        storage: Lock<Box<dyn Storage>>,
        rpc: Arc<dyn RaftServerRPC>,
        term_updates_rx: Receiver<()>,
    ) -> Candidate {
        let timeout = timeout.mul_f32(1. + 2. * rand::thread_rng().gen::<f32>());
        Candidate {
            id,
            configuration,
            timeout,
            storage,
            rpc,
            term_updates_rx,
        }
    }

    pub async fn run(&self) -> ProtocolState {
        loop {
            let election_timeout = tokio::time::delay_for(self.timeout);
            match self.run_once().await {
                ProtocolState::Candidate { .. } => {}
                other => return other,
            }
            // If the election fails quickly, make sure we wait out the timer.
            election_timeout.await;
        }
    }

    pub async fn run_once(&self) -> ProtocolState {
        match self.term_updates_rx.try_recv() {
            // We could receive an RPC indicating a newer term should be used:
            Ok(_) => return ProtocolState::Follower,
            // If we aren't connected to the RPC anymore, it's time to shut down:
            Err(TryRecvError::Closed) => return ProtocolState::Shutdown,
            _ => {}
        }

        let (term, votes) = match self.send_vote_requests().await {
            Ok(votes) => votes,
            Err(e) => {
                error!("Failed to send vote requests: {}", e);
                return ProtocolState::Follower;
            }
        };
        match tokio::time::timeout(self.timeout, self.tally_results(term, votes)).await {
            Ok(result) => result,
            Err(_) => {
                info!("Election timeout reached");
                ProtocolState::Candidate
            }
        }
    }

    async fn send_vote_requests(&self) -> std::io::Result<(usize, Receiver<ElectionResult>)> {
        let mut storage = self.storage.lock().await;

        // Increment the current term, and vote for ourselves:
        let current_term = storage.current_term()? + 1;
        storage.set_current_term(current_term)?;
        storage.set_voted_for(Some(self.id.clone()))?;

        let last_log_term = storage.last_term()?;
        let last_log_index = storage.last_index()?;

        let request = RequestVoteRequest {
            term: current_term,
            candidate_id: self.id.clone(),
            last_log_term,
            last_log_index,
        };
        info!("Starting election for term {}", current_term);

        // Send out the RequestVote RPC calls.
        let (votes_tx, votes_rx) = async_channel::bounded(5);
        for peer in self.configuration.lock().await.peers() {
            let request = request.clone();
            let votes_tx = votes_tx.clone();
            let current_term = request.term;
            let rpc = self.rpc.clone();
            tokio::spawn(async move {
                let vote = match rpc.request_vote(peer.address.clone(), request).await {
                    Ok(rv) => {
                        if rv.term > current_term {
                            info!("Peer {} is on a newer term {}", peer, rv.term);
                            ElectionResult::OutdatedTerm(rv.term)
                        } else if rv.success {
                            info!("Received vote from peer {} in term {}", peer, current_term);
                            ElectionResult::Winner
                        } else {
                            info!(
                                "Peer {} has voted for another candidate in term {}",
                                peer, current_term
                            );
                            ElectionResult::NotWinner
                        }
                    }
                    Err(e) => {
                        info!(
                            "Error received from peer {} in term {}: {}",
                            peer, current_term, e
                        );
                        ElectionResult::NotWinner
                    }
                };
                if votes_tx.send(vote).await.is_err() {
                    debug!(
                        "Term {} has already been tallied, not counting peer {}",
                        current_term, peer
                    );
                }
            });
        }
        Ok((current_term, votes_rx))
    }

    async fn tally_results(
        &self,
        term: usize,
        votes_rx: Receiver<ElectionResult>,
    ) -> ProtocolState {
        // Calculate the majority, then tally up the votes as they come in.
        // If we end up with an outdated term, update the stored term data and drop to being a follower.
        // If we don't win enough votes, remain a candidate.
        // If we win a majority of votes, promote to being the leader.
        let majority = match self.configuration.lock().await.voting_majority() {
            Some(majority) => majority,
            None => {
                info!("There cannot be a voting majority; dropping to follower state");
                return ProtocolState::Follower;
            }
        };
        let mut votes = 1;
        while let Ok(vote) = votes_rx.recv().await {
            match vote {
                ElectionResult::Winner => {
                    votes += 1;
                    if votes >= majority {
                        info!(
                            "Received a majority of votes in term {}, converting to leader",
                            term
                        );
                        return ProtocolState::Leader;
                    }
                }
                ElectionResult::NotWinner => {}
                ElectionResult::OutdatedTerm(term) => {
                    // Reset the term data and drop to being a follower
                    let mut storage = self.storage.lock().await;
                    storage
                        .set_current_term(term)
                        .unwrap_or_else(|e| error!("Could not set newer term {}: {}", term, e));
                    storage
                        .set_voted_for(None)
                        .unwrap_or_else(|e| error!("Could not clear vote: {}", e));
                    return ProtocolState::Follower;
                }
            }
        }
        info!(
            "Did not receive a majority of votes, term {} had no election winner",
            term
        );
        ProtocolState::Candidate
    }
}

enum ElectionResult {
    Winner,
    NotWinner,
    OutdatedTerm(usize),
}

pub struct Leader {
    configuration: Lock<RaftConfiguration>,
    storage: Lock<Box<dyn Storage>>,
    commit_index: Lock<usize>,
    term_updates_rx: Receiver<()>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
    new_logs_rx: Receiver<usize>,
    index_update_rx: Receiver<(String, usize)>,
    match_index: HashMap<String, usize>,
    forward_updates: Vec<Sender<usize>>,
}

impl Leader {
    pub async fn start(
        id: String,
        configuration: Lock<RaftConfiguration>,
        timeout: Duration,
        storage: Lock<Box<dyn Storage>>,
        rpc: Arc<dyn RaftServerRPC>,
        commit_index: Lock<usize>,
        term_updates_tx: Sender<()>,
        term_updates_rx: Receiver<()>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        new_logs_rx: Receiver<usize>,
    ) -> std::io::Result<Leader> {
        let timeout = timeout.mul_f32(0.5);
        let (index_update_tx, index_update_rx) = async_channel::bounded(15);
        let mut forward_updates = Vec::new();
        let next_index = { storage.lock().await.last_index()? + 1 };
        for peer in configuration.lock().await.peers() {
            let (hb_tx, hb_rx) = async_channel::bounded(1);
            forward_updates.push(hb_tx);

            let mut hb = Forwarder::new(
                id.clone(),
                peer,
                timeout,
                index_update_tx.clone(),
                storage.clone(),
                commit_index.clone(),
                hb_rx,
                term_updates_tx.clone(),
                rpc.clone(),
                next_index,
            );
            tokio::spawn(async move { hb.run().await });
        }
        Ok(Leader::new(
            configuration,
            storage,
            commit_index,
            term_updates_rx,
            commits_to_apply_tx,
            new_logs_rx,
            index_update_rx,
            forward_updates,
        ))
    }

    pub fn new(
        configuration: Lock<RaftConfiguration>,
        storage: Lock<Box<dyn Storage>>,
        commit_index: Lock<usize>,
        term_updates_rx: Receiver<()>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        new_logs_rx: Receiver<usize>,
        index_update_rx: Receiver<(String, usize)>,
        forward_updates: Vec<Sender<usize>>,
    ) -> Leader {
        Leader {
            configuration,
            storage,
            commit_index,
            term_updates_rx,
            commits_to_apply_tx,
            new_logs_rx,
            index_update_rx,
            match_index: HashMap::new(),
            forward_updates,
        }
    }

    pub async fn run(&mut self) -> ProtocolState {
        loop {
            match self.run_once().await {
                ProtocolState::Leader => {}
                other => return other,
            }
        }
    }

    pub async fn run_once(&mut self) -> ProtocolState {
        select! {
            // We could receive an RPC indicating a newer term should be used:
            t = self.term_updates_rx.recv() => match t {
                Ok(_) => ProtocolState::Follower,
                Err(_) => ProtocolState::Shutdown,
            },
            // If the local agent adds a commit, advertise it to our followers:
            log = self.new_logs_rx.recv() => match log {
                Ok(index) => {
                    self.advertise_new_log(index);
                    ProtocolState::Leader
                },
                _ => ProtocolState::Shutdown,
            },
            // If our followers process an update, check if we can advance our commit index:
            index = self.index_update_rx.recv() => match index {
                Ok((peer, index)) => self.process_index_update(peer, index).await,
                _ => ProtocolState::Shutdown,
            },
        }
    }

    fn advertise_new_log(&mut self, index: usize) {
        // Try to submit a new index, but don't block if the queue is full.
        // Discard any peer connections that have hung up.
        self.forward_updates.retain(|hb| match hb.try_send(index) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => true,
            Err(TrySendError::Closed(_)) => false,
        });
    }

    async fn process_index_update(&mut self, peer_id: String, index: usize) -> ProtocolState {
        self.match_index.insert(peer_id, index);

        match self.update_commit_index(index).await {
            Ok(state) => state,
            Err(e) => {
                error!("Failed to update current commit index: {}", e);
                ProtocolState::Follower
            }
        }
    }

    async fn update_commit_index(&self, index: usize) -> std::io::Result<ProtocolState> {
        // Find the number of voting peers that have this new index value or greater.
        // Add one to that count for ourselves; the matched index is always on the leader.
        let configuration = self.configuration.lock().await;
        let updated = self
            .match_index
            .iter()
            .filter(|(_, i)| **i >= index)
            .filter(|(peer_id, _)| {
                configuration
                    .peers
                    .iter()
                    .any(|p| &p.id == *peer_id && p.voting)
            })
            .count()
            + 1;

        // See if there are enough with the log persisted to consider it committed
        if configuration
            .voting_majority()
            .map(|majority| updated < majority)
            .unwrap_or(true)
        {
            // Not enough indices match - wait for more commits before updating the commit index.
            return Ok(ProtocolState::Leader);
        }

        let storage = self.storage.lock().await;
        let current_term = storage.current_term()?;
        match storage.get_term(index)? {
            Some(term) => {
                let mut commit_index = self.commit_index.lock().await;
                // To ensure data integrity, we do not commit entries that did not originate in
                // this term. This ensures that a committed entry had the same leader from when
                // the request started until the commit completed.
                if index > *commit_index && current_term == term {
                    let cmd = storage.get_command(index)?;
                    debug!(
                        "Increasing commit index to {} from {}",
                        index, *commit_index
                    );
                    *commit_index = index;
                    if self.commits_to_apply_tx.send((index, cmd)).await.is_err() {
                        trace!("State machine processing disconnected; shutting down");
                        return Ok(ProtocolState::Shutdown);
                    }
                } else {
                    debug!(
                        "A majority of servers have index {} (in old term {})",
                        index, term
                    );
                }
                Ok(ProtocolState::Leader)
            }
            None => {
                error!(
                    "Log index {} should have already been available; is the storage corrupted?",
                    index
                );
                Ok(ProtocolState::Follower)
            }
        }
    }
}

pub struct Forwarder {
    id: String,
    peer: Peer,
    heartbeat_interval: Duration,
    append_entries_timeout: Duration,
    next_index: usize,
    match_index_tx: Sender<(String, usize)>,
    storage: Lock<Box<dyn Storage>>,
    commit_index: Lock<usize>,
    new_logs_rx: Receiver<usize>,
    term_updates_tx: Sender<()>,
    rpc: Arc<dyn RaftServerRPC>,
}

impl Forwarder {
    pub fn new(
        id: String,
        peer: Peer,
        timeout: Duration,
        match_index_tx: Sender<(String, usize)>,
        storage: Lock<Box<dyn Storage>>,
        commit_index: Lock<usize>,
        new_logs_rx: Receiver<usize>,
        term_updates_tx: Sender<()>,
        rpc: Arc<dyn RaftServerRPC>,
        next_index: usize,
    ) -> Forwarder {
        // Raft is able to elect and maintain a steady leader as long as the system satisfies the
        // timing requirement
        //     broadcastTime << electionTimeout << MTBF
        // The broadcast time should be an order of magnitude less than the election timeout.
        let heartbeat_interval = timeout.mul_f32(0.5);
        // We should not wait indefinitely for an RPC call to complete; we may encounter a slow
        // follower, which needs more time to process the request. For this reason, wait an order
        // of magnitude longer than the election timeout.
        let append_entries_timeout = timeout.mul_f32(5.0);
        Forwarder {
            id,
            peer,
            heartbeat_interval,
            append_entries_timeout,
            next_index,
            match_index_tx,
            storage,
            commit_index,
            new_logs_rx,
            term_updates_tx,
            rpc,
        }
    }

    pub async fn run(&mut self) {
        while self.run_once().await {}
    }

    pub async fn run_once(&mut self) -> bool {
        if self.next_request().await.is_err() {
            debug!("Stopping forwarder {} because leadership hung up", self.id);
            return false;
        }
        let request = match self.build_request().await {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to load data for AppendEntries request: {}", e);
                return true;
            }
        };
        let current_term = request.term;
        let max_sent_index = request.entries.last().map(|l| l.index);

        // Send the request
        let response = self.rpc.append_entries(self.peer.address.clone(), request);
        // Only wait for so long before we consider it a failure:
        match tokio::time::timeout(self.append_entries_timeout, response).await {
            Ok(Ok(resp)) => {
                if resp.term > current_term {
                    info!("Peer {} is on a newer term {}", self.peer.id, resp.term);
                    let mut storage = self.storage.lock().await;
                    storage.set_current_term(resp.term).unwrap();
                    storage.set_voted_for(None).unwrap();
                    // Send the term update; if the send fails then the protocol has shutdown.
                    let _ = self.term_updates_tx.send(()).await;
                    debug!(
                        "Stopping forwarder {} because we received a newer term",
                        self.peer
                    );
                    return false;
                } else if !resp.success {
                    debug!(
                        "Peer {} could not process log index {}",
                        self.peer, self.next_index
                    );
                    if self.next_index > 1 {
                        self.next_index -= 1;
                    }
                } else if let Some(max_index) = max_sent_index {
                    trace!(
                        "Peer {} processed AppendEntries successfully to index {}",
                        self.peer,
                        max_index
                    );
                    self.next_index = max_index + 1;
                    if self
                        .match_index_tx
                        .send((self.peer.id.clone(), max_index))
                        .await
                        .is_err()
                    {
                        debug!("Stopping forwarder {} because leadership hung up", self.id);
                        return false;
                    }
                }
            }
            Ok(Err(e)) => {
                error!(
                    "Peer {} responded to AppendEntries with error: {}",
                    self.peer, e
                );
            }
            Err(_) => {
                error!("AppendEntries to peer {} timed out", self.peer);
            }
        }
        true
    }

    async fn next_request(&self) -> Result<(), RecvError> {
        if self.new_logs_rx.try_recv().is_ok() {
            return Ok(());
        }
        // Wait for the next update request, or the next heartbeat interval, whichever is first.
        // If the
        if let Ok(Err(e)) =
            tokio::time::timeout(self.heartbeat_interval, self.new_logs_rx.recv()).await
        {
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn build_request(&self) -> std::io::Result<AppendEntriesRequest> {
        let leader_commit = { *self.commit_index.lock().await };
        let storage = self.storage.lock().await;
        let term = storage.current_term()?;
        let prev_log_index = self.next_index - 1;
        let prev_log_term = storage
            .get_term(prev_log_index)?
            .expect("log is missing entries!");

        // Send up to 100 entries at a time.
        let mut entries = Vec::new();
        let max_index = storage.last_index()?;
        for index in (self.next_index..(max_index + 1)).take(100) {
            let command = storage.get_command(index)?;
            let term = storage.get_term(index)?.unwrap();
            entries.push(LogEntry {
                index,
                term,
                command,
            });
        }
        Ok(AppendEntriesRequest {
            leader_id: self.id.clone(),
            term,
            leader_commit,
            prev_log_index,
            prev_log_term,
            entries,
        })
    }
}

#[derive(Debug)]
pub enum RaftServiceError {
    RaftProtocolTerminated,
    StorageError(std::io::Error),
}

impl std::fmt::Display for RaftServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftServiceError::RaftProtocolTerminated => {
                write!(f, "The raft protocol has been terminated")
            }
            RaftServiceError::StorageError(e) => {
                write!(f, "Error while interacting with stable storage: {}", e)
            }
        }
    }
}

impl From<std::io::Error> for RaftServiceError {
    fn from(e: std::io::Error) -> Self {
        RaftServiceError::StorageError(e)
    }
}

impl std::error::Error for RaftServiceError {}

#[derive(Clone)]
pub struct RaftService {
    configuration: Lock<RaftConfiguration>,
    storage: Lock<Box<dyn Storage>>,
    term_updates: Sender<()>,
    append_entries_tx: Sender<usize>,
    current_state: Lock<ProtocolState>,
    new_logs_tx: Sender<usize>,
    last_applied_tx: Lock<Vec<Sender<usize>>>,
    state_machine: Lock<StateMachine>,
}

impl RaftService {
    pub async fn append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> std::io::Result<AppendEntriesResponse> {
        let mut storage = self.storage.lock().await;

        let current_term = storage.current_term()?;
        // Ensure we are on the latest term
        if req.term > current_term {
            storage.set_current_term(req.term)?;
            storage.set_voted_for(None)?;
            if self.term_updates.send(()).await.is_err() {
                error!("Raft protocol has been shut down");
            }
            return Ok(AppendEntriesResponse::failed(current_term));
        }
        // Ensure the leader is on the latest term
        if req.term < current_term {
            return Ok(AppendEntriesResponse::failed(current_term));
        }
        // Update the current leader:
        {
            let mut configuration = self.configuration.lock().await;
            if !configuration.is_current_leader(&req.leader_id) {
                info!(
                    "Updating local leader to point to the current leader {}",
                    req.leader_id
                );

                configuration.set_current_leader(&req.leader_id);
            }
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
        drop(storage);

        // Update the commit index with the latest committed value in our logs
        if self
            .append_entries_tx
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
    ) -> std::io::Result<RequestVoteResponse> {
        let mut storage = self.storage.lock().await;

        // Ensure we are on the latest term - if we are not, update and continue processing the request.
        let current_term = {
            let current_term = storage.current_term()?;
            if req.term > current_term {
                storage.set_current_term(req.term)?;
                if self.term_updates.send(()).await.is_err() {
                    error!("Raft protocol has been shut down");
                    return Ok(RequestVoteResponse::failed(current_term));
                }
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
            .map(|c| c == req.candidate_id)
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

    pub async fn apply(&self, cmd: Command) -> Result<ClientApplyResponse, RaftServiceError> {
        {
            match *self.current_state.lock().await {
                ProtocolState::Leader => (),
                ProtocolState::Shutdown => return Err(RaftServiceError::RaftProtocolTerminated),
                ProtocolState::Candidate | ProtocolState::Follower => {
                    let leader_address = self.configuration.lock().await.current_leader_address();
                    debug!("Not leader, responding to apply with {:?}", leader_address);
                    return Ok(ClientApplyResponse {
                        leader_address,
                        success: false,
                    });
                }
            }
        }
        let command = LogCommand::Command(cmd);

        let index = {
            let mut storage = self.storage.lock().await;

            let term = storage.current_term()?;

            storage.append_entry(term, command)?
        };

        let (tx, rx) = async_channel::bounded(1);
        {
            let mut vec = self.last_applied_tx.lock().await;
            vec.push(tx);
        }

        self.new_logs_tx
            .send(index)
            .await
            .map_err(|_| RaftServiceError::RaftProtocolTerminated)?;

        while let Ok(last_applied) = rx.recv().await {
            if last_applied >= index {
                let leader_address = self.configuration.lock().await.current_leader_address();
                return Ok(ClientApplyResponse {
                    leader_address,
                    success: true,
                });
            }
        }
        Err(RaftServiceError::RaftProtocolTerminated)
    }

    pub async fn query(&self, query: Query) -> Result<ClientQueryResponse, RaftServiceError> {
        {
            match *self.current_state.lock().await {
                ProtocolState::Leader => (),
                ProtocolState::Shutdown => return Err(RaftServiceError::RaftProtocolTerminated),
                ProtocolState::Candidate | ProtocolState::Follower => {
                    let leader_address = self.configuration.lock().await.current_leader_address();
                    return Ok(ClientQueryResponse {
                        leader_address,
                        response: None,
                    });
                }
            }
        }
        // TODO: Send heartbeat, wait for majority to respond
        // After majority responds, query state machine

        let leader_address = { self.configuration.lock().await.current_leader_address() };
        let state_machine = self.state_machine.lock().await;
        let response = state_machine.query(query);
        Ok(ClientQueryResponse {
            leader_address,
            response: Some(response),
        })
    }
}

pub struct ServerConfig {
    id: String,
    address: String,
    peers: Vec<Peer>,
    timeout: Option<Duration>,
    storage: Option<Lock<Box<dyn Storage>>>,
}

impl ServerConfig {
    pub fn new(id: &str, address: &str) -> ServerConfig {
        ServerConfig {
            id: id.to_string(),
            address: address.to_string(),
            peers: Vec::new(),
            timeout: None,
            storage: None,
        }
    }

    pub fn peer(&mut self, id: &str, address: &str) -> &mut Self {
        self.peers.push(Peer {
            id: id.to_string(),
            address: address.to_string(),
            voting: true,
        });
        self
    }

    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn in_memory_storage(&mut self) -> &mut Self {
        self.storage = Some(Lock::new(Box::new(MemoryStorage::new())));
        self
    }

    #[cfg(feature = "http-rpc")]
    pub fn spawn_http(&mut self) -> Result<(), std::io::Error> {
        let address = self.address.clone();
        self.spawn_protocol(|server| HttpRPC::spawn_server(&address, server))
    }

    fn spawn_protocol<R: RaftServerRPC + 'static, F: FnOnce(RaftService) -> std::io::Result<R>>(
        &mut self,
        rpc: F,
    ) -> std::io::Result<()> {
        let id = self.id.clone();
        let storage = self.storage.clone().expect("no storage device configured");
        self.storage = None;
        let timeout = self.timeout.unwrap_or_else(|| Duration::from_millis(150));
        let configuration = Lock::new(RaftConfiguration::new(
            id.clone(),
            self.address.clone(),
            self.peers.clone(),
        ));
        let state_machine = Lock::new(StateMachine::default());
        let current_state = Lock::new(ProtocolState::Follower);
        let last_applied_tx = Lock::new(Vec::new());
        let (commits_to_apply_tx, commits_to_apply_rx) = async_channel::bounded(1);
        let (append_entries_tx, append_entries_rx) = async_channel::bounded(1);
        let (term_updates_tx, term_updates_rx) = async_channel::bounded(1);
        let (new_logs_tx, new_logs_rx) = async_channel::bounded(1);

        let raft_server = RaftService {
            configuration: configuration.clone(),
            storage: storage.clone(),
            term_updates: term_updates_tx.clone(),
            append_entries_tx,
            current_state: current_state.clone(),
            new_logs_tx,
            last_applied_tx: last_applied_tx.clone(),
            state_machine: state_machine.clone(),
        };

        let rpc = Arc::new(rpc(raft_server)?) as Arc<dyn RaftServerRPC>;

        tokio::spawn(async move {
            debug!("Starting log committer task");
            let mut lc = LogCommitter::new(state_machine, commits_to_apply_rx, last_applied_tx);
            lc.run().await;
            debug!("Closing log committer task");
        });
        tokio::spawn(async move {
            let mut tasks = ProtocolTasks::new(
                id,
                configuration,
                timeout,
                storage,
                current_state,
                rpc,
                term_updates_tx,
                term_updates_rx,
                commits_to_apply_tx,
                append_entries_rx,
                new_logs_rx,
            );
            debug!("Starting raft protocol task");
            tasks.run().await;
            debug!("Closing raft protocol task");
        });
        Ok(())
    }
}
