use async_trait::async_trait;
use futures::lock::Mutex;
use futures::stream::FuturesUnordered;
use futures::{future::ready, join, FutureExt};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::error::{Error, Result};
use crate::rpc::{AppendEntriesRequest, RaftServer, RequestVoteRequest, RPC};
use crate::storage::{LogCommand, LogEntry, Storage};

#[derive(Clone)]
pub struct Peer {
    id: String,
    address: String,
    voting: bool,
    leader: bool,
}

impl Peer {
    pub fn voting(id: String, address: String) -> Peer {
        Peer {
            id,
            address,
            voting: true,
            leader: false,
        }
    }
}

#[async_trait]
pub trait StateMachine {
    async fn apply(&self, index: usize, command: &[u8]) -> Result<()>;
}

pub fn start<S, R, F>(
    id: String,
    peers: Vec<Peer>,
    timeout: Duration,
    storage: S,
    rpc: R,
    state_machine: F,
) -> Result<(
    impl Future<Output = Result<()>>,
    RaftServer<S>,
    Consensus<S>,
)>
where
    S: Storage,
    R: RPC,
    F: StateMachine,
{
    let storage = Arc::new(Mutex::new(storage));
    let (commits_protocol_tx, commits_rx) = channel(1);
    let (commits_tx, commits_protocol_rx) = channel(1);
    let (last_applied_tx, last_applied_rx) = channel(1);
    let (term_updates_tx, term_updates_rx) = channel(1);
    let (new_logs_tx, new_logs_rx) = channel(1);

    let mut lc = LogCommitter::new(storage.clone(), state_machine, commits_rx, last_applied_tx);

    let mut tasks = ProtocolTasks::start(
        id,
        peers,
        timeout,
        storage.clone(),
        rpc,
        term_updates_tx.clone(),
        term_updates_rx,
        commits_protocol_tx,
        commits_protocol_rx,
        new_logs_rx,
    )?;

    let raft_server = crate::rpc::RaftServer::new(storage.clone(), term_updates_tx, commits_tx);

    let consensus = Consensus {
        storage,
        current_state: tasks.current_state.clone(),
        log_updates: new_logs_tx,
        last_applied_rx,
    };

    let fut = async move {
        join!(lc.run(), tasks.run());
        Ok(())
    };

    Ok((fut, raft_server, consensus))
}

pub struct Consensus<S: Storage> {
    storage: Arc<Mutex<S>>,
    current_state: Arc<Mutex<ProtocolState>>,
    log_updates: Sender<usize>,
    last_applied_rx: Receiver<usize>,
}

impl<S: Storage> Consensus<S> {
    pub async fn commit_command(&mut self, cmd: &[u8]) -> Result<()> {
        if let ProtocolState::Leader = *self.current_state.lock().await {
            return Err(Error::NotLeader.into());
        }

        let index = {
            let mut storage = self.storage.lock().await;

            let term = storage.current_term()?;
            let command = LogCommand::Command(cmd.into());

            storage.append_entry(term, command)?
        };

        self.log_updates
            .send(index)
            .await
            .map_err(|_| Error::RaftProtocolTerminated)?;

        while let Some(last_applied) = self.last_applied_rx.recv().await {
            if last_applied >= index {
                return Ok(());
            }
        }
        Err(Error::RaftProtocolTerminated.into())
    }
}

#[derive(Clone, Copy)]
enum ProtocolState {
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

pub struct LogCommitter<S: Storage, F: StateMachine> {
    storage: Arc<Mutex<S>>,
    commits: Receiver<usize>,
    state_machine: F,
    last_applied: Arc<Mutex<usize>>,
    last_applied_tx: Sender<usize>,
}

impl<S: Storage, F: StateMachine> LogCommitter<S, F> {
    fn new(
        storage: Arc<Mutex<S>>,
        state_machine: F,
        commits: Receiver<usize>,
        last_applied_tx: Sender<usize>,
    ) -> LogCommitter<S, F> {
        let last_applied = Arc::new(Mutex::new(0));
        LogCommitter {
            storage,
            commits,
            state_machine,
            last_applied,
            last_applied_tx,
        }
    }

    async fn run(&mut self) {
        while let Some(commit_index) = self.commits.recv().await {
            let mut last_applied = self.last_applied.lock().await;
            while commit_index > *last_applied {
                let index = *last_applied + 1;
                let cmd = { self.storage.lock().await.get_command(index) };
                match cmd {
                    Ok(cmd) => {
                        *last_applied = index;
                        match cmd {
                            LogCommand::Command(data) => {
                                if let Err(e) = self.state_machine.apply(index, &data).await {
                                    error!(
                                        "Failed to apply index {} to state machine: {}",
                                        index, e
                                    );
                                    return;
                                }
                            }
                            LogCommand::Noop => {}
                        }
                        if let Err(_) = self.last_applied_tx.send(index).await {
                            trace!("consensus struct has been dropped; closing log committer");
                            return;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to load command for applying to state machine: {}",
                            e
                        );
                        // TODO: Better retry timing
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        }
    }
}

pub struct ProtocolTasks<S: Storage, R: RPC> {
    id: String,
    timeout: Duration,
    peers: Vec<Peer>,
    storage: Arc<Mutex<S>>,
    rpc: R,
    commit_index: Arc<Mutex<usize>>,
    current_state: Arc<Mutex<ProtocolState>>,
    term_updates_tx: Sender<usize>,
    term_updates_rx: Receiver<usize>,
    commits_rx: Receiver<usize>,
    commits_tx: Sender<usize>,
    new_logs_rx: Receiver<usize>,
}

impl<S: Storage, R: RPC> ProtocolTasks<S, R> {
    fn start(
        id: String,
        peers: Vec<Peer>,
        timeout: Duration,
        storage: Arc<Mutex<S>>,
        rpc: R,
        term_updates_tx: Sender<usize>,
        term_updates_rx: Receiver<usize>,
        commits_tx: Sender<usize>,
        commits_rx: Receiver<usize>,
        new_logs_rx: Receiver<usize>,
    ) -> Result<ProtocolTasks<S, R>> {
        let current_state = Arc::new(Mutex::new(ProtocolState::Follower));
        let commit_index = Arc::new(Mutex::new(0));

        Ok(ProtocolTasks {
            id,
            peers,
            timeout,
            storage,
            rpc,
            commit_index,
            current_state,
            term_updates_tx,
            term_updates_rx,
            commits_tx,
            commits_rx,
            new_logs_rx,
        })
    }

    async fn run(&mut self) {
        loop {
            // Grab the current state and run the appropriate protocol logic.
            let state = { *self.current_state.lock().await };
            let state = match state {
                ProtocolState::Follower => self.run_follower().await,
                ProtocolState::Candidate => self.run_candidate().await,
                ProtocolState::Leader => self.run_leader().await,
                ProtocolState::Shutdown => return,
            };
            // Update the state with any changes
            {
                *self.current_state.lock().await = state;
            }
        }
    }

    async fn run_follower(&mut self) -> ProtocolState {
        use rand::prelude::*;
        use tokio::stream::StreamExt;

        let timeout = self.timeout.mul_f32(1.0 + rand::thread_rng().gen::<f32>());

        loop {
            select! {
                // Consume the term updates; sender is responsible for updating the storage:
                _ = self.term_updates_rx.next() => continue,
                // Actually process commits as they come in
                state = self.commits_rx.next() => match state {
                    Some(index) => {
                        let mut commit_index = self.commit_index.lock().await;
                        if index > *commit_index {
                            *commit_index = index;
                            if let Err(_) = self.commits_tx.send(index).await {
                                error!("State machine processing disconnected; shutting down");
                                return ProtocolState::Shutdown;
                            }
                        } else {
                            trace!("Heartbeat received");
                        }
                    }
                    // Server hung up, no more RPCs:
                    None => return ProtocolState::Shutdown,
                },
                _ = tokio::time::delay_for(timeout) => {
                    debug!("Timeout; transitioning to Candidate");
                    return ProtocolState::Candidate
                }
            }
        }
    }

    async fn run_candidate(&mut self) -> ProtocolState {
        use futures::stream::{FuturesUnordered, StreamExt};

        let request = match self.init_election().await {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to initialize election: {}", e);
                return ProtocolState::Follower;
            }
        };
        // Send out the RequestVote RPC calls.
        let mut votes = FuturesUnordered::new();
        for peer in self.peers.clone() {
            let request = request.clone();
            let current_term = request.term;
            let timeout = self.timeout;
            let vote = self.rpc.request_vote(peer.address.clone(), request);
            // Use a timeout on each of them, so that we will
            // have an election result from each within the election timeout.
            let vote = async move {
                match tokio::time::timeout(timeout, vote).await {
                    Ok(Ok(rv)) => {
                        if rv.term > current_term {
                            info!("Peer {} is on a newer term {}", peer.id, rv.term);
                            ElectionResult::OutdatedTerm(rv.term)
                        } else if rv.success {
                            info!("Peer {} voted for us in term {}", peer.id, current_term);
                            ElectionResult::Winner
                        } else {
                            info!(
                                "Peer {} did not vote for us in term {}",
                                peer.id, current_term
                            );
                            ElectionResult::NotWinner
                        }
                    }
                    Ok(Err(e)) => {
                        error!("Error received from peer {}: {}", peer.id, e);
                        ElectionResult::NotWinner
                    }
                    Err(_) => {
                        error!("Vote timed out from peer {}", peer.id);
                        ElectionResult::NotWinner
                    }
                }
            };
            votes.push(vote);
        }

        // Calculate the majority, then tally up the votes as they come in.
        // If we end up with an outdated term, update the stored value and drop to being a follower.
        // If we don't win enough votes, remain a candidate.
        // If we win a majority of votes, promote to being the leader.
        let majority = calculate_majority(&self.peers);

        let (votes, newer_term) = votes
            .collect::<Vec<ElectionResult>>()
            .await
            .into_iter()
            .fold((1, None), |(votes, newer_term), vote| match vote {
                ElectionResult::OutdatedTerm(t) => (votes, Some(t)),
                ElectionResult::Winner => (votes + 1, newer_term),
                ElectionResult::NotWinner => (votes, newer_term),
            });
        if let Some(t) = newer_term {
            let mut storage = self.storage.lock().await;
            storage
                .set_current_term(t)
                .unwrap_or_else(|e| error!("Could not set newer term {}: {}", t, e));
            storage
                .set_voted_for(None)
                .unwrap_or_else(|e| error!("Could not clear vote: {}", e));
            ProtocolState::Follower
        } else if votes >= majority {
            ProtocolState::Leader
        } else {
            ProtocolState::Candidate
        }
    }

    async fn init_election(&mut self) -> Result<RequestVoteRequest> {
        let mut storage = self.storage.lock().await;

        // Increment the current term, and vote for ourselves:
        let current_term = storage.current_term()? + 1;
        storage.set_current_term(current_term)?;
        storage.set_voted_for(Some(self.id.clone()))?;

        let last_log_term = storage.last_term()?;
        let last_log_index = storage.last_index()?;

        Ok(RequestVoteRequest {
            term: current_term,
            candidate_id: self.id.clone(),
            last_log_term,
            last_log_index,
        })
    }

    async fn run_leader(&mut self) -> ProtocolState {
        let (new_logs_multi_tx, new_logs_multi_rx) = tokio::sync::watch::channel(0);
        let (index_update_tx, mut index_update_rx) = channel(1);
        let mut match_index = HashMap::new();
        let mut heartbeats = FuturesUnordered::new();
        for peer in self.peers.iter() {
            match_index.insert(peer.id.clone(), 0);

            let hb = Heartbeater::new(
                self.id.clone(),
                peer.clone(),
                self.timeout,
                index_update_tx.clone(),
                self.storage.clone(),
                self.commit_index.clone(),
                new_logs_multi_rx.clone(),
                self.term_updates_tx.clone(),
                self.rpc.clone(),
            );
            let peer_id = peer.id.clone();
            let term_updates_tx = self.term_updates_tx.clone();
            match hb {
                Ok(mut hb) => heartbeats.push(async move { hb.run().await }),
                Err(e) => {
                    error!("Failed to create heartbeat for peer {}: {}", peer_id, e);
                    return ProtocolState::Follower;
                }
            }
        }

        loop {
            select! {
                newer_term = self.term_updates_rx.recv() => {
                    info!("Newer term {:?} discovered - converting to follower", newer_term);
                    return ProtocolState::Follower;
                }
                index = self.new_logs_rx.recv() => {
                    if let Some(index) = index {
                        if let Err(_) = new_logs_multi_tx.broadcast(index) {
                            return ProtocolState::Follower;
                        }
                    }
                }
                res = index_update_rx.recv() => {
                    if let Some((peer, index)) = res {
                        match_index.insert(peer.id, index);

                        let updated = match_index.iter().filter(|(_, i)| **i >= index).count();
                        if updated >= calculate_majority(&self.peers) {
                            let storage = self.storage.lock().await;
                            match storage.current_term() {
                                Ok(current_term) => {
                                    match storage.get_term(index) {
                                        Ok(Some(term)) => {
                                            if current_term == term {
                                                let mut commit_index = self.commit_index.lock().await;
                                                *commit_index = index;
                                                if let Err(_) = self.commits_tx.send(index).await {
                                                    error!("State machine processing disconnected; shutting down");
                                                    return ProtocolState::Shutdown;
                                                }
                                            }
                                        }
                                        Ok(None) => {
                                            error!(
                                                "Failed to load expected log index {} while processing commit index",
                                                index
                                            );
                                            return ProtocolState::Follower;
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to load log index {} while processing commit index: {}",
                                                index, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to get current term to process commit index: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn calculate_majority(peers: &Vec<Peer>) -> usize {
    match peers.iter().filter(|p| p.voting).count() {
        0 => 1,
        1 | 2 => 2,
        3 | 4 => 3,
        x => panic!("Too many voting peers (found {})", x),
    }
}

enum ElectionResult {
    Winner,
    NotWinner,
    OutdatedTerm(usize),
}

struct Heartbeater<S: Storage, R: RPC> {
    id: String,
    peer: Peer,
    heartbeat_interval: Duration,
    append_entries_timeout: Duration,
    next_index: usize,
    match_index: Sender<(Peer, usize)>,
    storage: Arc<Mutex<S>>,
    commit_index: Arc<Mutex<usize>>,
    new_logs_rx: tokio::sync::watch::Receiver<usize>,
    term_updates_tx: Sender<usize>,
    rpc: R,
}

impl<S: Storage, R: RPC> Heartbeater<S, R> {
    fn new(
        id: String,
        peer: Peer,
        timeout: Duration,
        match_index: Sender<(Peer, usize)>,
        storage: Arc<Mutex<S>>,
        commit_index: Arc<Mutex<usize>>,
        new_logs_rx: tokio::sync::watch::Receiver<usize>,
        term_updates_tx: Sender<usize>,
        rpc: R,
    ) -> Result<Heartbeater<S, R>> {
        let heartbeat_interval = timeout.mul_f32(0.3);
        let append_entries_timeout = timeout.mul_f32(5.0);
        let next_index = futures::executor::block_on(storage.lock()).last_index()?;
        Ok(Heartbeater {
            id,
            peer,
            heartbeat_interval,
            append_entries_timeout,
            next_index,
            match_index,
            storage,
            commit_index,
            new_logs_rx,
            term_updates_tx,
            rpc,
        })
    }

    async fn run(&mut self) -> Option<usize> {
        let max_index = select! {
            _ = tokio::time::delay_for(self.heartbeat_interval) => None,
            log = self.new_logs_rx.recv() => Some(log),
        };
        while let Some(max_index) = max_index {
            let (request, current_term, max_index_sent) = match self.create_request(max_index).await
            {
                Ok(req) => req,
                Err(e) => {
                    error!("Failed to get data for AppendEntries request: {}", e);
                    continue;
                }
            };
            let response = self.rpc.append_entries(self.peer.address.clone(), request);

            match tokio::time::timeout(self.append_entries_timeout, response).await {
                Ok(Ok(resp)) => {
                    if resp.term > current_term {
                        info!("Peer {} is on a newer term {}", self.peer.id, resp.term);
                        return Some(resp.term);
                    } else if !resp.success {
                        info!(
                            "Peer {} could not process log index {}",
                            self.peer.id, self.next_index
                        );
                        if self.next_index > 1 {
                            self.next_index -= 1;
                        }
                    } else {
                        debug!("Peer {} processed AppendEntries successfully", self.peer.id);
                        if let Some(max_index) = max_index_sent {
                            self.next_index = max_index + 1;
                            if let Err(_) =
                                self.match_index.send((self.peer.clone(), max_index)).await
                            {
                                // The leader is no longer listening for updates, we must be in a different state now
                                return None;
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "Error received from AppendEntries request to peer {}: {}",
                        self.peer.id, e
                    );
                }
                Err(_) => {
                    error!("AppendEntries request timed out to peer {}", self.peer.id);
                }
            }
        }
        // Update channel closed; we don't know if there's a newer term.
        None
    }

    async fn create_request(
        &mut self,
        max_index: Option<usize>,
    ) -> Result<(AppendEntriesRequest, usize, Option<usize>)> {
        let leader_commit = { *self.commit_index.lock().await };
        let storage = self.storage.lock().await;
        let term = storage.current_term()?;
        let prev_log_index = self.next_index - 1;
        let prev_log_term = storage
            .get_term(prev_log_index)?
            .expect("log is missing entries!");

        let mut entries = Vec::new();
        if let Some(index) = max_index {
            for index in self.next_index..(index + 1) {
                let command = storage.get_command(index)?;
                let term = storage.get_term(index)?.unwrap();
                entries.push(LogEntry {
                    index,
                    term,
                    command,
                });
            }
        }
        let max_sent_index = entries.iter().last().map(|l| l.index);
        Ok((
            AppendEntriesRequest {
                leader_id: self.id.clone(),
                term,
                leader_commit,
                prev_log_index,
                prev_log_term,
                entries,
            },
            term,
            max_sent_index,
        ))
    }
}
