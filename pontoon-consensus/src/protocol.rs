use futures::stream::FuturesUnordered;
use futures::{executor::ThreadPool, future::ready, FutureExt, StreamExt};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

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

pub fn start<S, R, F>(
    id: String,
    peers: Vec<Peer>,
    timeout: Duration,
    storage: Arc<RwLock<S>>,
    rpc: R,
    state_machine: F,
) -> Result<(RaftServer<S>, Consensus<S>)>
where
    S: Storage,
    R: RPC,
    F: FnMut(usize, Vec<u8>) + Sync + Send + 'static,
{
    let (commit_updates, last_applied_rx) = LogCommitter::start(&storage, state_machine);

    let (current_state, new_logs_tx, term_updates_tx) =
        ProtocolTasks::start(id, peers, timeout, &storage, rpc, commit_updates)?;

    let consensus_new_logs_tx = new_logs_tx.clone();

    let raft_server = crate::rpc::RaftServer::new(
        &storage,
        move |term| {
            if let Err(_) = term_updates_tx.send(term) {
                warn!("Raft protocol has terminated");
            }
        },
        move |commit| {
            if let Err(_) = new_logs_tx.send(commit) {
                warn!("Raft protocol has terminated");
            }
        },
    );

    let consensus = Consensus {
        storage,
        current_state,
        log_updates: consensus_new_logs_tx,
        last_applied_rx,
    };

    Ok((raft_server, consensus))
}

pub struct Consensus<S: Storage> {
    storage: Arc<RwLock<S>>,
    current_state: Arc<Mutex<ProtocolState>>,
    log_updates: Sender<usize>,
    last_applied_rx: Receiver<usize>,
}

impl<S: Storage> Consensus<S> {
    pub fn commit_command(&mut self, cmd: &[u8]) -> Result<()> {
        if let ProtocolState::Leader = *self.current_state.lock().expect("poisoned lock") {
            return Err(Error::NotLeader.into());
        }

        let index = {
            let mut storage = self.storage.write().expect("poisoned lock");

            let term = storage.current_term()?;
            let command = LogCommand::Command(cmd.into());

            storage.append_entry(term, command)?
        };

        self.log_updates
            .send(index)
            .map_err(|_| Error::RaftProtocolTerminated)?;

        while self
            .last_applied_rx
            .recv()
            .map_err(|_| Error::RaftProtocolTerminated)?
            < index
        {}
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ProtocolState {
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

pub struct LogCommitter<S: Storage, F: FnMut(usize, Vec<u8>)> {
    storage: Arc<RwLock<S>>,
    commits: Receiver<usize>,
    state_machine: F,
    last_applied: Arc<Mutex<usize>>,
    last_applied_tx: Sender<usize>,
}

impl<S: Storage, F: FnMut(usize, Vec<u8>) + Send + 'static> LogCommitter<S, F> {
    fn start(storage: &Arc<RwLock<S>>, state_machine: F) -> (Sender<usize>, Receiver<usize>) {
        let storage = storage.clone();
        let last_applied = Arc::new(Mutex::new(0));
        let (incoming_commits, commits) = std::sync::mpsc::channel();
        let (last_applied_tx, last_applied_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let mut sm = LogCommitter {
                storage,
                commits,
                state_machine,
                last_applied,
                last_applied_tx,
            };
            sm.run();
        });
        (incoming_commits, last_applied_rx)
    }

    fn run(&mut self) {
        while let Ok(commit_index) = self.commits.recv() {
            let mut last_applied = self.last_applied.lock().expect("poisoned lock");
            while commit_index > *last_applied {
                let index = *last_applied + 1;
                let cmd = {
                    self.storage
                        .read()
                        .expect("poisoned lock")
                        .get_command(index)
                };
                match cmd {
                    Ok(cmd) => {
                        *last_applied = index;
                        match cmd {
                            LogCommand::Command(data) => {
                                (self.state_machine)(index, data);
                            }
                            LogCommand::Noop => {}
                        }
                        if let Err(_) = self.last_applied_tx.send(index) {
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
    storage: Arc<RwLock<S>>,
    rpc: R,
    commit_index: Arc<Mutex<usize>>,
    current_state: Arc<Mutex<ProtocolState>>,
    executor: ThreadPool,
    term_updates_tx: Sender<usize>,
    term_updates_rx: Receiver<usize>,
    commit_updates_tx: Sender<usize>,
    new_logs_rx: tokio::sync::watch::Receiver<usize>,
}

impl<S: Storage, R: RPC> ProtocolTasks<S, R> {
    fn start(
        id: String,
        peers: Vec<Peer>,
        timeout: Duration,
        storage: &Arc<RwLock<S>>,
        rpc: R,
        commit_updates_tx: Sender<usize>,
    ) -> Result<(Arc<Mutex<ProtocolState>>, Sender<usize>, Sender<usize>)> {
        let (term_updates_tx, term_updates_rx) = std::sync::mpsc::channel();
        let current_state = Arc::new(Mutex::new(ProtocolState::Follower));
        let commit_index = Arc::new(Mutex::new(0));
        let executor = ThreadPool::new()?;

        let (new_logs_tx, new_logs_rx) = std::sync::mpsc::channel();
        let (new_logs_multi_tx, new_logs_multi_rx) = tokio::sync::watch::channel(1);
        std::thread::spawn(move || {
            // Errors from either sending or receiving should terminate this thread
            while let Ok(index) = new_logs_rx.recv() {
                if new_logs_multi_tx.broadcast(index).is_err() {
                    return;
                }
            }
        });

        let mut protocol = ProtocolTasks {
            id,
            peers,
            timeout,
            storage: storage.clone(),
            rpc,
            commit_index,
            current_state: current_state.clone(),
            executor,
            term_updates_tx: term_updates_tx.clone(),
            term_updates_rx,
            commit_updates_tx,
            new_logs_rx: new_logs_multi_rx,
        };
        thread::spawn(move || {
            protocol.run();
        });
        Ok((current_state, new_logs_tx, term_updates_tx))
    }

    fn run(&mut self) {
        loop {
            // Grab the current state and run the appropriate protocol logic.
            let state = { *self.current_state.lock().expect("poisoned lock") };
            let state = match state {
                ProtocolState::Follower => self.run_follower(),
                ProtocolState::Candidate => self.run_candidate(),
                ProtocolState::Leader => self.run_leader(),
                ProtocolState::Shutdown => return,
            };
            // Update the state with any changes
            *self.current_state.lock().expect("poisoned lock") = state;
        }
    }

    fn run_follower(&mut self) -> ProtocolState {
        use rand::prelude::*;
        let timeout = self.timeout.mul_f32(1.0 + rand::thread_rng().gen::<f32>());

        loop {
            let update = tokio::time::timeout(timeout, self.new_logs_rx.recv());
            match futures::executor::block_on(async move { update.await }) {
                Ok(Some(index)) => {
                    let mut commit_index = self.commit_index.lock().expect("poisoned lock");
                    if index > *commit_index {
                        *commit_index = index;
                        if let Err(_) = self.commit_updates_tx.send(index) {
                            error!("State machine processing disconnected; shutting down");
                            return ProtocolState::Shutdown;
                        }
                    } else {
                        trace!("Heartbeat received");
                    }
                }
                Ok(None) => {
                    info!("Server processing disconnected; shutting down");
                    return ProtocolState::Shutdown;
                }
                Err(_) => {
                    info!("Exceeded timeout without being contacted by the leader; converting to candidate");
                    return ProtocolState::Candidate;
                }
            }
        }
    }

    fn run_candidate(&mut self) -> ProtocolState {
        let request = match self.init_election() {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to initialize election: {}", e);
                return ProtocolState::Follower;
            }
        };
        // Send out the RequestVote RPC calls. Use a timeout on each of them, so that we will
        // have an election result from each within the election timeout.
        let votes = FuturesUnordered::new();
        for peer in self.peers.iter().cloned() {
            let request = request.clone();
            let timeout = self.timeout;
            let current_term = request.term;

            let vote = self.rpc.request_vote(peer.address.clone(), request);
            let vote = tokio::time::timeout(timeout, vote).then(move |res| match res {
                Ok(Ok(rv)) => {
                    if rv.term > current_term {
                        info!("Peer {} is on a newer term {}", peer.id, rv.term);
                        ready(ElectionResult::OutdatedTerm(rv.term))
                    } else if rv.success {
                        info!("Peer {} voted for us in term {}", peer.id, current_term);
                        ready(ElectionResult::Winner)
                    } else {
                        info!(
                            "Peer {} did not vote for us in term {}",
                            peer.id, current_term
                        );
                        ready(ElectionResult::NotWinner)
                    }
                }
                Ok(Err(e)) => {
                    error!("Error received from peer {}: {}", peer.id, e);
                    ready(ElectionResult::NotWinner)
                }
                Err(_) => {
                    error!("Vote timed out from peer {}", peer.id);
                    ready(ElectionResult::NotWinner)
                }
            });
            votes.push(vote);
        }

        // Calculate the majority, then tally up the votes as they come in.
        // If we end up with an outdated term, update the stored value and drop to being a follower.
        // If we don't win enough votes, remain a candidate.
        // If we win a majority of votes, promote to being the leader.
        let majority = calculate_majority(&self.peers);

        let storage = self.storage.clone();

        let tally = votes
            .fold(ready((1, None)), |fut, vote| async move {
                let (votes, newer_term) = fut.await;
                match vote {
                    ElectionResult::OutdatedTerm(t) => ready((votes, Some(t))),
                    ElectionResult::Winner => ready((votes + 1, newer_term)),
                    ElectionResult::NotWinner => ready((votes, newer_term)),
                }
            })
            .then(|f| async move {
                let (votes, newer_term) = f.await;
                if let Some(t) = newer_term {
                    {
                        let mut storage = storage.write().expect("poisoned lock");
                        storage
                            .set_current_term(t)
                            .unwrap_or_else(|e| error!("Could not set newer term {}: {}", t, e));
                        storage
                            .set_voted_for(None)
                            .unwrap_or_else(|e| error!("Could not clear vote: {}", e));
                    }
                    ProtocolState::Follower
                } else if votes >= majority {
                    ProtocolState::Leader
                } else {
                    ProtocolState::Candidate
                }
            });

        // Run the election in this thread
        futures::executor::block_on(tally)
    }

    fn init_election(&mut self) -> Result<RequestVoteRequest> {
        let mut storage = self.storage.write().expect("poisoned lock");

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

    fn run_leader(&mut self) -> ProtocolState {
        let (index_update_tx, index_update_rx) = std::sync::mpsc::channel();
        let mut match_index = HashMap::new();
        for peer in self.peers.iter() {
            match_index.insert(peer.id.clone(), 0);

            let hb = Heartbeater::new(
                self.id.clone(),
                peer.clone(),
                self.timeout,
                index_update_tx.clone(),
                self.storage.clone(),
                self.commit_index.clone(),
                self.rpc.clone(),
            );
            let peer_id = peer.id.clone();
            let log_updates_rx = self.new_logs_rx.clone();
            let term_updates_tx = self.term_updates_tx.clone();
            match hb {
                Ok(mut hb) => self.executor.spawn_ok(async move {
                    if let Some(newer_term) = hb.run(log_updates_rx).await {
                        term_updates_tx.send(newer_term).unwrap()
                    }
                }),
                Err(e) => {
                    error!("Failed to create heartbeat for peer {}: {}", peer_id, e);
                    return ProtocolState::Follower;
                }
            }
        }

        loop {
            if let Ok(newer_term) = self.term_updates_rx.try_recv() {
                let mut storage = self.storage.write().expect("poisoned lock");
                storage
                    .set_current_term(newer_term)
                    .unwrap_or_else(|e| error!("Failed to update term to {}: {}", newer_term, e));
                info!(
                    "Newer term {} discovered - converting to follower",
                    newer_term
                );
                return ProtocolState::Follower;
            }

            while let Ok((peer, index)) = index_update_rx.try_recv() {
                match_index.insert(peer.id, index);

                let updated = match_index.iter().filter(|(_, i)| **i >= index).count();
                if updated >= calculate_majority(&self.peers) {
                    let storage = self.storage.read().expect("poisoned lock");
                    match storage.current_term() {
                        Ok(current_term) => {
                            match storage.get_term(index) {
                                Ok(Some(term)) => {
                                    if current_term == term {
                                        let mut commit_index =
                                            self.commit_index.lock().expect("poisoned lock");
                                        *commit_index = index;
                                        if let Err(_) = self.commit_updates_tx.send(index) {
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
    storage: Arc<RwLock<S>>,
    commit_index: Arc<Mutex<usize>>,
    rpc: R,
}

impl<S: Storage, R: RPC> Heartbeater<S, R> {
    fn new(
        id: String,
        peer: Peer,
        timeout: Duration,
        match_index: Sender<(Peer, usize)>,
        storage: Arc<RwLock<S>>,
        commit_index: Arc<Mutex<usize>>,
        rpc: R,
    ) -> Result<Heartbeater<S, R>> {
        let heartbeat_interval = timeout.mul_f32(0.3);
        let append_entries_timeout = timeout.mul_f32(5.0);
        let next_index = { storage.read().expect("poisoned lock").last_index()? };
        Ok(Heartbeater {
            id,
            peer,
            heartbeat_interval,
            append_entries_timeout,
            next_index,
            match_index,
            storage,
            commit_index,
            rpc,
        })
    }

    async fn run(&mut self, mut updates: tokio::sync::watch::Receiver<usize>) -> Option<usize> {
        while let Some(max_index) = tokio::time::timeout(self.heartbeat_interval, updates.recv())
            .then(|res| match res {
                // Heartbeat:
                Err(_) => ready(Some(None)),
                // Updated index:
                Ok(Some(index)) => ready(Some(Some(index))),
                // Nothing:
                Ok(None) => ready(None),
            })
            .await
        {
            let (request, current_term, max_index_sent) = match self.create_request(max_index) {
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
                            if let Err(_) = self.match_index.send((self.peer.clone(), max_index)) {
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

    fn create_request(
        &mut self,
        max_index: Option<usize>,
    ) -> Result<(AppendEntriesRequest, usize, Option<usize>)> {
        let leader_commit = { *self.commit_index.lock().expect("poisoned lock") };
        let storage = self.storage.read().expect("poisoned lock");
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
