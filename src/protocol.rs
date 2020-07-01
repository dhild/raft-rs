use crate::rpc::{AppendEntriesRequest, RaftServer, RequestVoteRequest, RPC};
use crate::state::{Command, StateMachine, StateMachineApplier};
use crate::storage::{LogCommand, LogEntry, Storage};
use async_channel::{Receiver, Sender, TrySendError};
use async_lock::Lock;
use futures::executor::ThreadPool;
use futures::prelude::*;
use futures::select;
use log::{debug, error, info, trace};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone)]
pub struct Peer {
    id: String,
    address: String,
    voting: bool,
}

impl Peer {
    pub fn voting(id: String, address: String) -> Peer {
        Peer {
            id,
            address,
            voting: true,
        }
    }
}

pub fn start<S, R, M>(
    id: String,
    peers: Vec<Peer>,
    timeout: Duration,
    storage: S,
    rpc: R,
) -> Result<M, std::io::Error>
where
    S: Storage,
    R: RPC,
    M: StateMachine<S>,
{
    let storage = Lock::new(storage);
    let (commits_protocol_tx, commits_to_apply_rx) = async_channel::bounded(1);
    let (commits_tx, commits_protocol_rx) = async_channel::bounded(1);
    let (last_applied_tx, last_applied_rx) = async_channel::bounded(1);
    let (term_updates_tx, term_updates_rx) = async_channel::bounded(1);
    let (new_logs_tx, new_logs_rx) = async_channel::bounded(1);

    let executor = ThreadPool::new()?;

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
        executor.clone(),
    );

    let consensus = Consensus {
        storage: storage.clone(),
        current_state: tasks.current_state.clone(),
        new_commands_tx: new_logs_tx,
        last_applied_rx,
    };

    let (state_machine, applier) = M::build(consensus);

    executor.spawn_ok(async move {
        let mut lc = LogCommitter::new(applier, commits_to_apply_rx, last_applied_tx);
        lc.run().await
    });
    executor.spawn_ok(async move { tasks.run().await });

    let raft_server = RaftServer::new(storage.clone(), term_updates_tx, commits_tx);

    Ok(state_machine)
}

pub struct Consensus<S: Storage> {
    storage: Lock<S>,
    current_state: Lock<ProtocolState>,
    new_commands_tx: Sender<usize>,
    last_applied_rx: Receiver<usize>,
}

#[derive(Debug)]
pub enum ConsensusError {
    NotLeader,
    RaftProtocolTerminated,
    SerializationError(Box<dyn std::error::Error + Send + Sync>),
    StorageError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusError::NotLeader => write!(f, "This raft instance is not the leader"),
            ConsensusError::RaftProtocolTerminated => {
                write!(f, "The raft protocol has been terminated")
            }
            ConsensusError::SerializationError(e) => {
                write!(f, "Error while serializing command: {}", e)
            }
            ConsensusError::StorageError(e) => {
                write!(f, "Error while interacting with stable storage: {}", e)
            }
        }
    }
}

impl std::error::Error for ConsensusError {}

impl<S: Storage> Consensus<S> {
    pub async fn commit<C: Command>(&mut self, cmd: C) -> Result<usize, ConsensusError> {
        let index = self.send(cmd).await?;

        while let Ok(last_applied) = self.last_applied_rx.recv().await {
            if last_applied >= index {
                return Ok(index);
            }
        }
        Err(ConsensusError::RaftProtocolTerminated)
    }

    pub async fn send<C: Command>(&mut self, cmd: C) -> Result<usize, ConsensusError> {
        let current_state = self.current_state.lock().await;
        if let ProtocolState::Leader = *current_state {
            return Err(ConsensusError::NotLeader);
        }
        let cmd = cmd
            .serialize()
            .map_err(|e| ConsensusError::SerializationError(Box::new(e)))?;
        let command = LogCommand::Command(cmd);

        let index = {
            let mut storage = self.storage.lock().await;

            let term = storage
                .current_term()
                .map_err(|e| ConsensusError::StorageError(Box::new(e)))?;

            storage
                .append_entry(term, command)
                .map_err(|e| ConsensusError::StorageError(Box::new(e)))?
        };

        self.new_commands_tx
            .send(index)
            .await
            .map_err(|_| ConsensusError::RaftProtocolTerminated)?;

        Ok(index)
    }
}

#[derive(Clone, Copy)]
enum ProtocolState {
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

pub struct LogCommitter<SM: StateMachineApplier> {
    commits: Receiver<(usize, LogCommand)>,
    state_machine: SM,
    last_applied_tx: Sender<usize>,
}

impl<SM: StateMachineApplier> LogCommitter<SM> {
    fn new(
        state_machine: SM,
        commits: Receiver<(usize, LogCommand)>,
        last_applied_tx: Sender<usize>,
    ) -> LogCommitter<SM> {
        LogCommitter {
            commits,
            state_machine,
            last_applied_tx,
        }
    }

    async fn run(&mut self) {
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
                LogCommand::Command(ref data) => {
                    let cmd = match SM::Command::deserialize(data) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            error!(
                                "Could not deserialize command at index {}: {}",
                                commit_index, e
                            );
                            return; // Triggers a graceful shutdown
                        }
                    };
                    self.state_machine.apply(commit_index, cmd).await
                }
                LogCommand::Noop => {}
            }
            if let Err(_) = self.last_applied_tx.send(commit_index).await {
                trace!("consensus struct has been dropped; closing log committer");
                return;
            }
        }
    }
}

#[derive(Debug)]
pub enum ProtocolError {
    NotLeader,
    RaftProtocolTerminated,
    StorageError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::NotLeader => write!(f, "This raft instance is not the leader"),
            ProtocolError::RaftProtocolTerminated => {
                write!(f, "The raft protocol has been terminated")
            }
            ProtocolError::StorageError(e) => {
                write!(f, "Error while interacting with stable storage: {}", e)
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

pub struct ProtocolTasks<S: Storage, R: RPC> {
    id: String,
    timeout: Duration,
    peers: Vec<Peer>,
    storage: Lock<S>,
    rpc: R,
    commit_index: Lock<usize>,
    current_state: Lock<ProtocolState>,
    term_updates_tx: Sender<usize>,
    term_updates_rx: Receiver<usize>,
    append_entries_rx: Receiver<usize>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
    new_logs_rx: Receiver<usize>,
    executor: ThreadPool,
}

impl<S: Storage, R: RPC> ProtocolTasks<S, R> {
    fn start(
        id: String,
        peers: Vec<Peer>,
        timeout: Duration,
        storage: Lock<S>,
        rpc: R,
        term_updates_tx: Sender<usize>,
        term_updates_rx: Receiver<usize>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        append_entries_rx: Receiver<usize>,
        new_logs_rx: Receiver<usize>,
        executor: ThreadPool,
    ) -> ProtocolTasks<S, R> {
        let current_state = Lock::new(ProtocolState::Follower);
        let commit_index = Lock::new(0);

        ProtocolTasks {
            id,
            peers,
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
            executor,
        }
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

        let timeout = self.timeout.mul_f32(1.0 + rand::thread_rng().gen::<f32>());

        loop {
            select! {
                // Consume the term updates; sender is responsible for updating the storage:
                _ = self.term_updates_rx.recv().fuse() => continue,
                // Actually process commits as they come in
                state = self.append_entries_rx.recv().fuse() => match state {
                    Ok(new_index) => {
                        let storage = self.storage.lock().await;
                        let mut commit_index = self.commit_index.lock().await;
                        while new_index > *commit_index {
                            let cmd = storage.get_command(*commit_index).unwrap();
                            if let Err(_) = self.commits_to_apply_tx.send((*commit_index, cmd)).await {
                                error!("State machine processing disconnected; shutting down");
                                return ProtocolState::Shutdown;
                            }
                            *commit_index += 1;
                        }
                    }
                    // Server hung up, no more RPCs:
                    Err(_) => return ProtocolState::Shutdown,
                },
                _ = crate::time::delay_for(timeout).fuse() => {
                    debug!("Timeout; transitioning to Candidate");
                    return ProtocolState::Candidate
                }
            }
        }
    }

    async fn run_candidate(&mut self) -> ProtocolState {
        use futures::stream::StreamExt;

        let request = match self.init_election().await {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to initialize election: {}", e);
                return ProtocolState::Follower;
            }
        };
        // Send out the RequestVote RPC calls.
        let (votes_tx, votes_rx) = async_channel::bounded(self.peers.len());
        for peer in self.peers.clone() {
            let request = request.clone();
            let votes_tx = votes_tx.clone();
            let current_term = request.term;
            let timeout = self.timeout;
            let rpc = self.rpc.clone();
            // Use a timeout on each of them, so that we will
            // have an election result from each within the election timeout.
            let vote = async move {
                let vote = rpc.request_vote(peer.address.clone(), request);
                match crate::time::timeout(timeout, vote).await {
                    Some(Ok(rv)) => {
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
                    Some(Err(e)) => {
                        error!("Error received from peer {}: {}", peer.id, e);
                        ElectionResult::NotWinner
                    }
                    None => {
                        error!("Vote timed out from peer {}", peer.id);
                        ElectionResult::NotWinner
                    }
                }
            }
            .then(|vote| async move {
                if let Err(_) = votes_tx.send(vote).await {
                    debug!("Vote receiver has hung up");
                }
            });
            self.executor.spawn_ok(vote);
        }

        // Calculate the majority, then tally up the votes as they come in.
        // If we end up with an outdated term, update the stored value and drop to being a follower.
        // If we don't win enough votes, remain a candidate.
        // If we win a majority of votes, promote to being the leader.
        let majority = calculate_majority(&self.peers);

        let (votes, newer_term) = votes_rx
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

    async fn init_election(&mut self) -> Result<RequestVoteRequest, ProtocolError> {
        let mut storage = self.storage.lock().await;

        // Increment the current term, and vote for ourselves:
        let current_term = storage
            .current_term()
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?
            + 1;
        storage
            .set_current_term(current_term)
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?;
        storage
            .set_voted_for(Some(self.id.clone()))
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?;

        let last_log_term = storage
            .last_term()
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?;
        let last_log_index = storage
            .last_index()
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?;

        Ok(RequestVoteRequest {
            term: current_term,
            candidate_id: self.id.clone(),
            last_log_term,
            last_log_index,
        })
    }

    async fn run_leader(&mut self) -> ProtocolState {
        let (index_update_tx, index_update_rx) = async_channel::bounded(1);
        let mut match_index = HashMap::new();
        let mut hb_updates = Vec::new();
        let last_index = match self.storage.lock().await.last_index() {
            Ok(i) => i,
            Err(e) => {
                error!("Failed to load last log index from stable storage: {}", e);
                return ProtocolState::Follower;
            }
        };
        for peer in self.peers.iter() {
            match_index.insert(peer.id.clone(), 0);

            // We make sure to periodically consume all elements from the heartbeat queue
            // so this does have a practical bound on the size. What we don't want is for one
            // heartbeater to block the whole setup. However, slow peers may not receive updates
            // at a slower pace because of this queue size.
            let (hb_tx, hb_rx) = async_channel::bounded(10);
            hb_updates.push(hb_tx);

            let mut hb = Heartbeater::new(
                self.id.clone(),
                peer.clone(),
                self.timeout,
                index_update_tx.clone(),
                self.storage.clone(),
                self.commit_index.clone(),
                hb_rx,
                self.term_updates_tx.clone(),
                self.rpc.clone(),
                last_index + 1,
            );
            self.executor.spawn_ok(async move { hb.run().await });
        }

        loop {
            select! {
                _ = self.term_updates_rx.recv().fuse() => {
                    info!("Newer term discovered - converting to follower");
                    return ProtocolState::Follower;
                }
                log = self.new_logs_rx.recv().fuse() => {
                    if let Ok(index) = log {
                        hb_updates.retain(|hb| match hb.try_send(index) {
                            Ok(()) => true,
                            Err(TrySendError::Full(_)) => true,
                            Err(TrySendError::Closed(_)) => false,
                        });
                    } else {
                        return ProtocolState::Shutdown;
                    }
                }
                res = index_update_rx.recv().fuse() => {
                    let (peer, index) = match res {
                        Ok((peer, index)) => (peer, index),
                        Err(_) => {
                            error!("We are no longer receiving updates from followers - converting to follower");
                            return ProtocolState::Follower;
                        }
                    };
                    match_index.insert(peer.id, index);

                    match self.leader_update_commit_index(&match_index, index).await {
                        Ok(Some(state)) => return state,
                        Ok(None) => {},
                        Err(e) => {
                            error!("Failed to update current commit index: {}", e);
                        }
                    }
                }
            }
        }
    }

    async fn leader_update_commit_index(
        &mut self,
        match_index: &HashMap<String, usize>,
        index: usize,
    ) -> Result<Option<ProtocolState>, ProtocolError> {
        let updated = match_index.iter().filter(|(_, i)| **i >= index).count();
        if updated < calculate_majority(&self.peers) {
            return Ok(None);
        }
        let storage = self.storage.lock().await;
        let current_term = storage
            .current_term()
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?;
        match storage
            .get_term(index)
            .map_err(|e| ProtocolError::StorageError(Box::new(e)))?
        {
            Some(term) => {
                if current_term == term {
                    let cmd = storage
                        .get_command(index)
                        .map_err(|e| ProtocolError::StorageError(Box::new(e)))?;
                    let mut commit_index = self.commit_index.lock().await;
                    *commit_index = index;
                    if let Err(_) = self.commits_to_apply_tx.send((index, cmd)).await {
                        error!("State machine processing disconnected; shutting down");
                        return Ok(Some(ProtocolState::Shutdown));
                    }
                }
            }
            None => {
                error!(
                    "Failed to load expected log index {} while processing commit index",
                    index
                );
                return Ok(Some(ProtocolState::Follower));
            }
        }
        Ok(None)
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
    storage: Lock<S>,
    commit_index: Lock<usize>,
    new_logs_rx: Receiver<usize>,
    term_updates_tx: Sender<usize>,
    rpc: R,
}

impl<S: Storage, R: RPC> Heartbeater<S, R> {
    fn new(
        id: String,
        peer: Peer,
        timeout: Duration,
        match_index: Sender<(Peer, usize)>,
        storage: Lock<S>,
        commit_index: Lock<usize>,
        new_logs_rx: Receiver<usize>,
        term_updates_tx: Sender<usize>,
        rpc: R,
        next_index: usize,
    ) -> Heartbeater<S, R> {
        let heartbeat_interval = timeout.mul_f32(0.3);
        let append_entries_timeout = timeout.mul_f32(5.0);
        Heartbeater {
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
        }
    }

    async fn run(&mut self) {
        loop {
            let mut max_index = None;
            // Consume any built-up updates:
            while let Ok(log) = self.new_logs_rx.try_recv() {
                max_index = Some(log);
            }
            if max_index.is_none() {
                max_index = select! {
                    _ = crate::time::delay_for(self.heartbeat_interval).fuse() => None,
                    log = self.new_logs_rx.recv().fuse() => match log {
                        Ok(log) => Some(log),
                        // If the sender hung up, we're no longer leader.
                        Err(_) => return,
                    },
                };
            }

            // Create the request data
            let (request, current_term, max_index_sent) = match self.create_request(max_index).await
            {
                Ok(req) => req,
                Err(e) => {
                    error!("Failed to get data for AppendEntries request: {}", e);
                    continue;
                }
            };
            // Send the request
            let response = self.rpc.append_entries(self.peer.address.clone(), request);
            // Only wait for so long before we consider it a failure:
            let response = crate::time::timeout(self.append_entries_timeout, response);

            match response.await {
                Some(Ok(resp)) => {
                    if resp.term > current_term {
                        info!("Peer {} is on a newer term {}", self.peer.id, resp.term);
                        let mut storage = self.storage.lock().await;
                        storage.set_current_term(resp.term).unwrap();
                        if self.term_updates_tx.send(resp.term).await.is_err() {
                            // service is shutting down
                        }
                        return;
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
                            if self
                                .match_index
                                .send((self.peer.clone(), max_index))
                                .await
                                .is_err()
                            {
                                // The leader is no longer listening for updates, we must be in a different state now
                                return;
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    error!(
                        "Error received from AppendEntries request to peer {}: {}",
                        self.peer.id, e
                    );
                }
                None => {
                    error!("AppendEntries request timed out to peer {}", self.peer.id);
                }
            }
        }
    }

    async fn create_request(
        &mut self,
        max_index: Option<usize>,
    ) -> Result<(AppendEntriesRequest, usize, Option<usize>), S::Error> {
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
