use crate::rpc::{AppendEntriesRequest, RPCBuilder, RaftServer, RequestVoteRequest, RPC};
use crate::state::{Command, StateMachine, StateMachineApplier};
use crate::storage::{LogCommand, LogEntry, Storage};
use async_channel::{Receiver, Sender, TryRecvError, TrySendError};
use async_lock::Lock;
use log::{debug, error, info, trace};
use rand::Rng;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::time::Duration;
use tokio::select;

#[cfg(test)]
mod tests;

#[derive(Clone)]
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

pub async fn start<R, S, M>(
    id: String,
    peers: Vec<Peer>,
    timeout: Duration,
    rpc_config: R,
    storage: S,
) -> Result<M, std::io::Error>
where
    R: RPCBuilder,
    S: Storage,
    M: StateMachine<S>,
{
    let storage = Lock::new(storage);
    let (commits_to_apply_tx, commits_to_apply_rx) = async_channel::bounded(1);
    let (append_entries_tx, append_entries_rx) = async_channel::bounded(1);
    let (last_applied_tx, last_applied_rx) = async_channel::bounded(1);
    let (term_updates_tx, term_updates_rx) = async_channel::bounded(1);
    let (new_logs_tx, new_logs_rx) = async_channel::bounded(1);

    let raft_server = RaftServer::new(storage.clone(), term_updates_tx.clone(), append_entries_tx);

    let rpc = rpc_config.build(raft_server)?;

    let mut tasks = ProtocolTasks::new(
        id,
        peers,
        timeout,
        storage.clone(),
        rpc,
        term_updates_tx,
        term_updates_rx,
        commits_to_apply_tx,
        append_entries_rx,
        new_logs_rx,
    );

    let consensus = Consensus {
        storage: storage.clone(),
        current_state: tasks.current_state.clone(),
        new_commands_tx: new_logs_tx,
        last_applied_rx,
    };

    let (state_machine, applier) = M::build(consensus);

    tokio::spawn(async move {
        let mut lc = LogCommitter::new(applier, commits_to_apply_rx, last_applied_tx);
        lc.run().await
    });
    tokio::spawn(async move { tasks.run().await });

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProtocolState {
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
    StorageError(std::io::Error),
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
    term_updates_tx: Sender<()>,
    term_updates_rx: Receiver<()>,
    append_entries_rx: Receiver<usize>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
    new_logs_rx: Receiver<usize>,
}

impl<S: Storage, R: RPC> ProtocolTasks<S, R> {
    fn new(
        id: String,
        peers: Vec<Peer>,
        timeout: Duration,
        storage: Lock<S>,
        rpc: R,
        term_updates_tx: Sender<()>,
        term_updates_rx: Receiver<()>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        append_entries_rx: Receiver<usize>,
        new_logs_rx: Receiver<usize>,
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
            self.timeout,
            self.peers.clone(),
            self.storage.clone(),
            self.rpc.clone(),
            self.term_updates_rx.clone(),
        );
        candidate.run().await
    }

    async fn run_leader(&mut self) -> ProtocolState {
        let mut leader = match Leader::start(
            self.id.clone(),
            self.timeout,
            self.peers.clone(),
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

pub struct Follower<S: Storage> {
    timeout: Duration,
    storage: Lock<S>,
    commit_index: Lock<usize>,
    term_updates_rx: Receiver<()>,
    append_entries_rx: Receiver<usize>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
}

impl<S: Storage> Follower<S> {
    pub fn new(
        timeout: Duration,
        storage: Lock<S>,
        commit_index: Lock<usize>,
        term_updates_rx: Receiver<()>,
        append_entries_rx: Receiver<usize>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
    ) -> Follower<S> {
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
                ProtocolState::Follower => {}
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
            if let Err(_) = self.commits_to_apply_tx.send((*commit_index, cmd)).await {
                debug!("State machine processing has been disconnected; shutting down");
                return ProtocolState::Shutdown;
            }
        }
        ProtocolState::Follower
    }
}

pub struct Candidate<S: Storage, R: RPC> {
    id: String,
    timeout: Duration,
    peers: Vec<Peer>,
    storage: Lock<S>,
    rpc: R,
    term_updates_rx: Receiver<()>,
}

impl<S: Storage, R: RPC> Candidate<S, R> {
    pub fn new(
        id: String,
        timeout: Duration,
        peers: Vec<Peer>,
        storage: Lock<S>,
        rpc: R,
        term_updates_rx: Receiver<()>,
    ) -> Candidate<S, R> {
        let timeout = timeout.mul_f32(1. + 2. * rand::thread_rng().gen::<f32>());
        Candidate {
            id,
            timeout,
            peers,
            storage,
            rpc,
            term_updates_rx,
        }
    }

    pub async fn run(&self) -> ProtocolState {
        loop {
            let election_timeout = tokio::time::delay_for(self.timeout);
            match self.run_once().await {
                ProtocolState::Candidate => {}
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
        let (votes_tx, votes_rx) = async_channel::bounded(self.peers.len());
        for peer in self.peers.clone() {
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
                if let Err(_) = votes_tx.send(vote).await {
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
        let majority = match self.voting_majority() {
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

    fn voting_majority(&self) -> Option<usize> {
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

enum ElectionResult {
    Winner,
    NotWinner,
    OutdatedTerm(usize),
}

pub struct Leader<S: Storage> {
    peers: Vec<Peer>,
    storage: Lock<S>,
    commit_index: Lock<usize>,
    term_updates_rx: Receiver<()>,
    commits_to_apply_tx: Sender<(usize, LogCommand)>,
    new_logs_rx: Receiver<usize>,
    index_update_rx: Receiver<(String, usize)>,
    match_index: HashMap<String, usize>,
    heartbeat_updates: Vec<Sender<usize>>,
}

impl<S: Storage> Leader<S> {
    pub async fn start<R: RPC>(
        id: String,
        timeout: Duration,
        peers: Vec<Peer>,
        storage: Lock<S>,
        rpc: R,
        commit_index: Lock<usize>,
        term_updates_tx: Sender<()>,
        term_updates_rx: Receiver<()>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        new_logs_rx: Receiver<usize>,
    ) -> std::io::Result<Leader<S>> {
        let timeout = timeout.mul_f32(0.5);
        let (index_update_tx, index_update_rx) = async_channel::bounded(peers.len());
        let mut heartbeat_updates = Vec::new();
        let next_index = { storage.lock().await.last_index()? + 1 };
        for peer in peers.iter() {
            // We make sure to periodically consume all elements from the heartbeat queue
            // so this does have a practical bound on the size. What we don't want is for one
            // heartbeater to block the whole setup. Slow acting peers may receive multiple updates
            // at a time.
            let (hb_tx, hb_rx) = async_channel::bounded(10);
            heartbeat_updates.push(hb_tx);

            let mut hb = Heartbeater::new(
                id.clone(),
                peer.clone(),
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
            peers,
            storage,
            commit_index,
            term_updates_rx,
            commits_to_apply_tx,
            new_logs_rx,
            index_update_rx,
            heartbeat_updates,
        ))
    }

    pub fn new(
        peers: Vec<Peer>,
        storage: Lock<S>,
        commit_index: Lock<usize>,
        term_updates_rx: Receiver<()>,
        commits_to_apply_tx: Sender<(usize, LogCommand)>,
        new_logs_rx: Receiver<usize>,
        index_update_rx: Receiver<(String, usize)>,
        heartbeat_updates: Vec<Sender<usize>>,
    ) -> Leader<S> {
        Leader {
            peers,
            storage,
            commit_index,
            term_updates_rx,
            commits_to_apply_tx,
            new_logs_rx,
            index_update_rx,
            match_index: HashMap::new(),
            heartbeat_updates,
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
        self.heartbeat_updates
            .retain(|hb| match hb.try_send(index) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Closed(_)) => false,
            });
    }

    async fn process_index_update(&mut self, peer_id: String, index: usize) -> ProtocolState {
        self.match_index.insert(peer_id, index);

        match self.update_commit_index(index).await {
            Ok(state) => return state,
            Err(e) => {
                error!("Failed to update current commit index: {}", e);
                ProtocolState::Follower
            }
        }
    }

    async fn update_commit_index(&self, index: usize) -> std::io::Result<ProtocolState> {
        // Find the number of voting peers that have this new index value or greater.
        let updated = self
            .match_index
            .iter()
            .filter(|(_, i)| **i >= index)
            .filter(|(peer_id, _)| self.peers.iter().any(|p| &p.id == *peer_id && p.voting))
            .count();

        // See if there are enough with the log persisted to consider it committed
        if self
            .commit_majority()
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
                // As long as we are in the current term, apply the log
                if current_term == term {
                    let cmd = storage.get_command(index)?;
                    let mut commit_index = self.commit_index.lock().await;
                    debug!(
                        "Increasing commit index to {} from {}",
                        index, *commit_index
                    );
                    *commit_index = index;
                    if let Err(_) = self.commits_to_apply_tx.send((index, cmd)).await {
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

    fn commit_majority(&self) -> Option<usize> {
        // Count the number of voting peers:
        match self
            .peers
            .iter()
            .filter(|peer| self.peers.iter().any(|p| p.id == peer.id && p.voting))
            .count()
        {
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

pub struct Heartbeater<S: Storage, R: RPC> {
    id: String,
    peer: Peer,
    heartbeat_interval: Duration,
    append_entries_timeout: Duration,
    next_index: usize,
    match_index_tx: Sender<(String, usize)>,
    storage: Lock<S>,
    commit_index: Lock<usize>,
    new_logs_rx: Receiver<usize>,
    term_updates_tx: Sender<()>,
    rpc: R,
}

impl<S: Storage, R: RPC> Heartbeater<S, R> {
    pub fn new(
        id: String,
        peer: Peer,
        timeout: Duration,
        match_index_tx: Sender<(String, usize)>,
        storage: Lock<S>,
        commit_index: Lock<usize>,
        new_logs_rx: Receiver<usize>,
        term_updates_tx: Sender<()>,
        rpc: R,
        next_index: usize,
    ) -> Heartbeater<S, R> {
        // Raft is able to elect and maintain a steady leader as long as the system satisfies the
        // timing requirement
        //     broadcastTime << electionTimeout << MTBF
        // The broadcast time should be an order of magnitude less than the election timeout.
        let heartbeat_interval = timeout.mul_f32(0.1);
        // We should not wait indefinitely for an RPC call to complete; we may encounter a slow
        // follower, which needs more time to process the request. For this reason, wait an order
        // of magnitude longer than the election timeout.
        let append_entries_timeout = timeout.mul_f32(10.0);
        Heartbeater {
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

    async fn run(&mut self) {
        loop {
            let mut max_index = None;
            // Consume any built-up updates:
            while let Ok(log) = self.new_logs_rx.try_recv() {
                max_index = Some(log);
            }
            if max_index.is_none() {
                let delay = tokio::time::delay_for(self.heartbeat_interval);
                max_index = select! {
                    _ = delay => None,
                    log = self.new_logs_rx.recv() => match log {
                        Ok(log) => Some(log),
                        // If the sender hung up, we're no longer leader, and should not send heartbeats.
                        Err(_) => return,
                    },
                };
            }

            // Create the request data
            let (request, current_term, max_index_sent) = match self.build_request(max_index).await
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
            match tokio::time::timeout(self.append_entries_timeout, response).await {
                Ok(Ok(resp)) => {
                    if resp.term > current_term {
                        info!("Peer {} is on a newer term {}", self.peer.id, resp.term);
                        let mut storage = self.storage.lock().await;
                        storage.set_current_term(resp.term).unwrap();
                        storage.set_voted_for(None).unwrap();
                        // Send the term update; if the send fails then the protocol has shutdown.
                        let _ = self.term_updates_tx.send(()).await;
                        return;
                    } else if !resp.success {
                        debug!(
                            "Peer {} could not process log index {}",
                            self.peer.id, self.next_index
                        );
                        if self.next_index > 1 {
                            self.next_index -= 1;
                        }
                    } else {
                        trace!("Peer {} processed AppendEntries successfully", self.peer.id);
                        if let Some(max_index) = max_index_sent {
                            self.next_index = max_index + 1;
                            if self
                                .match_index_tx
                                .send((self.peer.id.clone(), max_index))
                                .await
                                .is_err()
                            {
                                // The leader is no longer listening for index updates.
                                // We must be in a different state now
                                return;
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        "Peer {} responded with error to AppendEntries: {}",
                        self.peer.id, e
                    );
                }
                Err(_) => {
                    error!("Peer {} timed out on AppendEntries", self.peer.id);
                }
            }
        }
    }

    async fn build_request(
        &mut self,
        max_index: Option<usize>,
    ) -> std::io::Result<(AppendEntriesRequest, usize, Option<usize>)> {
        let leader_commit = { *self.commit_index.lock().await };
        let storage = self.storage.lock().await;
        let term = storage.current_term()?;
        let prev_log_index = self.next_index - 1;
        let prev_log_term = storage
            .get_term(prev_log_index)?
            .expect("log is missing entries!");

        let mut entries = Vec::new();
        if let Some(index) = max_index {
            // Send up to 100 entries at a time
            for index in (self.next_index..(index + 1)).take(100) {
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
