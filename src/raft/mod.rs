use futures::future::{join_all, ok};
use futures::prelude::*;
use std::error;
use std::fmt;
use std::result::Result;
use std::time::{Duration, Instant};

pub mod types;
pub use types::*;

pub trait Storage {
    type E: error::Error;

    fn current_term(&self) -> Result<Term, Self::E>;
    fn set_current_term(&mut self, t: Term) -> Result<(), Self::E>;

    fn voted_for(&self) -> Result<Option<ServerId>, Self::E>;
    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> Result<(), Self::E>;

    fn contains(&self, term: Term, index: LogIndex) -> Result<bool, Self::E>;
    fn append(&mut self, logs: Vec<LogEntry>) -> Result<(), Self::E>;

    fn last_log_entry(&self) -> Result<Option<(LogIndex, Term)>, Self::E>;
    fn get_entries_from(&self, index: LogIndex) -> Result<Vec<LogEntry>, Self::E>;
    fn get_term(&self, index: LogIndex) -> Result<Option<Term>, Self::E>;
}

pub trait RPC {
    fn append_entries(
        &mut self,
        to: &ServerId,
        req: AppendEntriesRequest,
    ) -> FutureAppendEntriesResponse;

    fn request_vote(&mut self, to: &ServerId, req: RequestVoteRequest)
        -> FutureRequestVoteResponse;
}

pub struct Raft<E> {
    id: ServerId,
    state: RaftState,
    storage: Box<dyn Storage<E = E> + Send>,
    commit_index: LogIndex,
    last_applied: LogIndex,
    known_peers: Vec<ServerId>,
    majority: usize,
}

enum RaftState {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

impl RaftState {
    fn needs_leader_election(&self) -> bool {
        match self {
            RaftState::Follower(ref f) => f.reached_timeout(),
            RaftState::Candidate(ref c) => c.reached_timeout(),
            RaftState::Leader(_) => false,
        }
    }

    fn election_timeout(&self) -> Duration {
        match self {
            RaftState::Follower(ref f) => f.election_timeout,
            RaftState::Candidate(ref c) => c.election_timeout,
            RaftState::Leader(ref l) => l.election_timeout,
        }
    }
}

impl fmt::Display for RaftState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RaftState::Follower(_) => write!(f, "Follower"),
            RaftState::Candidate(_) => write!(f, "Candidate"),
            RaftState::Leader(_) => write!(f, "Leader"),
        }
    }
}

struct Follower {
    last_leader_contact: Instant,
    election_timeout: Duration,
}

impl Follower {
    fn new() -> Follower {
        Follower {
            last_leader_contact: Instant::now(),
            election_timeout: Duration::from_millis(150), // TODO: Randomize
        }
    }

    fn leader_updated(&mut self) {
        self.last_leader_contact = Instant::now();
    }

    fn reached_timeout(&self) -> bool {
        self.last_leader_contact
            .elapsed()
            .checked_sub(self.election_timeout)
            .is_some()
    }
}

impl From<&mut Candidate> for Follower {
    fn from(c: &mut Candidate) -> Self {
        Follower {
            last_leader_contact: Instant::now(),
            election_timeout: c.election_timeout,
        }
    }
}

impl From<&mut Leader> for Follower {
    fn from(_l: &mut Leader) -> Self {
        Follower::new()
    }
}

struct Candidate {
    election_started: Instant,
    election_timeout: Duration,

    result: Option<bool>,
    future: Box<dyn Send + Future<Item = bool, Error = ()>>,
}

impl Candidate {
    fn new(election_timeout: Duration, votes: Vec<FutureRequestVoteResponse>) -> Candidate {
        let required = 1 + (votes.len() / 2);
        let results = votes.into_iter().map(|f| {
            f.map(|(_, x)| x.vote_granted)
                .or_else(|_| ok::<bool, ()>(false))
        });
        let result = join_all(results).and_then(move |v| {
            let votes = v.iter().filter(|x| **x).count();
            ok::<bool, ()>(votes >= required)
        });
        Candidate {
            election_started: Instant::now(),
            election_timeout,
            result: None,
            future: Box::new(result),
        }
    }
    fn reached_timeout(&self) -> bool {
        self.election_started
            .elapsed()
            .checked_sub(self.election_timeout)
            .is_some()
    }

    fn has_majority_votes(&mut self) -> bool {
        if self.result.is_none() {
            if let Ok(Async::Ready(ans)) = self.future.poll() {
                self.result = Some(ans);
            }
        }
        self.result.unwrap_or(false)
    }
}

impl From<&mut Follower> for Candidate {
    fn from(f: &mut Follower) -> Self {
        Candidate {
            election_started: Instant::now(),
            election_timeout: f.election_timeout,
            result: None,
            future: Box::new(ok::<bool, ()>(false)),
        }
    }
}

struct Leader {
    followers: Vec<RemoteServer>,
    election_timeout: Duration,
}

struct RemoteServer {
    id: ServerId,
    next_index: LogIndex,
    match_index: LogIndex,
    last_sent: Instant,
    last_append_entries: Option<FutureAppendEntriesResponse>,
}

impl Leader {
    fn next_index(&self, id: &ServerId) -> Option<LogIndex> {
        self.followers
            .iter()
            .find(|server| &server.id == id)
            .map(|x| x.next_index)
    }

    fn process_updates(&mut self) {
        let updated = Vec::new();
        while !self.followers.is_empty() {
            if let Some(mut server) = self.followers.pop() {
                if let Some(mut future) = server.last_append_entries {
                    server.last_append_entries = match future.poll() {
                        Ok(Async::Ready((req, resp))) => {
                            debug!(
                                "Append Entries request succeeded to {}: {:?}",
                                server.id, resp
                            );
                            if resp.success {
                                if let Some(entry) = req.entries.iter().max() {
                                    server.next_index = entry.index + 1;
                                    server.match_index = entry.index;
                                }
                            } else {
                                server.next_index -= 1;
                            }
                            None
                        }
                        Ok(Async::NotReady) => Some(future),
                        Err(_) => {
                            error!("Append Entries request failed to {}", server.id);
                            None
                        }
                    }
                }
            }
        }
        self.followers = updated;
    }

    fn followers_majority_commit(&self, majority: usize) -> LogIndex {
        let mut indices: Vec<LogIndex> = self
            .followers
            .iter()
            .map(|server| server.match_index)
            .collect();
        indices.sort();
        indices.reverse();
        for i in &indices {
            if indices.iter().filter(|&x| x >= i).count() >= majority {
                return *i;
            }
        }
        0
    }

    fn needs_heartbeat(&self, id: &ServerId) -> bool {
        self.followers
            .iter()
            .find(|server| &server.id == id)
            .map(|x| {
                x.last_append_entries.is_some()
                    && x.last_sent
                        .elapsed()
                        .checked_sub(self.election_timeout)
                        .is_some()
            })
            .unwrap_or(true)
    }

    fn update_follower(
        &mut self,
        id: &ServerId,
        next_index: LogIndex,
        request: FutureAppendEntriesResponse,
    ) {
        for server in &mut self.followers {
            if &server.id == id {
                server.last_append_entries = Some(request);
                server.last_sent = Instant::now();
                return;
            }
        }
        self.followers.push(RemoteServer {
            id: *id,
            next_index,
            match_index: 0,
            last_sent: Instant::now(),
            last_append_entries: Some(request),
        })
    }
}

impl From<&mut Candidate> for Leader {
    fn from(c: &mut Candidate) -> Self {
        Leader {
            followers: Vec::new(),
            election_timeout: c.election_timeout,
        }
    }
}

impl<E: error::Error> Raft<E> {
    pub fn new(
        id: ServerId,
        storage: Box<dyn Storage<E = E> + Send>,
        servers: Vec<ServerId>,
    ) -> Self {
        let majority = 1 + (servers.len() / 2);
        Raft {
            id,
            state: RaftState::Follower(Follower::new()),
            storage,
            commit_index: 0,
            last_applied: 0,
            known_peers: servers.into_iter().filter(|&x| x != id).collect(),
            majority,
        }
    }

    pub fn update<R: RPC>(&mut self, rpc: &mut R) {
        trace!("Updating server state {}", self.state);
        if let RaftState::Candidate(ref mut c) = self.state {
            if c.has_majority_votes() {
                self.state = RaftState::Leader(c.into());
            }
        }
        if let Err(e) = self.start_election(rpc) {
            error!("Failed to start next election: {}", e);
        }
        if let Err(e) = self.update_leader(rpc) {
            error!("Failed to send out heartbeats: {}", e);
        }
    }

    fn start_election<R: RPC>(&mut self, rpc: &mut R) -> Result<(), E> {
        if !self.state.needs_leader_election() {
            return Ok(());
        }
        let term = 1 + self.storage.current_term()?;
        self.storage.set_current_term(term)?;
        self.storage.set_voted_for(Some(self.id))?;
        let last_log_entry = self.storage.last_log_entry()?;
        let request = RequestVoteRequest {
            term,
            candidate_id: self.id,
            last_log_index: last_log_entry.map_or(0, |(i, _t)| i),
            last_log_term: last_log_entry.map_or(0, |(_i, t)| t),
        };
        info!("Starting leader election: {:?}", request);
        let votes = self
            .known_peers
            .iter()
            .map(|peer| rpc.request_vote(peer, request.clone()))
            .collect();
        self.state = RaftState::Candidate(Candidate::new(self.state.election_timeout(), votes));
        Ok(())
    }

    fn update_leader<R: RPC>(&mut self, rpc: &mut R) -> Result<(), E> {
        if let RaftState::Leader(ref mut leader) = self.state {
            // Update from previous events:
            leader.process_updates();

            let last_log_entry = self.storage.last_log_entry()?.unwrap_or((0, 0));
            let current_term = self.storage.current_term()?;

            // See if the current commit index can be updated:
            {
                let n = leader.followers_majority_commit(self.majority);
                if n > self.commit_index {
                    if let Some(n_term) = self.storage.get_term(n)? {
                        if n_term == current_term {
                            info!("Updating commit index to {}", n);
                            self.commit_index = n;
                        }
                    }
                }
            }

            // Send out new entries:
            for peer in &self.known_peers {
                if !leader.needs_heartbeat(peer) {
                    continue;
                }
                let next_index = leader.next_index(peer).unwrap_or(last_log_entry.0);
                let entries = self.storage.get_entries_from(next_index)?;
                if entries.is_empty() {
                    info!("Sending heartbeat to peer {}", peer);
                } else {
                    info!("Sending {} entries to peer {}", entries.len(), peer);
                }
                let heartbeat = AppendEntriesRequest {
                    term: current_term,
                    leader_id: self.id,
                    prev_log_index: next_index,
                    entries,
                    leader_commit: self.commit_index,
                };

                let resp = rpc.append_entries(peer, heartbeat);
                leader.update_follower(peer, next_index, resp);
            }
        };
        Ok(())
    }

    pub fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, E> {
        let current_term = self.storage.current_term()?;
        if req.term < current_term {
            return Ok(AppendEntriesResponse {
                term: current_term,
                success: false,
            });
        }
        match &mut self.state {
            RaftState::Follower(f) => {
                trace!(target: "raft_state", "Follower received heartbeat message");
                f.leader_updated();
            }
            RaftState::Candidate(c) => {
                debug!(target: "raft_state", "Candidate received message from leader; converting to follower");
                self.state = RaftState::Follower(c.into());
            }
            RaftState::Leader(l) => {
                debug!(target: "raft_state", "Discovered leader with higher term; converting to follower");
                self.state = RaftState::Follower(l.into());
            }
        }

        if req.prev_log_index > 0 && !self.storage.contains(req.term, req.prev_log_index)? {
            return Ok(AppendEntriesResponse {
                term: current_term,
                success: false,
            });
        }
        if req.leader_commit > self.commit_index {
            let updated = req
                .entries
                .iter()
                .max()
                .map(|entry| entry.index)
                .unwrap_or(self.commit_index);
            self.commit_index = req.leader_commit.min(updated);
        }
        Ok(AppendEntriesResponse {
            term: current_term,
            success: true,
        })
    }

    pub fn request_vote(&mut self, req: RequestVoteRequest) -> Result<RequestVoteResponse, E> {
        trace!("Received vote request {:?}", req);
        let current_term = self.storage.current_term()?;
        if req.term < current_term {
            return Ok(RequestVoteResponse {
                term: current_term,
                vote_granted: false,
            });
        }
        if let Some(id) = self.storage.voted_for()? {
            if id != req.candidate_id {
                debug!(
                    "Already voted for {}, will not vote for {} in term {}",
                    id, req.candidate_id, req.term
                );
                return Ok(RequestVoteResponse {
                    term: current_term,
                    vote_granted: false,
                });
            }
        }
        let (last_index, last_term) = self.storage.last_log_entry()?.unwrap_or((0, 0));

        if (last_term > req.last_log_term)
            || (last_term == req.last_log_term && last_index > req.last_log_index)
        {
            debug!(
                "We have newer logs than the candidate, denying vote for term {}",
                req.term
            );
            return Ok(RequestVoteResponse {
                term: current_term,
                vote_granted: false,
            });
        }
        debug!(
            "Voting for server {} in term {}",
            req.candidate_id, req.term
        );
        Ok(RequestVoteResponse {
            term: current_term,
            vote_granted: false,
        })
    }

    pub fn status(&self) -> Result<Status, E> {
        let last_entry = self.storage.last_log_entry()?;
        let status = Status {
            leader: self.storage.voted_for()?,
            peers: self.known_peers.clone(),
            last_log_term: last_entry.map(|x| x.1),
            last_log_index: last_entry.map(|x| x.0),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
        };
        debug!("Responding to status with {:?}", status);
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::*;

    fn follower(term: Term, commit_index: LogIndex) -> Raft<Error> {
        let mut s = InMemoryStorage::new();
        s.set_current_term(term).unwrap();
        let addr = ([127, 0, 0, 1], 8080).into();
        let mut r = Raft::new(addr, Box::new(s), vec![addr]);
        r.commit_index = commit_index;
        r
    }

    fn heartbeat(
        term: Term,
        prev_log_index: LogIndex,
        leader_commit: LogIndex,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: ([127, 0, 0, 1], 8080).into(),
            prev_log_index,
            entries: Vec::new(),
            leader_commit,
        }
    }

    #[test]
    fn test_follower_append_entries_heartbeat() {
        // Term is earlier, should fail
        let mut f = follower(3, 0);
        let resp = f.append_entries(heartbeat(2, 0, 0));
        assert!(
            !resp.unwrap().success,
            "Reply should be false if term < current_term"
        );
        assert_eq!(0, f.commit_index, "Commit index should be unchanged");

        // Term is the same, should fail when index is too new
        let mut f = follower(3, 0);
        let resp = f.append_entries(heartbeat(3, 1, 0));
        assert!(
            !resp.unwrap().success,
            "Reply should be false when entries are missing"
        );
        assert_eq!(0, f.commit_index, "Commit index should be unchanged");

        // Term is the same, should succeed when index matches
        let mut f = follower(3, 0);
        let resp = f.append_entries(heartbeat(3, 0, 0));
        assert!(
            resp.unwrap().success,
            "Reply should be true if term = current_term"
        );
        assert_eq!(0, f.commit_index, "Commit index should be unchanged");

        // Term is newer, should fail when index is too new
        let mut f = follower(3, 0);
        let resp = f.append_entries(heartbeat(4, 1, 0));
        assert!(
            !resp.unwrap().success,
            "Reply should be false when entries are missing"
        );
        assert_eq!(0, f.commit_index, "Commit index should be unchanged");

        // Term is newer, should succeed when index matches
        let mut f = follower(3, 0);
        let resp = f.append_entries(heartbeat(4, 0, 0));
        assert!(
            resp.unwrap().success,
            "Reply should be true if term > current_term"
        );
        assert_eq!(0, f.commit_index, "Commit index should be unchanged");
    }
}
