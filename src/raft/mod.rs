use futures::prelude::*;
use std::error;
use std::result::Result;
use std::time::Instant;

mod types;
pub use types::*;

pub trait Storage {
    type E: error::Error;

    fn current_term(&self) -> Result<Term, Self::E>;
    fn set_current_term(&mut self, t: Term) -> Result<(), Self::E>;

    fn voted_for(&self) -> Result<Option<ServerId>, Self::E>;
    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> Result<(), Self::E>;

    fn contains(&self, term: Term, index: LogIndex) -> Result<bool, Self::E>;
    fn append(&mut self, logs: Vec<LogEntry>) -> Result<(), Self::E>;
}

pub trait RPC {
    type E: error::Error;

    fn append_entries(
        &mut self,
        server: &ServerId,
        req: AppendEntriesRequest,
    ) -> Box<dyn Future<Item = AppendEntriesResponse, Error = Self::E>>;

    fn request_vote(
        &mut self,
        server: &ServerId,
        req: RequestVoteRequest,
    ) -> Box<dyn Future<Item = RequestVoteResponse, Error = Self::E>>;
}

pub struct Raft<E> {
    id: ServerId,
    state: RaftState,
    storage: Box<dyn Storage<E = E> + Send>,
    commit_index: LogIndex,
    last_applied: LogIndex,
}

enum RaftState {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

struct Follower {
    last_leader_contact: Instant,
}

impl Follower {
    fn new() -> Follower {
        Follower {
            last_leader_contact: Instant::now(),
        }
    }

    fn leader_updated(&mut self) {
        self.last_leader_contact = Instant::now();
    }
}

impl From<&mut Candidate> for Follower {
    fn from(_c: &mut Candidate) -> Self {
        Follower::new()
    }
}

impl From<&mut Leader> for Follower {
    fn from(_l: &mut Leader) -> Self {
        Follower::new()
    }
}

struct Candidate {
    election_started: Instant,
}
struct Leader {
    next_index: Vec<(ServerId, LogIndex)>,
    match_index: Vec<(ServerId, LogIndex)>,
}

impl<E: error::Error> Raft<E> {
    pub fn new(id: ServerId, storage: Box<dyn Storage<E = E> + Send>) -> Self {
        Raft {
            id: id,
            state: RaftState::Follower(Follower::new()),
            storage: storage,
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn update<RE, R>(&mut self, rpc: &mut R)
    where
        RE: error::Error,
        R: RPC<E = RE>,
    {
        info!("not implemented yet")
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
                debug!(target: "raft_state", "Leader received message from newer leader; converting to follower");
                self.state = RaftState::Follower(l.into());
            }
        }

        if req.prev_log_index > 0 && !self.storage.contains(req.term, req.prev_log_index)? {
            return Ok(AppendEntriesResponse {
                term: current_term,
                success: false,
            });
        }
        Ok(AppendEntriesResponse {
            term: current_term,
            success: true,
        })
    }

    pub fn request_vote(&mut self, req: RequestVoteRequest) -> Result<RequestVoteResponse, E> {
        panic!("not implemented yet")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::*;

    fn follower(term: Term, commit_index: LogIndex) -> Raft<Error> {
        let mut s = InMemoryStorage::new();
        s.set_current_term(term).unwrap();
        let mut r = Raft::new(([127, 0, 0, 1], 8080).into(), Box::new(s));
        r.commit_index = commit_index;
        r
    }

    fn heartbeat(
        term: Term,
        prev_log_index: LogIndex,
        leader_commit: LogIndex,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term: term,
            leader_id: ([127, 0, 0, 1], 8080).into(),
            prev_log_index: prev_log_index,
            entries: Vec::new(),
            leader_commit: leader_commit,
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
