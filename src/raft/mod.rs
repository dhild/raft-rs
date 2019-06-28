use serde::{Deserialize, Serialize};
use std::error;
use std::result;
use std::slice;

mod logs;
pub use logs::*;

pub enum Server<E> {
    Follower(Raft<Follower, E>),
    Candidate(Raft<Candidate, E>),
    Leader(Raft<Leader, E>),
}

impl<E: error::Error> Server<E> {
    pub fn new(storage: Box<dyn Storage<E = E>>) -> Self {
        Server::Follower(Raft::<Follower, E>::new(storage))
    }

    pub fn run(self) {}
}

pub struct Raft<S, E> {
    state: S,
    storage: Box<dyn Storage<E = E>>,
    commit_index: LogIndex,
    last_applied: LogIndex,
}

pub struct Leader {
    next_index: Vec<(ServerId, LogIndex)>,
    match_index: Vec<(ServerId, LogIndex)>,
}
pub struct Candidate {}
pub struct Follower {}

impl<E: error::Error> Raft<Follower, E> {
    fn new(storage: Box<dyn Storage<E = E>>) -> Self {
        Raft {
            state: Follower {},
            storage: storage,
            commit_index: 0,
            last_applied: 0,
        }
    }
}

impl<E: error::Error> From<Raft<Follower, E>> for Raft<Candidate, E> {
    fn from(val: Raft<Follower, E>) -> Raft<Candidate, E> {
        Raft {
            state: Candidate {},
            storage: val.storage,
            commit_index: val.commit_index,
            last_applied: val.last_applied,
        }
    }
}

impl<E: error::Error> From<Raft<Candidate, E>> for Raft<Follower, E> {
    fn from(val: Raft<Candidate, E>) -> Raft<Follower, E> {
        Raft {
            state: Follower {},
            storage: val.storage,
            commit_index: val.commit_index,
            last_applied: val.last_applied,
        }
    }
}

impl<E: error::Error> From<(Raft<Candidate, E>, LogIndex, Term, &[ServerId])> for Raft<Leader, E> {
    fn from(val: (Raft<Candidate, E>, LogIndex, Term, &[ServerId])) -> Raft<Leader, E> {
        let (val, last_log, term, servers) = val;
        Raft {
            state: Leader {
                next_index: servers
                    .iter()
                    .map(|x| (x.clone(), last_log + (1 as usize)))
                    .collect(),
                match_index: servers.iter().map(|x| (x.clone(), 0 as usize)).collect(),
            },
            storage: val.storage,
            commit_index: val.commit_index,
            last_applied: val.last_applied,
        }
    }
}

impl<E: error::Error> From<Raft<Leader, E>> for Raft<Follower, E> {
    fn from(val: Raft<Leader, E>) -> Raft<Follower, E> {
        Raft {
            state: Follower {},
            storage: val.storage,
            commit_index: val.commit_index,
            last_applied: val.last_applied,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesRequest {
    term: Term,
    leader_id: ServerId,
    prev_log_index: LogIndex,
    entries: Vec<LogEntry>,
    leader_commit: LogIndex,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse {
    term: Term,
    success: bool,
}

impl<E: error::Error> Raft<Follower, E> {
    fn append_entries(
        mut self,
        req: AppendEntriesRequest,
    ) -> (Self, Result<AppendEntriesResponse, E>) {
        let res = self.append_entries_mut(req);
        (
            Raft {
                state: Follower {},
                storage: self.storage,
                commit_index: self.commit_index,
                last_applied: self.last_applied,
            },
            res,
        )
    }

    fn append_entries_mut(
        &mut self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, E> {
        let term = self.storage.current_term()?;
        if req.term < term {
            return Ok(AppendEntriesResponse {
                term: term,
                success: false,
            });
        }
        if req.prev_log_index > 0 && !self.storage.contains(req.term, req.prev_log_index)? {
            return Ok(AppendEntriesResponse {
                term: term,
                success: false,
            });
        }
        Ok(AppendEntriesResponse {
            term: term,
            success: true,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteResponse {
    term: Term,
    vote_granted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::*;

    fn follower(term: Term, commit_index: LogIndex) -> Raft<Follower, Error> {
        let mut s = InMemoryStorage::new();
        s.set_current_term(term);
        let mut r = Raft::<Follower, Error>::new(Box::new(s));
        r.commit_index = commit_index;
        r
    }

    fn heartbeat(
        term: Term,
        prev_log_index: LogIndex,
        commit_index: LogIndex,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term: term,
            leader_id: "leader".to_string(),
            prev_log_index: prev_log_index,
            entries: Vec::new(),
            leader_commit: commit_index,
        }
    }

    #[test]
    fn test_follower_append_entries_heartbeat() {
        // Term is earlier, should fail
        let f = follower(3, 0);
        let (f, resp) = f.append_entries(heartbeat(2, 0, 0));
        assert!(
            !resp.unwrap().success,
            "Reply should be false if term < current_term"
        );

        // Term is the same, should fail when index is too new
        let f = follower(3, 0);
        let (f, resp) = f.append_entries(heartbeat(3, 1, 0));
        assert!(
            !resp.unwrap().success,
            "Reply should be false when entries are missing"
        );

        // Term is the same, should succeed when index matches
        let f = follower(3, 0);
        let (f, resp) = f.append_entries(heartbeat(3, 0, 0));
        assert!(
            resp.unwrap().success,
            "Reply should be true if term = current_term"
        );

        // Term is newer, should fail when index is too new
        let f = follower(3, 0);
        let (f, resp) = f.append_entries(heartbeat(4, 1, 0));
        assert!(
            !resp.unwrap().success,
            "Reply should be false when entries are missing"
        );

        // Term is newer, should succeed when index matches
        let f = follower(3, 0);
        let (f, resp) = f.append_entries(heartbeat(4, 0, 0));
        assert!(
            resp.unwrap().success,
            "Reply should be true if term > current_term"
        );
    }
}
