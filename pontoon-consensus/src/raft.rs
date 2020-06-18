use std::ops::Deref;
use std::sync::{mpsc, Arc, Mutex, MutexGuard, RwLock};

#[derive(Clone)]
pub struct Raft<S: Storage> {
    storage: Arc<Mutex<S>>,
    commit_index: Arc<Mutex<usize>>,
    last_applied: Arc<Mutex<usize>>,
    commit_listeners: Arc<Mutex<Vec<mpsc::Sender<usize>>>>,
    status: Arc<Mutex<Status>>,
}

pub enum Error {
    NotLeader,
    Other(Box<dyn std::error::Error + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl<S: Storage> Raft<S> {
    pub fn new(storage: S) -> Raft<S> {
        let storage = Arc::new(Mutex::new(storage));
        let commit_index = Arc::new(Mutex::new(0));
        let last_applied = Arc::new(Mutex::new(0));
        let commit_listeners = Arc::new(Mutex::new(Vec::new()));
        let status = Arc::new(Mutex::new(Status::Follower));

        let raft = Raft {
            storage: storage.clone(),
            commit_index: commit_index.clone(),
            last_applied: last_applied.clone(),
            commit_listeners: commit_listeners.clone(),
            status: status.clone(),
        };

        Raft {
            storage,
            commit_index,
            last_applied,
            commit_listeners,
            status,
        }
    }

    pub fn command(&mut self, cmd: &[u8]) -> Result<()> {
        if !self.status.lock()?.is_leader() {
            return Err(Error::NotLeader);
        }

        let index = {
            let mut storage = self.storage.lock()?;

            let term = storage.current_term()?;
            let command = LogCommand::Command(cmd.into());

            storage.append_entry(term, command)?
        };

        // TODO: Trigger an append_entries immediately

        // Attempt to be notified when the command completes:
        let (sender, receiver) = mpsc::channel();
        self.commit_listeners.lock()?.push(sender);
        for committed in receiver.iter() {
            if committed >= index {
                return Ok(());
            }
        }
        Err(Error::NotLeader)
    }

    fn check_term_and_get_storage(
        &mut self,
        term: usize,
    ) -> Result<Option<(MutexGuard<S>, usize)>> {
        let mut storage = self.storage.lock()?;
        let current_term = storage.current_term()?;
        // Ensure we are on the latest term
        if term > current_term {
            storage.set_current_term(term)?;
            *self.status.lock()? = Status::Follower;
            Ok(None)
        } else {
            Ok(Some((storage, current_term)))
        }
    }

    fn append_entries(
        &mut self,
        term: usize,
        prev_log_index: usize,
        prev_log_term: usize,
        leader_commit: usize,
        entries: Vec<LogEntry>,
    ) -> Result<(bool, usize)> {
        let Some((mut storage, current_term)) = self.check_term_and_get_storage(term)?;
        // Ensure the leader is on the latest term
        if term < current_term {
            return Ok((false, current_term));
        }
        // Ensure our previous entries match
        if let Some(term) = storage.get_term(prev_log_index)? {
            if term != prev_log_term {
                return Ok((false, current_term));
            }
        }
        // Remove any conflicting log entries
        for e in entries.iter() {
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
        for e in entries {
            if e.index > index {
                storage.append_entry(e.term, e.command)?;
                index = e.index;
            }
        }

        // Update the commit index with the latest committed value in our logs
        self.update_commit_index(leader_commit.min(index));

        Ok((true, current_term))
    }

    fn update_commit_index(&mut self, commit_index: usize) -> Result<()> {
        let mut ci = self.commit_index.lock()?;
        if commit_index > *ci {
            *ci = commit_index;
            // TODO: Fire off listeners
        }
        Ok(())
    }

    fn request_vote(
        &mut self,
        term: usize,
        candidate_id: String,
        last_log_index: usize,
        last_log_term: usize,
    ) -> Result<(bool, usize)> {
        let Some((mut storage, current_term)) = self.check_term_and_get_storage(term)?;
        // Ensure the candidate is on the latest term
        if term < current_term {
            return Ok((false, current_term));
        }
        // Check if we can vote for this candidate
        match storage.voted_for()? {
            None | Some(x) if x == &candidate_id => {
                // Grant the vote as long as their log is up to date.
                // Raft determines which of two logs is more up-to-date by comparing the index
                // and term of the last entries in the logs. If the logs have last entries with
                // different terms, then the log with the later term is more up-to-date. If the
                // logs end with the same term, then whichever log is longer is more up-to-date.
                let term = storage.last_term()?;
                let index = storage.last_index()?;
                if term > last_log_term || index > last_log_index {
                    Ok((false, current_term))
                } else {
                    storage.set_voted_for(Some(candidate_id))?;
                    Ok((true, current_term))
                }
            }
            _ => Ok((false, current_term)),
        }
    }
}

enum Status {
    Follower,
    Candidate,
    Leader,
}

impl Status {
    fn is_leader(&self) -> bool {
        match &self {
            Status::Follower => false,
            Status::Candidate => false,
            Status::Leader => true,
        }
    }
}

pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub command: LogCommand,
}

pub enum LogCommand {
    Command(Vec<u8>),
    Noop,
}

pub trait Storage: Sized {
    fn current_term(&self) -> Result<usize>;
    fn set_current_term(&mut self, current_term: usize) -> Result<()>;
    fn voted_for(&self) -> Result<Option<String>>;
    fn set_voted_for(&mut self, candidate_id: Option<String>) -> Result<()>;
    fn last_index(&self) -> Result<usize>;
    fn last_term(&self) -> Result<usize>;
    fn append_entry(&mut self, term: usize, command: LogCommand) -> Result<usize>;
    fn get_term(&self, log_index: usize) -> Result<Option<usize>>;
    fn remove_entries_starting_at(&self, log_index: usize) -> Result<()>;
}
