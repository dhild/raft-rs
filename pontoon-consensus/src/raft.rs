use std::sync::{mpsc, Arc, Mutex};

pub struct Raft<S: Storage> {
    storage: Arc<Mutex<S>>,
    commit_index: Arc<Mutex<usize>>,
    commit_listeners: Arc<Mutex<Vec<mpsc::Sender<usize>>>>,
    state: RaftState,
}

pub enum Error {
    NotLeader,
    Other(Box<dyn std::error::Error + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl<S: Storage> Raft<S> {
    pub fn command(&mut self, cmd: &[u8]) -> Result<()> {
        if !self.state.is_leader() {
            // TODO: If we aren't the leader, forward the request
            return Err(Error::NotLeader);
        }

        let term = storage.current_term()?;
        let command = LogCommand::Command(cmd.into());

        let index = self
            .storage
            .lock()?
            .append_entry(LogEntry { term, command })?;

        // TODO: Trigger an append_entries immediately

        // Block until the command is committed:
        loop {
            let (sender, receiver) = mpsc::channel();
            self.commit_listeners.lock()?.push(sender);
            for committed in receiver.iter() {
                if committed >= index {
                    return Ok(());
                }
            }
            // If the sender hung up, check and see if we're committed yet.
            // If we aren't, loop again.
            if self.commit_index.lock()? >= index {
                return Ok(());
            }
        }
    }
}

enum RaftState {
    Follower,
    Candidate,
    Leader,
}

impl RaftState {
    fn is_leader(&self) -> bool {
        match &self {
            RaftState::Follower => false,
            RaftState::Candidate => false,
            RaftState::Leader => true,
        }
    }
}

pub struct LogEntry {
    pub term: usize,
    pub command: LogCommand,
}

pub enum LogCommand {
    Command(Vec<u8>),
    Noop,
}

pub trait Storage: ?Sized {
    fn current_term(&self) -> Result<usize>;
    fn append_entry(&mut self, entry: LogEntry) -> Result<usize>;
}
