use futures::executor::ThreadPool;
use std::sync::{mpsc, Arc, Mutex};

use crate::error::{Error, Result};
use crate::rpc::*;
use crate::state::Peer;
use crate::storage::{LogCommand, Storage};

#[derive(Clone)]
pub struct Raft<S: Storage, R: RPC> {
    id: String,
    peers: Vec<Peer>,
    storage: Arc<Mutex<S>>,
    rpc: Arc<Mutex<R>>,
    commit_index: Arc<Mutex<usize>>,
    last_applied: Arc<Mutex<usize>>,
    commit_listeners: Arc<Mutex<Vec<mpsc::Sender<usize>>>>,
    status: Arc<Mutex<Status>>,
    executor: ThreadPool,
}

impl<S: Storage, R: RPC> Raft<S, R> {
    fn new(id: String, peers: Vec<Peer>, storage: S, rpc: R) -> Result<Raft<S, R>> {
        let storage = Arc::new(Mutex::new(storage));
        let rpc = Arc::new(Mutex::new(rpc));
        let commit_index = Arc::new(Mutex::new(0));
        let last_applied = Arc::new(Mutex::new(0));
        let commit_listeners = Arc::new(Mutex::new(Vec::new()));
        let status = Arc::new(Mutex::new(Status::Follower));

        let raft = Raft {
            id,
            peers,
            storage,
            rpc,
            commit_index,
            last_applied,
            commit_listeners,
            status,
            executor: ThreadPool::new()?,
        };

        raft.executor.spawn_ok(async {
            loop {
                // Run ops ....
            }
        });

        Ok(raft)
    }

    pub fn command(&mut self, cmd: &[u8]) -> Result<()> {
        if !self.status.lock().expect("poisoned lock").is_leader() {
            return Err(Error::NotLeader.into());
        }

        let index = {
            let mut storage = self.storage.lock().expect("poisoned lock");

            let term = storage.current_term()?;
            let command = LogCommand::Command(cmd.into());

            storage.append_entry(term, command)?
        };

        // TODO: Trigger an append_entries immediately

        // Attempt to be notified when the command completes:
        let (sender, receiver) = mpsc::channel();
        self.commit_listeners
            .lock()
            .expect("poisoned lock")
            .push(sender);
        for committed in receiver.iter() {
            if committed >= index {
                return Ok(());
            }
        }
        Err(Error::NotLeader.into())
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
