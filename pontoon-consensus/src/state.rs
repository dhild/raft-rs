use futures::{future::ready, FutureExt, StreamExt};
use log::{error, info};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crate::error::Result;
use crate::rpc::{RequestVoteRequest, RPC};
use crate::storage::{LogCommand, Storage};
use futures::stream::FuturesUnordered;

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

pub(crate) struct State<S: Storage> {
    storage: S,
    // state_changes: Sender<Leadership>,
    state_thread: thread::JoinHandle<()>,
}

impl<S: Storage> State<S> {
    fn new(storage: S) -> Result<State<S>> {
        let state_thread = thread::spawn(|| {});

        Ok(State {
            storage,
            state_thread,
        })
    }

    fn is_leader(&self) -> bool {
        // let state = self.state.lock().expect("poisoned lock");
        // match *state {
        //     Leadership::Follower => false,
        //     Leadership::Candidate => false,
        //     Leadership::Leader => true,
        // }
        false
    }

    pub fn convert_to_follower(&mut self, term: usize) -> Result<()> {
        self.storage.set_current_term(term)?;
        self.storage.set_voted_for(None)?;
        Ok(())
    }
}

enum Leadership {
    Follower,
    Candidate,
    Leader,
    Shutdown,
}

fn handle_state(drop_to_follower: Receiver<()>) {
    loop {}
}

struct StateMachine<S: Storage> {
    storage: S,
    commits: Receiver<usize>,
    state_machine: Sender<(usize, Vec<u8>)>,
    last_applied: Arc<Mutex<usize>>,
}

impl<S: Storage> StateMachine<S> {
    fn apply(&mut self, timeout: Duration) -> std::result::Result<(), RecvTimeoutError> {
        self.commits.recv_timeout(timeout).map(|commit_index| {
            let mut last_applied = self.last_applied.lock().expect("poisoned lock");
            while commit_index > *last_applied {
                let index = *last_applied + 1;
                match self.storage.get_command(index) {
                    Ok(cmd) => {
                        *last_applied = index;
                        match cmd {
                            LogCommand::Command(data) => {
                                if let Err(_) = self.state_machine.send((index, data)) {
                                    error!("State machine channel has been closed");
                                }
                            }
                            LogCommand::Noop => {}
                        }
                        *last_applied = index;
                    }
                    Err(e) => {
                        error!(
                            "Failed to load command for applying to state machine: {}",
                            e
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        })
    }
}

struct Follower<S: Storage> {
    timeout: Duration,
    state_machine: StateMachine<S>,
}

impl<S: Storage> Follower<S> {
    fn run(&mut self) -> Leadership {
        use rand::prelude::*;
        let timeout = self.timeout.mul_f32(1.0 + rand::thread_rng().gen::<f32>());
        match self.state_machine.apply(timeout) {
            Ok(()) => {
                // Remain a follower:
                Leadership::Follower
            }
            Err(RecvTimeoutError::Timeout) => {
                info!("Exceeded timeout without being contacted by the leader; converting to candidate");
                Leadership::Candidate
            }
            Err(RecvTimeoutError::Disconnected) => {
                info!("State processing disconnected; shutting down");
                Leadership::Shutdown
            }
        }
    }
}

struct Candidate<S: Storage, R: RPC> {
    id: String,
    peers: Vec<Peer>,
    timeout: Duration,
    storage: Arc<RwLock<S>>,
    rpc: R,
}

impl<S: Storage, R: RPC> Candidate<S, R> {
    fn run(&mut self) -> Leadership {
        let request = match self.init_election() {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to initialize election: {}", e);
                return Leadership::Follower;
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
        let majority = match self.peers.iter().filter(|p| p.voting).count() {
            0 => 1,
            1 | 2 => 2,
            3 | 4 => 3,
            x => panic!("Too many voting peers (found {})", x),
        };

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
                    Leadership::Follower
                } else if votes >= majority {
                    Leadership::Leader
                } else {
                    Leadership::Candidate
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
}

enum ElectionResult {
    Winner,
    NotWinner,
    OutdatedTerm(usize),
}
