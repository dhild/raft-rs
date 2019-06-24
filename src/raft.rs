use std::error;
use std::result;
use std::slice;

pub type Term = u32;
pub type ServerId = String;

pub type LogEntry = String;

pub trait Storage {
    type E: error::Error;

    fn current_term(&self) -> result::Result<Term, Self::E>;
    fn set_current_term(&mut self, t: Term) -> result::Result<(), Self::E>;

    fn voted_for(&self) -> result::Result<Option<ServerId>, Self::E>;
    fn set_voted_for(&mut self, candidate: Option<ServerId>) -> result::Result<(), Self::E>;

    fn logs(&self) -> result::Result<slice::Iter<LogEntry>, Self::E>;
    fn apppend(&mut self, logs: Vec<LogEntry>) -> result::Result<(), Self::E>;
}

type LogIndex = usize;

pub fn run<E: error::Error>(store: Box<Storage<E = E>>) {
    let mut server = Server::new(store);
    server.run();
}

enum Server<E> {
    Follower {
        storage: Box<Storage<E = E>>,
        commitIndex: LogIndex,
        lastApplied: LogIndex,
    },
    Candidate {
        storage: Box<Storage<E = E>>,
        commitIndex: LogIndex,
        lastApplied: LogIndex,
    },
    Leader {
        storage: Box<Storage<E = E>>,
        commitIndex: LogIndex,
        lastApplied: LogIndex,
        nextIndex: Vec<(ServerId, LogIndex)>,
        matchIndex: Vec<(ServerId, LogIndex)>,
    },
}

impl<E: error::Error> Server<E> {
    fn new(storage: Box<Storage<E = E>>) -> Server<E> {
        Server::Follower {
            storage: storage,
            commitIndex: 0,
            lastApplied: 0,
        }
    }
    fn run(&mut self) {}
}
