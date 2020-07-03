use crate::protocol::{Candidate, Follower, Leader, Peer, ProtocolState};
use crate::rpc::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse, RPC,
};
use crate::storage::{LogCommand, MemoryStorage};
use crate::Storage;
use async_channel::{Receiver, Sender};
use async_lock::Lock;
use env_logger::Env;
use log::debug;
use std::error::Error;
use std::time::Duration;

const TEST_TIMEOUT_MS: u64 = 150;

pub fn setup() {
    let _ = env_logger::from_env(Env::default())
        .filter_module("pontoon", log::LevelFilter::Debug)
        .is_test(true)
        .try_init();
}

#[tokio::test]
async fn follower_timeout() {
    setup();
    let (term_updates_tx, term_updates_rx) = async_channel::bounded(1);
    let (append_entries_tx, append_entries_rx) = async_channel::bounded(1);
    let (commits_to_apply_tx, _commits_to_apply_rx) = async_channel::bounded(1);
    let follower = Follower::new(
        Duration::from_millis(TEST_TIMEOUT_MS),
        Lock::new(MemoryStorage::new()),
        Lock::new(0),
        term_updates_rx,
        append_entries_rx,
        commits_to_apply_tx,
    );

    append_entries_tx.send(0).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    append_entries_tx.send(0).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    term_updates_tx.send(()).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    append_entries_tx.send(0).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    term_updates_tx.send(()).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);

    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(2 * TEST_TIMEOUT_MS)).await;
    tokio::time::resume();
    assert_eq!(follower.run_once().await, ProtocolState::Candidate);
}

#[tokio::test]
async fn follower_commit_processing() {
    setup();
    let (_term_updates_tx, term_updates_rx) = async_channel::bounded(1);
    let (append_entries_tx, append_entries_rx) = async_channel::bounded(10);
    let (commits_to_apply_tx, _commits_to_apply_rx) = async_channel::bounded(10);
    let storage = Lock::new(MemoryStorage::new());
    let follower = Follower::new(
        Duration::from_millis(TEST_TIMEOUT_MS),
        storage.clone(),
        Lock::new(0),
        term_updates_rx,
        append_entries_rx,
        commits_to_apply_tx,
    );

    append_entries_tx.send(0).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);

    {
        let mut storage = storage.lock().await;
        storage.append_entry(1, LogCommand::Noop).unwrap();
        storage.append_entry(1, LogCommand::Noop).unwrap();
        storage.append_entry(2, LogCommand::Noop).unwrap();
    }
    // Send another heartbeat, then a new entry, then a heartbeat, then a new entry, then a new entry, then a heartbeat
    append_entries_tx.send(0).await.unwrap();
    append_entries_tx.send(1).await.unwrap();
    append_entries_tx.send(1).await.unwrap();
    append_entries_tx.send(2).await.unwrap();
    append_entries_tx.send(3).await.unwrap();
    append_entries_tx.send(3).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
}

#[tokio::test]
async fn follower_rpc_shutdown() {
    setup();
    let (_term_updates_tx, term_updates_rx) = async_channel::bounded(1);
    let (append_entries_tx, append_entries_rx) = async_channel::bounded(10);
    let (commits_to_apply_tx, _commits_to_apply_rx) = async_channel::bounded(10);
    let storage = Lock::new(MemoryStorage::new());
    let follower = Follower::new(
        Duration::from_millis(TEST_TIMEOUT_MS),
        storage.clone(),
        Lock::new(0),
        term_updates_rx,
        append_entries_rx,
        commits_to_apply_tx,
    );

    append_entries_tx.send(0).await.unwrap();
    assert_eq!(follower.run_once().await, ProtocolState::Follower);
    drop(append_entries_tx);
    assert_eq!(follower.run_once().await, ProtocolState::Shutdown);
}

#[derive(Clone)]
struct TestRPC {
    peers: Vec<TestRPCPeer>,
}

#[derive(Clone)]
struct TestRPCPeer {
    pub peer: Peer,
    pub append_entries_tx: Sender<(AppendEntriesRequest, Sender<AppendEntriesResponse>)>,
    pub append_entries_rx: Receiver<(AppendEntriesRequest, Sender<AppendEntriesResponse>)>,
    pub request_vote_tx: Sender<(RequestVoteRequest, Sender<RequestVoteResponse>)>,
    pub request_vote_rx: Receiver<(RequestVoteRequest, Sender<RequestVoteResponse>)>,
}

impl TestRPCPeer {
    fn new(addr: &str) -> TestRPCPeer {
        let peer = Peer {
            id: addr.to_string(),
            address: addr.to_string(),
            voting: true,
        };
        let (append_entries_tx, append_entries_rx) = async_channel::unbounded();
        let (request_vote_tx, request_vote_rx) = async_channel::unbounded();
        TestRPCPeer {
            peer,
            append_entries_tx,
            append_entries_rx,
            request_vote_tx,
            request_vote_rx,
        }
    }

    fn clear_request_queues(&self) {
        while let Ok((r, _)) = self.append_entries_rx.try_recv() {
            debug!("{} dropping {:?}", self.peer, r);
        }
        while let Ok((r, _)) = self.request_vote_rx.try_recv() {
            debug!("{} dropping {:?}", self.peer, r);
        }
    }

    fn setup_vote(&self, success: bool) {
        let rx = self.request_vote_rx.clone();
        tokio::spawn(async move {
            let (request, tx) = rx.recv().await.unwrap();
            tx.send(RequestVoteResponse {
                success,
                term: request.term,
            })
            .await
            .unwrap();
        });
    }

    fn setup_vote_newer_term(&self) {
        let rx = self.request_vote_rx.clone();
        tokio::spawn(async move {
            let (request, tx) = rx.recv().await.unwrap();
            tx.send(RequestVoteResponse {
                success: false,
                term: request.term + 1,
            })
            .await
            .unwrap();
        });
    }
}

impl TestRPC {
    fn new() -> TestRPC {
        TestRPC { peers: Vec::new() }
    }

    fn add_peer(&mut self, addr: &str) -> TestRPCPeer {
        let peer = TestRPCPeer::new(addr);
        self.peers.push(peer.clone());
        peer
    }

    fn get_peer(&self, addr: &str) -> TestRPCPeer {
        self.peers
            .iter()
            .find(|p| p.peer.address == addr)
            .cloned()
            .expect("Peer not found")
    }
}

#[async_trait::async_trait]
impl RPC for TestRPC {
    async fn append_entries(
        &self,
        peer_address: String,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Box<dyn Error + Send + Sync>> {
        let peer = self.get_peer(&peer_address);
        let (tx, rx) = async_channel::bounded(1);
        peer.append_entries_tx.send((request, tx)).await.unwrap();
        let resp = rx.recv().await?;
        Ok(resp)
    }

    async fn request_vote(
        &self,
        peer_address: String,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Box<dyn Error + Send + Sync>> {
        let peer = self.get_peer(&peer_address);
        let (tx, rx) = async_channel::bounded(1);
        peer.request_vote_tx.send((request, tx)).await.unwrap();
        let resp = rx.recv().await?;
        Ok(resp)
    }
}

struct CandidateTest {
    term_updates_tx: Sender<()>,
    rpc: TestRPC,
    peer1: TestRPCPeer,
    peer2: TestRPCPeer,
    candidate: Candidate<MemoryStorage, TestRPC>,
}

impl CandidateTest {
    fn new() -> CandidateTest {
        setup();
        let (term_updates_tx, term_updates_rx) = async_channel::bounded(1);
        let mut rpc = TestRPC::new();
        let peer1 = rpc.add_peer("peer1");
        let peer2 = rpc.add_peer("peer2");
        let candidate = Candidate::new(
            "test-candidate".to_string(),
            Duration::from_millis(TEST_TIMEOUT_MS),
            vec![peer1.peer.clone(), peer2.peer.clone()],
            Lock::new(MemoryStorage::new()),
            rpc.clone(),
            term_updates_rx,
        );
        CandidateTest {
            term_updates_tx,
            rpc,
            peer1,
            peer2,
            candidate,
        }
    }

    fn clear_rpc_queue(&self) {
        self.peer1.clear_request_queues();
        self.peer2.clear_request_queues();
    }
}

#[tokio::test]
async fn candidate_term_update() {
    let c = CandidateTest::new();

    c.term_updates_tx.send(()).await.unwrap();
    assert_eq!(c.candidate.run_once().await, ProtocolState::Follower);
}

#[tokio::test]
async fn candidate_peer_term_update() {
    let c = CandidateTest::new();

    // If either peer responds with a newer term, go to follower:
    c.peer2.setup_vote_newer_term();
    assert_eq!(c.candidate.run_once().await, ProtocolState::Follower);

    c.clear_rpc_queue();

    c.peer1.setup_vote_newer_term();
    assert_eq!(c.candidate.run_once().await, ProtocolState::Follower);
}

#[tokio::test]
async fn candidate_election_results() {
    let c = CandidateTest::new();

    // If not enough peers respond, timeout the election:
    assert_eq!(c.candidate.run_once().await, ProtocolState::Candidate);

    c.clear_rpc_queue();

    c.peer1.setup_vote(false);
    c.peer2.setup_vote(false);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Candidate);

    c.clear_rpc_queue();

    c.peer2.setup_vote(false);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Candidate);

    c.clear_rpc_queue();

    c.peer1.setup_vote(false);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Candidate);

    c.clear_rpc_queue();

    // Any majority of votes is enough to win:
    c.peer1.setup_vote(true);
    c.peer2.setup_vote(false);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Leader);

    c.clear_rpc_queue();

    c.peer1.setup_vote(false);
    c.peer2.setup_vote(true);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Leader);

    c.clear_rpc_queue();

    c.peer1.setup_vote(true);
    c.peer2.setup_vote(true);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Leader);
}

#[tokio::test]
async fn candidate_rpc_shutdown() {
    let c = CandidateTest::new();

    drop(c.term_updates_tx);
    assert_eq!(c.candidate.run_once().await, ProtocolState::Shutdown);
}

struct LeaderTest {
    storage: Lock<MemoryStorage>,
    term_updates_tx: Sender<()>,
    commits_to_apply_rx: Receiver<(usize, LogCommand)>,
    new_logs_tx: Sender<usize>,
    index_update_tx: Sender<(String, usize)>,
    peer1: Receiver<usize>,
    peer2: Receiver<usize>,
    leader: Leader<MemoryStorage>,
}

const LEADER_PEER_QUEUE_SIZE: usize = 10;

impl LeaderTest {
    fn new() -> LeaderTest {
        setup();
        let (term_updates_tx, term_updates_rx) = async_channel::unbounded();
        let (commits_to_apply_tx, commits_to_apply_rx) = async_channel::unbounded();
        let (new_logs_tx, new_logs_rx) = async_channel::unbounded();
        let (index_update_tx, index_update_rx) = async_channel::unbounded();
        let (peer1_tx, peer1) = async_channel::bounded(LEADER_PEER_QUEUE_SIZE);
        let (peer2_tx, peer2) = async_channel::bounded(LEADER_PEER_QUEUE_SIZE);
        let storage = Lock::new(MemoryStorage::new());

        let leader = Leader::new(
            vec![
                Peer {
                    id: "peer1".to_string(),
                    address: "peer1".to_string(),
                    voting: true,
                },
                Peer {
                    id: "peer2".to_string(),
                    address: "peer2".to_string(),
                    voting: true,
                },
            ],
            storage.clone(),
            Lock::new(0),
            term_updates_rx,
            commits_to_apply_tx,
            new_logs_rx,
            index_update_rx,
            vec![peer1_tx, peer2_tx],
        );
        LeaderTest {
            storage,
            term_updates_tx,
            commits_to_apply_rx,
            new_logs_tx,
            index_update_tx,
            peer1,
            peer2,
            leader,
        }
    }
}

#[tokio::test]
async fn leader_term_update() {
    let mut l = LeaderTest::new();

    l.term_updates_tx.send(()).await.unwrap();
    assert_eq!(l.leader.run_once().await, ProtocolState::Follower);
}

#[tokio::test]
async fn leader_forward_new_log() {
    let mut l = LeaderTest::new();

    let new_log_index = {
        let mut storage = l.storage.lock().await;
        storage.append_entry(1, LogCommand::Noop).unwrap()
    };

    l.new_logs_tx.send(new_log_index).await.unwrap();
    assert_eq!(l.leader.run_once().await, ProtocolState::Leader);
    assert_eq!(l.peer1.try_recv().unwrap(), new_log_index);
    assert_eq!(l.peer2.try_recv().unwrap(), new_log_index);
}

#[tokio::test]
async fn leader_does_not_block_on_forwarders() {
    let mut l = LeaderTest::new();

    let indices = {
        let mut storage = l.storage.lock().await;
        let mut indices = Vec::new();
        for _ in 0..(2 * LEADER_PEER_QUEUE_SIZE) {
            let index = storage.append_entry(1, LogCommand::Noop).unwrap();
            l.new_logs_tx.send(index).await.unwrap();
            indices.push(index);
        }
        indices
    };

    // We should be able to process each log index without blocking on sending the update:
    for _ in indices.iter() {
        assert_eq!(l.leader.run_once().await, ProtocolState::Leader);
    }

    // Both peers should get a series of updates:
    for _ in 0..LEADER_PEER_QUEUE_SIZE {
        assert!(l.peer1.try_recv().is_ok());
        assert!(l.peer2.try_recv().is_ok());
    }

    // Further updates don't block on the dropped values:
    let new_log_index = {
        let mut storage = l.storage.lock().await;
        storage.append_entry(1, LogCommand::Noop).unwrap()
    };

    l.new_logs_tx.send(new_log_index).await.unwrap();
    assert_eq!(l.leader.run_once().await, ProtocolState::Leader);
    assert_eq!(l.peer1.try_recv().unwrap(), new_log_index);
    assert_eq!(l.peer2.try_recv().unwrap(), new_log_index);
}

#[tokio::test]
async fn leader_rpc_shutdown_terms() {
    let mut l = LeaderTest::new();

    drop(l.term_updates_tx);
    assert_eq!(l.leader.run_once().await, ProtocolState::Shutdown);
}

#[tokio::test]
async fn leader_rpc_shutdown_consensus() {
    let mut l = LeaderTest::new();

    drop(l.new_logs_tx);
    assert_eq!(l.leader.run_once().await, ProtocolState::Shutdown);
}
