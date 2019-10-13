use log::{debug, error, warn};
use std::net::SocketAddr;
use std::result::Result;
use std::sync::mpsc::{channel, Receiver};
use std::thread;

use crate::api::*;

pub trait RPC {
    type Error: ::std::error::Error + Send + 'static;

    fn append_entries(
        &mut self,
        peer: SocketAddr,
        req: AppendEntriesRequest,
    ) -> Receiver<Result<AppendEntriesResponse, Self::Error>>;

    fn request_vote(
        &mut self,
        peer: SocketAddr,
        req: RequestVoteRequest,
    ) -> Receiver<Result<RequestVoteResponse, Self::Error>>;

    fn request_votes(
        &mut self,
        peers: &Vec<Peer>,
        req: RequestVoteRequest,
    ) -> Receiver<(Peer, RequestVoteResponse)> {
        let (ballot_box, ballots) = channel();
        for peer in peers.iter() {
            let my_peer = peer.clone();
            let my_ballot_box = ballot_box.clone();
            let vote = self.request_vote(my_peer.address, req.clone());
            thread::spawn(move || match vote.recv() {
                Ok(Ok(vote)) => {
                    if let Err(e) = my_ballot_box.send((my_peer.clone(), vote)) {
                        debug!(
                            "vote from peer {:?} discarded because the receiver disconnected: {}",
                            my_peer, e
                        );
                    }
                }
                Ok(Err(e)) => {
                    error!("failed to recieve vote from peer {:?}: {}", my_peer, e);
                }
                Err(e) => {
                    warn!(
                        "failed to recieve vote from peer {:?} (disconnected): {}",
                        my_peer, e
                    );
                }
            });
        }
        ballots
    }
}
