extern crate log;
extern crate serde;
extern crate serde_json;
extern crate tiny_http;
extern crate http_req;

pub mod api;
mod error;
pub use api::*;
pub use error::{Error, Result};

use crate::raft;
use crate::raft::Raft;
use crate::RaftConfig;
use http_req::uri::Uri;
use std::time::Duration;
use tiny_http::{Method, Request, Response, Server};

pub fn run_server<F: FnMut() -> bool>(config: RaftConfig, stop: &mut F) -> Result<()> {
    let mut raft = Raft::new(config.clone());
    let server = Server::http(config.server_addr)?;
    loop {
        if stop() {
            return Ok(());
        }
        if let Some(req) = server.recv_timeout(Duration::from_millis(50))? {
            debug!("Serving request: {} {}", req.method(), req.url());
            handle_request(&mut raft, req)?;
        }
        debug!("Updating raft");
        raft = raft.update()?;
    }
}

fn handle_request(raft: &mut Raft, req: Request) -> Result<()> {
    let uri: Uri = req.url().parse()?;
    match (req.method(), uri.path()) {
        (&Method::Post, Some("/append_entries")) => {
            raft_write(raft, req, |raft, x| raft.append_entries(x))?;
        }
        (&Method::Post, Some("/request_vote")) => {
            raft_write(raft, req, |raft, x| raft.request_vote(x))?;
        }
        (&Method::Get, Some("/status")) => {
            raft_read(raft, req, |raft| raft.status())?;
        }
        _ => {
            req.respond(Response::empty(404))?;
        }
    }
    Ok(())
}

fn raft_write<X, Y, F>(raft: &mut Raft, mut req: Request, func: F) -> Result<()>
where
    X: serde::de::DeserializeOwned,
    Y: serde::Serialize,
    F: FnOnce(&mut Raft, X) -> raft::Result<Y>,
{
    let arg = serde_json::from_reader(req.as_reader())?;
    let resp = func(raft, arg)?;
    let resp = serde_json::to_string(&resp)?;
    req.respond(Response::from_string(resp).with_status_code(200))?;
    Ok(())
}

fn raft_read<Y, F>(raft: &mut Raft, req: Request, func: F) -> Result<()>
where
    Y: serde::Serialize + Send + 'static,
    F: FnOnce(&Raft) -> raft::Result<Y> + Send + 'static,
{
    let resp = func(raft)?;
    let resp = serde_json::to_string(&resp)?;
    req.respond(Response::from_string(resp).with_status_code(200))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
