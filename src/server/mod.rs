pub mod api;
mod error;
pub use api::*;
pub use error::Error;

use crate::raft;
use crate::raft::Raft;
use crate::RaftConfig;
use futures::future::{err, lazy, loop_fn, ok, Loop};
use futures::{Future, Poll, Stream};
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_timer::Delay;

pub fn new_server<F: Future<Item = ()> + Send + 'static>(
    config: RaftConfig,
    stop: F,
) -> ServerFuture<()> {
    let addr = config.server_addr;
    let inner = lazy(|| Raft::new(config))
        .map(Arc::new)
        .map_err(Error::from)
        .and_then(move |raft| {
            let r = Arc::clone(&raft);
            info!("Listening on {}", addr);
            hyper::Server::bind(&addr)
                .serve(move || {
                    let r = Arc::clone(&raft);
                    service_fn(move |req: Request<Body>| route(&r, req))
                })
                .with_graceful_shutdown(stop)
                .map_err(Error::from)
                .select(update_raft(r))
                .then(|res| match res {
                    Ok((a, _)) => ok(a),
                    Err((a, _)) => err(a),
                })
        });

    ServerFuture {
        inner: Box::new(inner),
    }
}

fn update_raft(raft: Arc<Raft>) -> ServerFuture<()> {
    let inner = loop_fn(raft, |raft| {
        lazy(move || match raft.update() {
            Ok(()) => Ok(Loop::Continue(raft)),
            Err(e) => {
                error!("Update failed, stopping server: {}", e);
                Err(e.into())
            }
        })
        .join(Delay::new(Instant::now() + Duration::from_millis(50)).map_err(Error::from))
        .and_then(|(res, _)| Ok(res))
    });
    ServerFuture {
        inner: Box::new(inner),
    }
}

fn route(raft: &Arc<Raft>, req: Request<Body>) -> ServerFuture<Response<Body>> {
    let raft = Arc::clone(raft);
    let method = req.method();
    let path = req.uri().path();
    trace!("{} {}", method, path);
    let fut = match (method, path) {
        (&Method::POST, "/append_entries") => {
            raft_write(raft, req, |raft, x| raft.append_entries(x))
        }
        (&Method::POST, "/request_vote") => raft_write(raft, req, |raft, x| raft.request_vote(x)),
        (&Method::GET, "/status") => raft_read(raft, |raft| raft.status()),
        _ => {
            return ServerFuture {
                inner: Box::new(ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap())),
            };
        }
    }
    .then(|res| match res {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(body)),
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("{}", e))),
    })
    .map_err(Error::from);
    ServerFuture {
        inner: Box::new(fut),
    }
}

pub struct ServerFuture<T> {
    inner: Box<dyn Future<Item = T, Error = Error> + Send>,
}

fn raft_write<X, Y, F>(raft: Arc<Raft>, req: Request<Body>, func: F) -> ServerFuture<String>
where
    X: serde::de::DeserializeOwned,
    Y: serde::Serialize + Send + 'static,
    F: FnOnce(&Raft, X) -> raft::Result<Y> + Send + 'static,
{
    let resp = req
        .into_body()
        .concat2()
        .map_err(Error::from)
        .and_then(move |chunk| {
            let parsed = match serde_json::from_slice(&chunk) {
                Err(e) => {
                    return Err(Error::Json(e));
                }
                Ok(p) => p,
            };
            match func(&raft, parsed) {
                Err(e) => Err(Error::from(e)),
                Ok(res) => match serde_json::to_string(&res) {
                    Err(e) => Err(Error::from(e)),
                    Ok(body) => Ok(body),
                },
            }
        });
    ServerFuture {
        inner: Box::new(resp),
    }
}

fn raft_read<Y, F>(raft: Arc<Raft>, func: F) -> ServerFuture<String>
where
    Y: serde::Serialize + Send + 'static,
    F: FnOnce(&Raft) -> raft::Result<Y> + Send + 'static,
{
    let inner = lazy(move || func(&raft))
        .map_err(Error::from)
        .and_then(|res| serde_json::to_string(&res).map_err(Error::from));
    ServerFuture {
        inner: Box::new(inner),
    }
}

impl<T> Future for ServerFuture<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}
