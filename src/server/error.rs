use crate::raft;
use std::error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Http(hyper::http::Error),
    Rest(hyper::Error),
    Json(serde_json::Error),
    Timer(tokio_timer::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref e) => write!(f, "I/O error: {}", e),
            Error::Http(ref e) => write!(f, "REST error: {}", e),
            Error::Rest(ref e) => write!(f, "REST error: {}", e),
            Error::Json(ref e) => write!(f, "JSON error: {}", e),
            Error::Timer(ref e) => write!(f, "Timer error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(_) => "I/O error",
            Error::Http(_) => "HTTP error",
            Error::Rest(_) => "REST error",
            Error::Json(_) => "JSON error",
            Error::Timer(_) => "Timer error",
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Io(ref e) => Some(e),
            Error::Http(ref e) => Some(e),
            Error::Rest(ref e) => Some(e),
            Error::Json(ref e) => Some(e),
            Error::Timer(ref e) => Some(e),
        }
    }
}

impl From<raft::Error> for Error {
    fn from(err: raft::Error) -> Error {
        match err {
            raft::Error::Io(e) => Error::Io(e),
            raft::Error::Json(e) => Error::Json(e),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        use serde_json::error::Category;
        match err.classify() {
            Category::Io => Error::Io(err.into()),
            Category::Syntax | Category::Data | Category::Eof => Error::Json(err),
        }
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Error {
        Error::Rest(err)
    }
}

impl From<hyper::http::Error> for Error {
    fn from(err: hyper::http::Error) -> Error {
        Error::Http(err)
    }
}

impl From<tokio_timer::Error> for Error {
    fn from(err: tokio_timer::Error) -> Error {
        Error::Timer(err)
    }
}
