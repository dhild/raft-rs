use crate::raft;
use std::error;
use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Http(http_req::error::Error),
    Json(serde_json::Error),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref e) => write!(f, "I/O error: {}", e),
            Error::Http(ref e) => write!(f, "HTTP error: {}", e),
            Error::Json(ref e) => write!(f, "JSON error: {}", e),
            Error::Other(ref e) => write!(f, "Other error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(_) => "I/O error",
            Error::Http(_) => "HTTP error",
            Error::Json(_) => "JSON error",
            Error::Other(_) => "Other error",
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Io(ref e) => Some(e),
            Error::Http(ref e) => Some(e),
            Error::Json(ref e) => Some(e),
            Error::Other(_) => None,
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

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<http_req::error::Error> for Error {
    fn from(e: http_req::error::Error) -> Error {
        Error::Http(e)
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

impl From<Box<std::error::Error + Send + Sync>> for Error {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Error {
        Error::Other(e)
    }
}
