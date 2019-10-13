use std::error;
use std::fmt;
use std::io;

#[cfg(feature = "disk")]
use serde_json;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    #[cfg(feature = "disk")]
    Json(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref e) => write!(f, "I/O error: {}", e),
            #[cfg(feature = "disk")]
            Error::Json(ref e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Io(_) => "I/O error",
            #[cfg(feature = "disk")]
            Error::Json(_) => "JSON error",
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::Io(ref e) => Some(e),
            #[cfg(feature = "disk")]
            Error::Json(ref e) => Some(e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

#[cfg(feature = "disk")]
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        use serde_json::error::Category;
        match err.classify() {
            Category::Io => Error::Io(err.into()),
            Category::Syntax | Category::Data | Category::Eof => Error::Json(err),
        }
    }
}
