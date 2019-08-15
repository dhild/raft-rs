use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    REST(hyper::Error),
    JSON(serde_json::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::REST(ref e) => f.write_fmt(format_args!("REST error: {}", e)),
            Error::JSON(ref e) => f.write_fmt(format_args!("JSON error: {}", e)),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::REST(_) => "REST error",
            Error::JSON(_) => "JSON error",
        }
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            Error::REST(ref e) => Some(e),
            Error::JSON(ref e) => Some(e),
        }
    }
}
