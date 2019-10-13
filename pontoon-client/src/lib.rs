mod error;
pub use crate::server::api::*;
pub use error::{Error, Result};

use crate::raft::ServerId;
use http_req::request::{Method, Request};
use http_req::uri::Uri;

pub struct Client {}

impl Default for Client {
    fn default() -> Self {
        Self {}
    }
}

impl Client {
    pub fn health(&mut self, id: ServerId) -> Result<Health> {
        let uri: Uri = format!("http://{}/status", id).parse()?;
        trace!("Sending GET to {}", uri);

        let mut writer = Vec::new();
        let response = Request::new(&uri).method(Method::GET).send(&mut writer)?;

        let val = serde_json::from_slice(&writer)?;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
