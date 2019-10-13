use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone)]
pub struct RaftConfig {
    pub server_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            server_addr: ([127, 0, 0, 1], 8080).into(),
            peers: Vec::new(),
        }
    }
}

impl RaftConfig {
    pub fn election_timeout(&self) -> Duration {
        Duration::from_millis(150)
    }
}
