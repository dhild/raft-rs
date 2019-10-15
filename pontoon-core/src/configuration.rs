use std::net::SocketAddr;

use crate::LogIndex;

#[derive(Debug, Clone)]
pub struct Server {
    pub id: String,
    pub address: SocketAddr,
    pub voter: bool,
}

pub enum ConfigurationChange {
    AddNonVoter(String, SocketAddr),
    DemoteVoter(String),
    RemoveServer(String),
    Promote(String),
}

pub struct Configuration {
    servers: Vec<Server>,
    majority: usize,
}

impl Configuration {
    fn new(id: String, address: SocketAddr) -> Configuration {
        Configuration {
            servers: vec![Server {
                id,
                address,
                voter: true,
            }],
            majority: 1,
        }
    }

    pub fn has_vote(&self, id: &str) -> bool {
        self.servers.iter().any(|x| id == &x.id && x.voter)
    }

    pub fn quorum_size(&self) -> usize {
        1 + (self.count_voters() / 2)
    }

    pub fn count_voters(&self) -> usize {
        self.servers.iter().filter(|x| x.voter).count()
    }

    pub fn voters(&self) -> Vec<Server> {
        self.servers.iter().filter(|x| x.voter).cloned().collect()
    }

    pub fn servers(&self) -> Vec<Server> {
        self.servers.clone()
    }

    fn updated(servers: Vec<Server>) -> Configuration {
        let majority = match servers.iter().filter(|x| x.voter).count() {
            x if x < 3 => x,
            x => (x + 1) / 2,
        };
        Configuration { servers, majority }
    }

    pub fn apply_change(&self, change: &ConfigurationChange) -> Configuration {
        match change {
            ConfigurationChange::AddNonVoter(id, addr) => {
                let mut servers = self.servers.clone();
                servers.push(Server {
                    id: id.clone(),
                    address: *addr,
                    voter: false,
                });
                Configuration {
                    servers,
                    majority: self.majority,
                }
            }
            ConfigurationChange::DemoteVoter(id) => {
                let mut servers = self.servers.clone();
                for s in servers.iter_mut() {
                    if &s.id == id {
                        s.voter = false
                    }
                }
                Self::updated(servers)
            }
            ConfigurationChange::RemoveServer(id) => {
                let mut servers = self.servers.clone();
                servers.retain(|s| &s.id != id);
                Self::updated(servers)
            }
            ConfigurationChange::Promote(id) => {
                let mut servers = self.servers.clone();
                for s in servers.iter_mut() {
                    if &s.id == id {
                        s.voter = true
                    }
                }
                Self::updated(servers)
            }
        }
    }
}

pub enum ConfigurationState {
    Committed(LogIndex, Configuration),
    Latest(LogIndex, Configuration, Configuration),
}

impl ConfigurationState {
    pub fn new(id: String, address: SocketAddr) -> ConfigurationState {
        ConfigurationState::Committed(0, Configuration::new(id, address))
    }

    pub fn latest(&self) -> &Configuration {
        match self {
            ConfigurationState::Committed(_, c) => c,
            ConfigurationState::Latest(_, _, c) => c,
        }
    }
}
