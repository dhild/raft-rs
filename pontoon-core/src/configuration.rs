use std::net::SocketAddr;

use crate::LogIndex;

pub type ServerId = String;

#[derive(Debug, Clone)]
pub struct Server {
    pub id: ServerId,
    pub address: SocketAddr,
    pub voter: bool,
}

pub enum ConfigurationChange {
    AddNonVoter(ServerId, SocketAddr),
    DemoteVoter(ServerId),
    RemoveServer(ServerId),
    Promote(ServerId),
}

pub struct Configuration {
    servers: Vec<Server>,
    majority: usize,
}

impl Configuration {
    pub fn has_vote(&self, id: &ServerId) -> bool {
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

    fn majority(servers: &Vec<Server>) -> usize {
        match servers.iter().filter(|x| x.voter).count() {
            x if x < 3 => x,
            x => (x + 1) / 2,
        }
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
                let majority = Self::majority(&servers);
                Configuration { servers, majority }
            }
            ConfigurationChange::RemoveServer(id) => {
                let mut servers = self.servers.clone();
                servers.retain(|s| &s.id != id);
                let majority = Self::majority(&servers);
                Configuration { servers, majority }
            }
            ConfigurationChange::Promote(id) => {
                let mut servers = self.servers.clone();
                for s in servers.iter_mut() {
                    if &s.id == id {
                        s.voter = true
                    }
                }
                let majority = Self::majority(&servers);
                Configuration { servers, majority }
            }
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            servers: Vec::new(),
            majority: 0,
        }
    }
}

pub enum ConfigurationState {
    Committed(LogIndex, Configuration),
    Latest(LogIndex, Configuration, Configuration),
}

impl ConfigurationState {
    pub fn latest(&self) -> &Configuration {
        match self {
            ConfigurationState::Committed(_, c) => c,
            ConfigurationState::Latest(_, _, c) => c,
        }
    }
}

impl Default for ConfigurationState {
    fn default() -> Self {
        ConfigurationState::Committed(0, Configuration::default())
    }
}
