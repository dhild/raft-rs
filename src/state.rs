use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum Command {
    KV(KVCommand),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Query {
    KV(KVQuery),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum QueryResponse {
    KV(KVQueryResponse),
}

pub struct StateMachine {
    kv: KeyValueStore,
}

impl Default for StateMachine {
    fn default() -> Self {
        StateMachine {
            kv: KeyValueStore::default(),
        }
    }
}

impl StateMachine {
    pub fn apply<C>(&mut self, cmd: C)
    where
        C: Into<Command>,
    {
        match cmd.into() {
            Command::KV(ref cmd) => self.kv.apply(cmd),
        }
    }

    pub fn query<Q>(&self, query: Q) -> QueryResponse
    where
        Q: Into<Query>,
    {
        match query.into() {
            Query::KV(ref query) => QueryResponse::KV(self.kv.query(query)),
        }
    }
}

pub struct KeyValueStore {
    data: HashMap<String, Bytes>,
}

impl Default for KeyValueStore {
    fn default() -> Self {
        let data = HashMap::new();
        KeyValueStore { data }
    }
}

impl KeyValueStore {
    pub fn apply(&mut self, cmd: &KVCommand) {
        match cmd {
            KVCommand::Put { key, value } => {
                self.data.insert(key.to_string(), value.clone());
            }
        }
    }

    pub fn query(&self, query: &KVQuery) -> KVQueryResponse {
        match query {
            KVQuery::Get { key } => {
                let value = self.data.get(key).cloned();
                KVQueryResponse::Get { value }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum KVCommand {
    Put { key: String, value: Bytes },
}

impl Into<Command> for KVCommand {
    fn into(self) -> Command {
        Command::KV(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVQuery {
    Get { key: String },
}

impl Into<Query> for KVQuery {
    fn into(self) -> Query {
        Query::KV(self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KVQueryResponse {
    Get { value: Option<Bytes> },
}
