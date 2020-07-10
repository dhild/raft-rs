use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub enum Command {
    #[cfg(feature = "kv-store")]
    KV(KVCommand),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Query {
    #[cfg(feature = "kv-store")]
    KV(KVQuery),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum QueryResponse {
    None,
    #[cfg(feature = "kv-store")]
    KV(KVQueryResponse),
}

pub struct StateMachine {
    #[cfg(feature = "kv-store")]
    kv: kv::KeyValueStore,
}

impl Default for StateMachine {
    fn default() -> Self {
        StateMachine {
            #[cfg(feature = "kv-store")]
            kv: kv::KeyValueStore::default(),
        }
    }
}

impl StateMachine {
    pub fn apply<C>(&mut self, cmd: C)
    where
        C: Into<Command>,
    {
        match cmd.into() {
            #[cfg(feature = "kv-store")]
            Command::KV(ref cmd) => self.kv.apply(cmd),
        }
    }

    pub fn query<Q>(&self, query: Q) -> QueryResponse
    where
        Q: Into<Query>,
    {
        match query.into() {
            #[cfg(feature = "kv-store")]
            Query::KV(ref query) => QueryResponse::KV(self.kv.query(query)),
        }
    }
}

#[cfg(feature = "kv-store")]
pub use kv::{Command as KVCommand, Query as KVQuery, QueryResponse as KVQueryResponse};

#[cfg(feature = "kv-store")]
mod kv {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

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
        pub fn apply(&mut self, cmd: &Command) {
            match cmd {
                Command::Put { key, value } => {
                    self.data.insert(key.to_string(), value.clone());
                }
            }
        }

        pub fn query(&self, query: &Query) -> QueryResponse {
            match query {
                Query::Get { key } => {
                    let value = self.data.get(key).cloned();
                    QueryResponse::Get { value }
                }
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
    pub enum Command {
        Put { key: String, value: Bytes },
    }

    impl Into<super::Command> for Command {
        fn into(self) -> super::Command {
            super::Command::KV(self)
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Query {
        Get { key: String },
    }

    impl Into<super::Query> for Query {
        fn into(self) -> super::Query {
            super::Query::KV(self)
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum QueryResponse {
        Get { value: Option<Bytes> },
    }
}
