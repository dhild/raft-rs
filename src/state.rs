use crate::storage::Storage;
use crate::Consensus;
use async_trait::async_trait;

pub trait Command: Sized + Send + Sync + 'static {
    type Error: std::error::Error + Sync + Send + Sized + 'static;
    fn deserialize(data: &[u8]) -> Result<Self, Self::Error>;
    fn serialize(&self) -> Result<Vec<u8>, Self::Error>;
}

#[async_trait]
pub trait StateMachineApplier: Send + Sync + 'static {
    type Command: Command;

    async fn apply(&self, index: usize, cmd: Self::Command);
}

pub trait StateMachine<S: Storage>: Sized {
    type Applier: StateMachineApplier;

    fn build(consensus: Consensus<S>) -> (Self, Self::Applier);
}

#[cfg(feature = "kv-store")]
pub use kv::{KVCommand, KeyValueStore};

#[cfg(feature = "kv-store")]
mod kv {
    use crate::error::Result;
    use crate::state::{Command, StateMachineApplier};
    use crate::storage::Storage;
    use crate::{Consensus, StateMachine};
    use async_lock::Lock;
    use async_trait::async_trait;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    pub struct KeyValueStore<S: Storage> {
        data: Lock<HashMap<String, Bytes>>,
        consensus: Consensus<S>,
    }

    impl<S: Storage> KeyValueStore<S> {
        pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<usize> {
            let index = self
                .consensus
                .commit(KVCommand::Put {
                    key: key.into(),
                    value: value.to_vec().into(),
                })
                .await?;
            Ok(index)
        }

        pub async fn get(&self, key: &str) -> Option<Bytes> {
            let data = self.data.lock().await;
            data.get(key).cloned()
        }
    }

    impl<S: Storage> StateMachine<S> for KeyValueStore<S> {
        type Applier = KVApplier;

        fn build(consensus: Consensus<S>) -> (Self, KVApplier) {
            let lock = Lock::new(HashMap::new());
            (
                KeyValueStore {
                    data: lock.clone(),
                    consensus,
                },
                KVApplier { data: lock },
            )
        }
    }

    pub struct KVApplier {
        data: Lock<HashMap<String, Bytes>>,
    }

    #[async_trait]
    impl StateMachineApplier for KVApplier {
        type Command = KVCommand;

        async fn apply(&self, _index: usize, cmd: Self::Command) {
            let mut data = self.data.lock().await;
            match cmd {
                KVCommand::Put { key, value } => {
                    data.insert(key, value);
                }
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum KVCommand {
        Put { key: String, value: Bytes },
    }

    impl Command for KVCommand {
        type Error = serde_json::Error;

        fn deserialize(data: &[u8]) -> std::result::Result<Self, Self::Error> {
            serde_json::from_slice(data)
        }

        fn serialize(&self) -> std::result::Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(&self)
        }
    }
}
