use crate::error::Result;
use crate::protocol::Peer;
use crate::storage::Command;
use crate::Consensus;
use async_lock::Lock;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

pub struct RaftConfig {
    pub id: String,
    pub peers: Vec<Peer>,
    pub timeout: Duration,
}

pub struct KeyValueStore {
    data: Lock<HashMap<String, Bytes>>,
    consensus: Consensus<KVCommand>,
}

#[async_trait::async_trait]
pub trait StateMachineApplier: Send + Sync {
    type Command: Command;

    async fn apply(&self, index: usize, cmd: Self::Command);
}

pub trait StateMachine: Sized {
    type Command: Command;
    type Applier: StateMachineApplier<Command = Self::Command>;

    fn build(consensus: Consensus<Self::Command>) -> (Self, Self::Applier);
}

impl KeyValueStore {
    pub async fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.consensus
            .commit(KVCommand::Put {
                key: key.into(),
                value: value.to_vec().into(),
            })
            .await?;

        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        let data = self.data.lock().await;
        data.get(key).cloned()
    }
}

impl StateMachine for KeyValueStore {
    type Command = KVCommand;
    type Applier = KVApplier;

    fn build(consensus: Consensus<Self::Command>) -> (Self, KVApplier) {
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

struct KVApplier {
    data: Lock<HashMap<String, Bytes>>,
}

#[async_trait::async_trait]
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
enum KVCommand {
    Put { key: String, value: Bytes },
}

impl Command for KVCommand {}
