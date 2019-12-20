use super::{FiniteStateMachine, Serializable};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::result::Result;

/// A `FiniteStateMachine` which holds a set of Key-Value pairs.
#[derive(Serialize, Deserialize)]
pub struct KeyValueStore {
    data: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyValueStoreCommand {
    Put(String, Vec<u8>),
    Delete(String),
}

impl Serializable<serde_json::Error> for KeyValueStoreCommand {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: R) -> Result<Self, serde_json::Error> {
        serde_json::from_reader(reader)
    }

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyValueStoreQuery {
    Get(String),
}

impl Serializable<serde_json::Error> for KeyValueStoreQuery {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: R) -> Result<Self, serde_json::Error> {
        serde_json::from_reader(reader)
    }

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyValueStoreQueryResponse {
    Get(Vec<u8>),
}

impl Serializable<serde_json::Error> for KeyValueStoreQueryResponse {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: R) -> Result<Self, serde_json::Error> {
        serde_json::from_reader(reader)
    }

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

impl FiniteStateMachine for KeyValueStore {
    type Error = serde_json::Error;
    type Command = KeyValueStoreCommand;
    type Query = KeyValueStoreQuery;
    type QueryResponse = KeyValueStoreQueryResponse;

    /// Accepts a binary command, applying it to the state machine.
    fn command(&mut self, cmd: Self::Command) {
        match cmd {
            KeyValueStoreCommand::Put(key, value) => self.data.insert(key, value),
            KeyValueStoreCommand::Delete(key) => self.data.remove(&key),
        };
    }

    /// Processes a query, returning some data concerning the current state.
    fn query(&self, query: Self::Query) -> Result<Self::QueryResponse, Self::Error> {
        match query {
            KeyValueStoreQuery::Get(key) => {
                let data = self.data.get(&key).cloned().unwrap_or_default();
                Ok(KeyValueStoreQueryResponse::Get(data))
            }
        }
    }

    /// Records the current state of the `FiniteStateMachine` so that it can be restored later.
    fn take_snapshot<W: io::Write>(&self, w: W) -> Result<(), Self::Error> {
        serde_json::to_writer(w, self)
    }

    /// Restores the `FiniteStateMachine` from a snapshot written with `take_snapshot`.
    fn restore_snapshot<R: io::Read>(&mut self, r: R) -> Result<(), Self::Error> {
        let res: KeyValueStore = serde_json::from_reader(r)?;
        self.data = res.data;
        Ok(())
    }
}

impl Default for KeyValueStore {
    fn default() -> Self {
        KeyValueStore {
            data: HashMap::default(),
        }
    }
}
