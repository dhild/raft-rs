//! Types for working with Finite State Machines.

use std::collections::HashMap;
use std::io;
use std::result::Result;

#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

/// A state machine which is to be eventually consistent across the raft cluster.
pub trait FiniteStateMachine: Sized + Send {
    type Error: std::error::Error + Send;
    type Command: Serializable<Self::Error>;
    type Query: Serializable<Self::Error>;
    type QueryResponse: Serializable<Self::Error>;

    /// Accepts a binary command, applying it to the state machine.
    fn command(&mut self, command: Self::Command) -> Result<(), Self::Error>;

    /// Processes a query, returning some data concerning the current state.
    fn query(&self, query: &Self::Query) -> Result<Self::QueryResponse, Self::Error>;

    /// Records the current state of the `FiniteStateMachine` so that it can be restored later.
    fn take_snapshot<W: io::Write>(&self, w: &mut W) -> Result<(), Self::Error>;

    /// Restores the `FiniteStateMachine` from a snapshot written with `take_snapshot`.
    fn restore_snapshot<R: io::Read>(&mut self, r: &mut R) -> Result<(), Self::Error>;
}

/// An object that can be written to or read from a byte stream.
pub trait Serializable<E>: Sized + Send {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: &mut R) -> Result<Self, E>;

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: &mut W) -> Result<(), E>;
}

/// A `FiniteStateMachine` which holds a set of Key-Value pairs.
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub struct KeyValueStore {
    data: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub enum KeyValueStoreCommand {
    Put(String, Vec<u8>),
    Delete(String),
}

#[cfg(feature = "json")]
impl Serializable<serde_json::Error> for KeyValueStoreCommand {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: &mut R) -> Result<Self, serde_json::Error> {
        serde_json::from_reader(reader)
    }

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: &mut W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub enum KeyValueStoreQuery {
    Get(String),
}

#[cfg(feature = "json")]
impl Serializable<serde_json::Error> for KeyValueStoreQuery {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: &mut R) -> Result<Self, serde_json::Error> {
        serde_json::from_reader(reader)
    }

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: &mut W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
pub enum KeyValueStoreQueryResponse {
    Get(Vec<u8>),
}

#[cfg(feature = "json")]
impl Serializable<serde_json::Error> for KeyValueStoreQueryResponse {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: &mut R) -> Result<Self, serde_json::Error> {
        serde_json::from_reader(reader)
    }

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: &mut W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

#[cfg(feature = "json")]
impl FiniteStateMachine for KeyValueStore {
    type Error = serde_json::Error;
    type Command = KeyValueStoreCommand;
    type Query = KeyValueStoreQuery;
    type QueryResponse = KeyValueStoreQueryResponse;

    /// Accepts a binary command, applying it to the state machine.
    fn command(&mut self, cmd: Self::Command) -> Result<(), Self::Error> {
        match cmd {
            KeyValueStoreCommand::Put(key, value) => self.data.insert(key, value),
            KeyValueStoreCommand::Delete(key) => self.data.remove(&key),
        };
        Ok(())
    }

    /// Processes a query, returning some data concerning the current state.
    fn query(&self, query: &Self::Query) -> Result<Self::QueryResponse, Self::Error> {
        match query {
            KeyValueStoreQuery::Get(key) => {
                let data = self.data.get(key).cloned().unwrap_or_default();
                Ok(KeyValueStoreQueryResponse::Get(data))
            }
        }
    }

    /// Records the current state of the `FiniteStateMachine` so that it can be restored later.
    fn take_snapshot<W: io::Write>(&self, w: &mut W) -> Result<(), Self::Error> {
        serde_json::to_writer(w, self)
    }

    /// Restores the `FiniteStateMachine` from a snapshot written with `take_snapshot`.
    fn restore_snapshot<R: io::Read>(&mut self, r: &mut R) -> Result<(), Self::Error> {
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
