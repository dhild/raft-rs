//! Types for working with Finite State Machines.

#[cfg(feature = "key-value-store")]
mod kvs;
#[cfg(feature = "key-value-store")]
pub use kvs::*;

use std::io;
use std::result::Result;

/// A state machine which is to be eventually consistent across the raft cluster.
pub trait FiniteStateMachine: Sized + Send {
    type Error: std::error::Error + Send;
    type Command: Serializable<Self::Error>;
    type Query: Serializable<Self::Error>;
    type QueryResponse: Serializable<Self::Error>;

    /// Accepts a binary command, applying it to the state machine.
    fn command(&mut self, command: Self::Command);

    /// Processes a query, returning some data concerning the current state.
    fn query(&self, query: Self::Query) -> Result<Self::QueryResponse, Self::Error>;

    /// Records the current state of the `FiniteStateMachine` so that it can be restored later.
    fn take_snapshot<W: io::Write>(&self, w: W) -> Result<(), Self::Error>;

    /// Restores the `FiniteStateMachine` from a snapshot written with `take_snapshot`.
    fn restore_snapshot<R: io::Read>(&mut self, r: R) -> Result<(), Self::Error>;
}

/// An object that can be written to or read from a byte stream.
pub trait Serializable<E>: Sized + Send {
    /// Reads a command from the given byte stream.
    fn deserialize<R: io::Read>(reader: R) -> Result<Self, E>;

    /// Writes a command to the given byte stream.
    fn serialize<W: io::Write>(&self, writer: W) -> Result<(), E>;
}
