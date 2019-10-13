extern crate pontoon_core;
#[cfg(feature = "disk")]
extern crate serde;
#[cfg(feature = "disk")]
extern crate serde_json;

mod error;
pub use error::{Error, Result};

mod memory;
pub use memory::InMemoryStorage;
