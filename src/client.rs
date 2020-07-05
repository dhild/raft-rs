pub use crate::state::{Command, Query, QueryResponse};

struct Client {
    leader_address: String,
    max_retries: usize,
}

impl Client {
    pub fn apply(&self, cmd: Command) {
        unimplemented!()
    }
    pub fn query(&self, query: Query) -> QueryResponse {
        unimplemented!()
    }
}

#[cfg(feature = "kv-store")]
mod kv {
    struct Client {}

    impl Client {
        pub fn put(&self, key: &str, value: &[u8]) -> Option<Vec<u8>> {
            unimplemented!()
        }

        pub fn get(&self, key: &str) -> Option<Vec<u8>> {
            unimplemented!()
        }
    }
}
