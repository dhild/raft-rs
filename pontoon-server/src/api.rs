use crate::raft;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Health {
    pub raft: raft::Status,
}
