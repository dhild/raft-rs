use crate::raft;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HealthStatus {
    pub status: Option<raft::Status>,
}
