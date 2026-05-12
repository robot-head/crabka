//! openraft `TypeConfig` for Crabka. Single source of truth for the
//! generic parameter set every adapter uses.

use serde::{Deserialize, Serialize};

use crabka_metadata::MetadataRecord;

pub type NodeId = u64;

/// `BasicNode` from openraft carries the network address. We use it
/// directly rather than wrapping.
pub type Node = openraft::BasicNode;

/// What we ask Raft to replicate. A batch of `MetadataRecord`s so
/// `submit_change` can group related records (Topic + N Partitions)
/// in a single committed entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppData {
    pub records: Vec<MetadataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AppDataResponse {
    /// Filled in by the state machine on apply; carries the new log
    /// index so callers can correlate.
    pub applied_index: u64,
    /// Records that failed `MetadataImage::validate` at apply-time and
    /// were skipped. Carries the validation error message in order of
    /// rejection. `submit_change` translates a non-empty list into
    /// `RaftError::Metadata` so a concurrent `CreateTopics` race ends
    /// with one winner + one `TopicExists` per loser, rather than
    /// silently committing every duplicate.
    pub rejected: Vec<String>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppData,
        R = AppDataResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Re-export the openraft-derived `Raft` alias so adapters can name it
/// without re-stating the type config.
pub type Raft = openraft::Raft<TypeConfig>;
