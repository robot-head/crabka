use thiserror::Error;

use crate::types::NodeId;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RaftError {
    #[error("storage: {0}")]
    Storage(#[from] crabka_log::LogError),

    #[error("network: {0}")]
    Network(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("records: {0}")]
    Records(#[from] crabka_protocol::records::RecordsError),

    #[error("metadata: {0}")]
    Metadata(#[from] crabka_metadata::MetadataError),

    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    #[error("not leader; current leader: {current_leader:?}")]
    NotLeader { current_leader: Option<NodeId> },

    #[error("leader unknown (election in progress)")]
    LeaderUnknown,

    #[error("change rejected: {0}")]
    ChangeRejected(String),

    #[error("reconfiguration rejected: {0}")]
    ReconfigRejected(String),

    #[error("a reconfiguration is already in progress")]
    ReconfigInProgress,

    #[error("voter {id} is not a caught-up observer (lag {lag})")]
    VoterNotCaughtUp { id: NodeId, lag: u64 },

    #[error("serialization: {0}")]
    SerdeFailed(#[from] wincode::error::WriteError),

    #[error("deserialization: {0}")]
    SerdeFailedDecode(#[from] wincode::error::ReadError),

    #[error("startup misconfiguration: {0}")]
    Startup(String),

    #[error("controller shut down")]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn display_not_leader_with_id() {
        let e = RaftError::NotLeader {
            current_leader: Some(7),
        };
        assert!(e.to_string().contains("Some(7)"));
    }

    #[test]
    fn display_not_leader_without_id() {
        let e = RaftError::NotLeader {
            current_leader: None,
        };
        assert!(e.to_string().contains("None"));
    }
}
