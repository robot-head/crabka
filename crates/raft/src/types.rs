//! Shared controller types: the `NodeId` alias, the KIP-853 voter `Node`
//! identity, and the `AppData`/`AppDataResponse` records carried through the
//! controller. These are the plain Crabka types the engine and reconfig
//! coordinator use.

use serde::{Deserialize, Serialize};

use crabka_metadata::MetadataRecord;

pub type NodeId = u64;

/// KIP-853 voter node identity used by controller membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub directory_id: uuid::Uuid,
    pub endpoints: Vec<crabka_metadata::VoterEndpoint>,
    pub kraft_version: crabka_metadata::KRaftVersionRange,
}

impl Node {
    /// The controller RPC endpoint peers dial. By convention the first
    /// endpoint named "CONTROLLER"; falls back to the first endpoint.
    #[must_use]
    pub fn controller_addr(&self) -> Option<std::net::SocketAddr> {
        let endpoint = self
            .endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .or_else(|| self.endpoints.first())?;
        match format!("{}:{}", endpoint.host, endpoint.port).parse() {
            Ok(addr) => Some(addr),
            Err(error) => {
                tracing::warn!(
                    endpoint = %endpoint.name,
                    host = %endpoint.host,
                    port = endpoint.port,
                    %error,
                    "controller endpoint host:port failed to parse as a SocketAddr"
                );
                None
            }
        }
    }
}

/// What we ask Raft to replicate. A batch of `MetadataRecord`s so
/// `submit_change` can group related records (Topic + N Partitions)
/// in a single committed entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[cfg(test)]
mod node_tests {
    use super::*;
    use assert2::assert;
    #[test]
    fn node_controller_addr_prefers_controller_listener() {
        let n = Node {
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![
                crabka_metadata::VoterEndpoint {
                    name: "PLAINTEXT".into(),
                    host: "127.0.0.1".into(),
                    port: 9092,
                },
                crabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "127.0.0.1".into(),
                    port: 9093,
                },
            ],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        };
        assert!(n.controller_addr().unwrap().port() == 9093);
    }
}
