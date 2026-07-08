//! Shared controller types: the `NodeId` alias, the KIP-853 voter `Node`
//! identity, and the `AppData`/`AppDataResponse` records carried through the
//! controller. These are the plain Crabka types the engine and reconfig
//! coordinator use.

pub use crabka_ids::NodeId;
use crabka_metadata::{MetadataRecord, VoterEndpoint};
use serde::{Deserialize, Serialize};

/// KIP-853 voter node identity used by controller membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub directory_id: uuid::Uuid,
    pub endpoints: Vec<crabka_metadata::VoterEndpoint>,
    pub kraft_version: crabka_metadata::KRaftVersionRange,
}

impl Node {
    /// The controller RPC endpoint peers dial, as a `"<host>:<port>"` string.
    /// By convention the first endpoint named "CONTROLLER"; falls back to the
    /// first endpoint.
    ///
    /// The host is returned VERBATIM (a DNS name), never pre-resolved to a
    /// `SocketAddr`: voter endpoints carry per-pod `StatefulSet` FQDNs, and the
    /// dialer re-resolves the host per connect (`TcpStream::connect`) so a peer
    /// that restarts on a new pod IP stays reachable. Parsing to a `SocketAddr`
    /// here returned `None` for any DNS hostname — the same footgun that
    /// silently broke `submit_change` leader-forwarding via
    /// `ControllerHandle::voter_addr` (now `controller_endpoint_addr` in
    /// `controller.rs`; mirrors `controller_addr` in `network.rs`).
    #[must_use]
    pub fn controller_addr(&self) -> Option<String> {
        controller_endpoint_addr(&self.endpoints)
    }
}

pub(crate) fn controller_endpoint_addr(endpoints: &[VoterEndpoint]) -> Option<String> {
    let endpoint = endpoints
        .iter()
        .find(|e| e.name == "CONTROLLER")
        .or_else(|| endpoints.first())?;
    Some(format!("{}:{}", endpoint.host, endpoint.port))
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitChangeResult {
    pub offset_reservations: Vec<OffsetReservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetReservation {
    pub topic: String,
    pub partition: i32,
    pub base_offset: i64,
    pub count: i64,
}

#[cfg(test)]
mod node_tests {
    use assert2::assert;

    use super::*;
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
        assert!(n.controller_addr() == Some("127.0.0.1:9093".to_string()));
    }

    #[test]
    fn node_controller_addr_keeps_dns_hostname_not_parsed_socketaddr() {
        // A per-pod FQDN must come back verbatim as "<host>:<port>", NOT parsed
        // to a SocketAddr (which is None for a hostname). The dialer re-resolves
        // it per connect, so a peer on a new pod IP stays reachable.
        let host = "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local";
        let n = Node {
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![crabka_metadata::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: host.into(),
                port: 9093,
            }],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        };
        assert!(n.controller_addr() == Some(format!("{host}:9093")));
    }
}
