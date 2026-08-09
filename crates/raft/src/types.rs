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
    /// The controller RPC endpoint that peers dial, as a `"<host>:<port>"`
    /// string.
    ///
    /// By convention this is the first endpoint named "CONTROLLER". If there is
    /// no such endpoint, this method falls back to the first endpoint.
    ///
    /// This method returns the host VERBATIM, as a DNS name, and never
    /// pre-resolves it to a `SocketAddr`. Voter endpoints carry per-pod
    /// `StatefulSet` FQDNs, and the dialer re-resolves the host on each connect
    /// through `TcpStream::connect`, so a peer that restarts on a new pod IP
    /// stays reachable. A parse to a `SocketAddr` here gives `None` for any DNS
    /// hostname, and that silently breaks `submit_change` leader-forwarding
    /// through `ControllerHandle::voter_addr`. That function is now
    /// `controller_endpoint_addr` in `controller.rs`, and it mirrors
    /// `controller_addr` in `network.rs`.
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

/// What the controller asks Raft to replicate.
///
/// This is a batch of `MetadataRecord`s, so `submit_change` can group related
/// records, such as one Topic and N Partitions, in a single committed entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppData {
    pub records: Vec<MetadataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AppDataResponse {
    /// The state machine fills this in on apply. It carries the new log index,
    /// so callers can correlate.
    pub applied_index: u64,
    /// Records that failed `MetadataImage::validate` at apply time and were
    /// skipped. This field carries the validation error message in order of
    /// rejection. `submit_change` translates a non-empty list into
    /// `RaftError::Metadata`, so a concurrent `CreateTopics` race ends with one
    /// winner and one `TopicExists` for each loser, and not with a silent
    /// commit of every duplicate.
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
        assert2::assert!(n.controller_addr() == Some("127.0.0.1:9093".to_string()));
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
        assert2::assert!(n.controller_addr() == Some(format!("{host}:9093")));
    }
}
