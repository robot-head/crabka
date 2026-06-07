//! Real KIP-595 [`PeerSender`] over Kafka TCP framing, using the existing
//! [`crabka_client_core::Connection`]. One cached connection per peer.
//!
//! The engine's transport seam ([`crate::kraft::transport::PeerSender`]) hands
//! this an already-encoded KIP-595 request *body* plus the destination peer +
//! api key; the impl resolves the peer's controller endpoint, dials (TLS/SASL
//! terminating in the injected [`OutboundDialer`]), and issues a
//! `raw_request(api_key, version, body)`. `raw_request` builds the v2
//! `RequestHeader` and strips the v1 `ResponseHeader`, so the returned bytes are the
//! bare response body the engine decodes back into a `Receive*Response` event.
//!
//! Peer addresses are resolved from the static voter set's CONTROLLER
//! endpoints.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;

use crabka_client_core::{ClientError, Connection, ConnectionOptions};
use crabka_metadata::voters::VoterSet;

use crate::error::RaftError;
use crate::kraft::transport::{PeerSender, api_key};
use crate::kraft::types::NodeId;

/// Outbound dialer the controller hands to the peer sender.
///
/// `crabka-raft` cannot depend on `crabka-broker` (that would be a cycle), so
/// the broker provides an impl wrapping its `InterBrokerClient` (TLS + SASL)
/// and injects it via [`ControllerConfig::dialer`](crate::ControllerConfig).
/// When no dialer is injected, the controller falls back to a plain
/// `Connection::connect(addr)` — the PLAINTEXT path.
#[async_trait]
pub trait OutboundDialer: Send + Sync {
    /// Open a `Connection` to the raft peer `target` reachable on `addr`. The
    /// returned connection has already negotiated `ApiVersions` and is usable
    /// for `raw_request` immediately.
    async fn dial(
        &self,
        target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError>;
}

/// Default no-op dialer: opens a raw `TcpStream` via `Connection::connect`.
/// Used when the broker hasn't injected an `InterBrokerClient`-backed dialer
/// (legacy PLAINTEXT path).
pub struct PlaintextDialer;

#[async_trait]
impl OutboundDialer for PlaintextDialer {
    async fn dial(
        &self,
        _target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError> {
        let sock: SocketAddr = addr.parse().map_err(|e: std::net::AddrParseError| {
            ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid raft peer address {addr:?}: {e}"),
            ))
        })?;
        Connection::connect(sock, options).await
    }
}

/// Resolve a voter's controller-listener address from the voter set. By
/// convention the endpoint named `CONTROLLER`, falling back to the first.
fn controller_addr(voters: &VoterSet, id: NodeId) -> Option<String> {
    let voter = voters.get(id)?;
    let endpoint = voter
        .endpoints
        .iter()
        .find(|e| e.name == "CONTROLLER")
        .or_else(|| voter.endpoints.first())?;
    Some(format!("{}:{}", endpoint.host, endpoint.port))
}

/// KIP-595 api version per api key, matching the bodies the engine's transport
/// codec produces (Vote v2, Begin/End `QuorumEpoch` v1, Fetch v17).
fn api_version_for(key: i16) -> i16 {
    match key {
        api_key::VOTE => 2,
        api_key::BEGIN_QUORUM_EPOCH | api_key::END_QUORUM_EPOCH | api_key::FETCH_SNAPSHOT => 1,
        api_key::FETCH => 17,
        _ => 0,
    }
}

/// Real [`PeerSender`]: dials each voter's controller listener and issues the
/// KIP-595 RPC over [`crabka_client_core::Connection::raw_request`]. Caches one
/// connection per peer; a failed RPC evicts the cached connection so the next
/// send redials.
pub(crate) struct RealPeerSender {
    connections: DashMap<NodeId, Arc<Connection>>,
    voters: VoterSet,
    client_id: String,
    dialer: Arc<dyn OutboundDialer>,
}

impl RealPeerSender {
    pub(crate) fn new(
        voters: VoterSet,
        client_id: String,
        dialer: Arc<dyn OutboundDialer>,
    ) -> Self {
        Self {
            connections: DashMap::new(),
            voters,
            client_id,
            dialer,
        }
    }

    /// Look up or open a connection to `peer`.
    async fn connect(&self, peer: NodeId) -> Result<Arc<Connection>, RaftError> {
        if let Some(c) = self.connections.get(&peer) {
            return Ok(c.value().clone());
        }
        let addr = controller_addr(&self.voters, peer).ok_or(RaftError::NotLeader {
            current_leader: None,
        })?;
        let opts = ConnectionOptions {
            client_id: self.client_id.clone(),
            ..ConnectionOptions::default()
        };
        let conn = Arc::new(self.dialer.dial(peer, &addr, opts).await?);
        self.connections.insert(peer, conn.clone());
        Ok(conn)
    }
}

#[async_trait]
impl PeerSender for RealPeerSender {
    async fn send(&self, peer: NodeId, key: i16, body: Bytes) -> Result<Bytes, RaftError> {
        let conn = self.connect(peer).await?;
        let version = api_version_for(key);
        match conn.raw_request(key, version, body).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop the cached connection on any transport error so the next
                // send redials a fresh socket (a crashed/restarted peer).
                self.connections.remove(&peer);
                Err(RaftError::Network(e))
            }
        }
    }
}
