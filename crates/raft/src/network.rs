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
    #[tracing::instrument(level = "debug", skip_all, fields(target = _target, addr), err)]
    async fn dial(
        &self,
        _target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError> {
        // Re-resolve `addr` (a `<host>:<port>`) on every dial. A `StatefulSet`
        // peer that restarts keeps its stable DNS name but gets a fresh pod IP;
        // resolving here (rather than once at startup) reaches the new IP.
        // `lookup_host` also accepts a literal `ip:port` (returns it verbatim),
        // so this stays correct for IP-form addresses.
        let sock: SocketAddr = tokio::net::lookup_host(addr)
            .await
            .map_err(ClientError::Io)?
            .next()
            .ok_or_else(|| {
                ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("raft peer address {addr:?} resolved to no addresses"),
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
    #[tracing::instrument(level = "debug", skip_all, fields(peer), err)]
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
    #[tracing::instrument(level = "debug", skip_all, fields(peer, api_key = key), err)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::BufMut;
    use crabka_protocol::Encode;
    use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn voter_set_with_controller(id: NodeId, host: &str, port: u16) -> VoterSet {
        VoterSet::from_voters([crabka_metadata::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![crabka_metadata::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: host.into(),
                port,
            }],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }])
    }

    fn api_versions_response_v0() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: api_key::VOTE,
                    min_version: 0,
                    max_version: 2,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    async fn read_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        stream.read_exact(&mut frame).await.unwrap();
        frame
    }

    async fn write_response_frame(
        stream: &mut tokio::net::TcpStream,
        correlation_id: i32,
        tagged_fields: bool,
        body: &[u8],
    ) {
        let mut frame = bytes::BytesMut::new();
        frame.put_i32(correlation_id);
        if tagged_fields {
            frame.put_u8(0);
        }
        frame.put_slice(body);

        let mut out = Vec::with_capacity(frame.len() + 4);
        out.extend_from_slice(&(u32::try_from(frame.len()).unwrap()).to_be_bytes());
        out.extend_from_slice(&frame);
        stream.write_all(&out).await.unwrap();
        stream.flush().await.unwrap();
    }

    fn parse_request_header(frame: &[u8]) -> (i16, i16, i32, String, &[u8]) {
        assert!(frame.len() >= 10);
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let version = i16::from_be_bytes([frame[2], frame[3]]);
        let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
        let client_len = i16::from_be_bytes([frame[8], frame[9]]);
        assert!(client_len >= 0);
        let client_start = 10;
        let client_end = client_start + usize::try_from(client_len).unwrap();
        let client_id = std::str::from_utf8(&frame[client_start..client_end])
            .unwrap()
            .to_string();
        let body_start = if frame.get(client_end) == Some(&0) {
            client_end + 1
        } else {
            client_end
        };
        (
            api_key,
            version,
            correlation_id,
            client_id,
            &frame[body_start..],
        )
    }

    #[test]
    fn controller_addr_prefers_controller_endpoint_and_reports_unknown_voter() {
        let voters = VoterSet::from_voters([crabka_metadata::Voter {
            id: 7,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![
                crabka_metadata::VoterEndpoint {
                    name: "REPLICATION".into(),
                    host: "replication-host".into(),
                    port: 9092,
                },
                crabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "controller-host".into(),
                    port: 9093,
                },
            ],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }]);

        assert!(controller_addr(&voters, 7) == Some("controller-host:9093".to_string()));
        assert!(controller_addr(&voters, 8).is_none());
    }

    #[test]
    fn controller_addr_falls_back_to_first_endpoint() {
        let voters = VoterSet::from_voters([crabka_metadata::Voter {
            id: 7,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![crabka_metadata::VoterEndpoint {
                name: "PLAINTEXT".into(),
                host: "only-host".into(),
                port: 9094,
            }],
            kraft_version: crabka_metadata::KRaftVersionRange::default(),
        }]);

        assert!(controller_addr(&voters, 7) == Some("only-host:9094".to_string()));
    }

    #[test]
    fn api_version_for_matches_kip595_codecs() {
        assert!(api_version_for(api_key::VOTE) == 2);
        assert!(api_version_for(api_key::BEGIN_QUORUM_EPOCH) == 1);
        assert!(api_version_for(api_key::END_QUORUM_EPOCH) == 1);
        assert!(api_version_for(api_key::FETCH_SNAPSHOT) == 1);
        assert!(api_version_for(api_key::FETCH) == 17);
        assert!(api_version_for(-123) == 0);
    }

    #[tokio::test]
    async fn real_peer_sender_sends_expected_api_version_client_id_and_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (observed_tx, mut observed_rx) = tokio::sync::mpsc::channel(1);

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            let api_versions = read_frame(&mut stream).await;
            let (key, _version, corr, client_id, _body) = parse_request_header(&api_versions);
            assert!(key == 18);
            assert!(client_id == "raft-client");
            write_response_frame(&mut stream, corr, false, &api_versions_response_v0()).await;

            let request = read_frame(&mut stream).await;
            let (key, version, corr, client_id, body) = parse_request_header(&request);
            observed_tx
                .send((key, version, client_id, bytes::Bytes::copy_from_slice(body)))
                .await
                .unwrap();
            write_response_frame(&mut stream, corr, true, b"raft-response").await;
        });

        let voters = voter_set_with_controller(2, &addr.ip().to_string(), addr.port());
        let sender = RealPeerSender::new(voters, "raft-client".into(), Arc::new(PlaintextDialer));
        let response = sender
            .send(2, api_key::VOTE, Bytes::from_static(b"vote-body"))
            .await
            .expect("send");

        assert!(response == Bytes::from_static(b"raft-response"));
        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), observed_rx.recv())
            .await
            .expect("server observed request")
            .expect("server sent request details");
        assert!(observed.0 == api_key::VOTE);
        assert!(observed.1 == 2);
        assert!(observed.2 == "raft-client");
        assert!(observed.3 == Bytes::from_static(b"vote-body"));

        server.await.unwrap();
    }
}
