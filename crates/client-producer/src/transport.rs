//! Transport seam for the background sender.
//!
//! The sender needs only a narrow slice of [`crabka_client_core::Client`]. It
//! ships a single-partition `ProduceRequest` to a partition leader, or to the
//! bootstrap connection when the leader is unknown. It evicts a dead broker
//! connection, asks whether a broker id is dialable, and refreshes cluster
//! metadata. [`ProduceTransport`] captures exactly that surface, so tests can
//! drive the sender against an in-process broker model,
//! [`MockTransport`](#tests), with no socket. That makes idempotent-sequencing
//! hangs reproducible.
//!
//! [`ClientTransport`] is the thin production adapter over a real `Client`.

use async_trait::async_trait;
use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::{
    metadata_response::MetadataResponse, produce_request::ProduceRequest,
    produce_response::ProduceResponse,
};

/// The broker-facing operations the sender performs.
///
/// `send_produce` folds the difference between `Client::broker(id).send` and
/// the bootstrap `Client::send` into one call. `leader = Some(id)` routes to
/// that broker, and `None` uses the bootstrap connection. This mirrors the
/// sender's existing `BOOTSTRAP_LEADER` fallback, and it keeps the trait a
/// clean, testable seam.
///
/// The trait uses `async_trait` for dyn-compatibility, so `SenderConfig` can
/// hold a `Box<dyn ProduceTransport>` without leaking generics across the whole
/// crate.
#[async_trait]
pub(crate) trait ProduceTransport: Send + Sync {
    /// Send a single-partition `ProduceRequest` to `leader`, which is a broker
    /// id, or to the bootstrap connection when `leader` is `None`.
    async fn send_produce(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<ProduceResponse, ClientError>;

    /// Enqueue Produce `acks=0`, for which the broker sends no response.
    async fn send_produce_no_response(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<(), ClientError> {
        self.send_produce(leader, req).await.map(drop)
    }

    /// Drop any pooled connection to `broker_id` so the next send reconnects.
    /// No-op for the bootstrap connection.
    fn evict_broker(&self, broker_id: i32);

    /// Whether the transport has a dialable address for `broker_id`.
    fn knows_broker(&self, broker_id: i32) -> bool;

    /// Refresh cluster metadata, refill the broker-address registry, and
    /// return the typed response, so the sender can update its leader map.
    async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError>;
}

/// Production [`ProduceTransport`] backed by a real [`Client`].
pub(crate) struct ClientTransport {
    client: Client,
}

impl ClientTransport {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ProduceTransport for ClientTransport {
    #[tracing::instrument(level = "debug", skip_all, fields(leader = ?leader), err)]
    async fn send_produce(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<ProduceResponse, ClientError> {
        match leader {
            Some(id) => self.client.broker(id).send(req).await,
            None => self.client.send(req).await,
        }
    }

    async fn send_produce_no_response(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<(), ClientError> {
        match leader {
            Some(id) => self.client.broker(id).send_no_response(req).await,
            None => self.client.send_no_response(req).await,
        }
    }

    fn evict_broker(&self, broker_id: i32) {
        self.client.evict_broker(broker_id);
    }

    fn knows_broker(&self, broker_id: i32) -> bool {
        self.client.knows_broker(broker_id)
    }

    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
        self.client.refresh_metadata().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    };

    use bytes::BytesMut;
    use crabka_client_core::{Client, MockBroker};
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request,
            metadata_response::{
                FLEXIBLE_MIN as META_FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
            },
            produce_request::ProduceRequest,
            produce_response::{
                self, FLEXIBLE_MIN as PROD_FLEXIBLE_MIN, PartitionProduceResponse, ProduceResponse,
                TopicProduceResponse,
            },
        },
    };

    use super::*;

    /// `ApiVersionsResponse` (header v0) advertising `ApiVersions`, `Metadata`,
    /// and Produce so the client can negotiate all three against the mock.
    fn api_versions_v0() -> Vec<u8> {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion {
                    api_key: api_versions_request::API_KEY,
                    min_version: 0,
                    max_version: 3,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: metadata_request::API_KEY,
                    min_version: 0,
                    max_version: 12,
                    ..Default::default()
                },
                ApiVersion {
                    api_key: produce_response::API_KEY,
                    min_version: 0,
                    max_version: 9,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    /// `MetadataResponse` advertising one broker (id 1) at the mock's own port,
    /// encoded at `version` with the correct `ResponseHeader` prefix.
    fn metadata_v(version: i16, port: u16) -> Vec<u8> {
        let resp = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: i32::from(port),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= META_FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00u8]); // empty tagged fields
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    /// A non-default `ProduceResponse` with one topic and partition, encoded
    /// at `version` with the correct `ResponseHeader` prefix.
    fn produce_v(version: i16) -> Vec<u8> {
        let resp = ProduceResponse {
            responses: vec![TopicProduceResponse {
                name: "t".into(),
                partition_responses: vec![PartitionProduceResponse {
                    index: 0,
                    error_code: 0,
                    base_offset: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        if version >= PROD_FLEXIBLE_MIN {
            buf.extend_from_slice(&[0x00u8]); // empty tagged fields
        }
        resp.encode(&mut buf, version).unwrap();
        buf.to_vec()
    }

    /// `ClientTransport` forwards each operation to the underlying `Client`. A
    /// real in-process broker confirms that the delegations are live.
    /// `refresh_metadata` and `send_produce` return the broker's data rather
    /// than a default, `knows_broker` reflects the pool, and `evict_broker`
    /// drops the cached connection so that the next send re-handshakes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_transport_delegates_to_client() {
        // One ApiVersions handshake happens per new TCP connection, so this
        // counts connections established to the mock.
        let handshakes = Arc::new(AtomicUsize::new(0));
        let port = Arc::new(AtomicU16::new(0));
        let h_handshakes = handshakes.clone();
        let h_port = port.clone();

        let mock = MockBroker::start(move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                h_handshakes.fetch_add(1, Ordering::SeqCst);
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                return Some(metadata_v(version, h_port.load(Ordering::SeqCst)));
            }
            if api_key == produce_response::API_KEY {
                return Some(produce_v(version));
            }
            None
        })
        .await;
        port.store(mock.addr.port(), Ordering::SeqCst);

        let client = Client::builder()
            .bootstrap(mock.addr.to_string())
            .build()
            .await
            .expect("client connects to the mock");
        let transport = ClientTransport::new(client);

        // refresh_metadata returns the live broker list (a default would be empty).
        let md = transport
            .refresh_metadata()
            .await
            .expect("refresh_metadata");
        assert2::assert!(!md.brokers.is_empty());

        // knows_broker reflects the pool: broker 1 is registered, 999 is not.
        assert2::assert!(transport.knows_broker(1));
        assert2::assert!(!transport.knows_broker(999));

        // send_produce returns the broker's real response (a default is empty) and
        // caches a connection to broker 1.
        let resp = transport
            .send_produce(Some(1), ProduceRequest::default())
            .await
            .expect("send_produce to broker 1");
        assert2::assert!(!resp.responses.is_empty());

        // evict_broker drops the cached connection, so the next send must open a
        // fresh one — observable as another handshake.
        let before = handshakes.load(Ordering::SeqCst);
        transport.evict_broker(1);
        let _ = transport
            .send_produce(Some(1), ProduceRequest::default())
            .await
            .expect("send_produce after evict reconnects");
        let after = handshakes.load(Ordering::SeqCst);
        assert2::assert!(after > before);

        mock.stop();
    }
}
