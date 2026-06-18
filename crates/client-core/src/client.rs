//! Top-level [`Client`]. Wraps a [`BrokerPool`] and
//! exposes a typed-request `send` API.

use std::sync::Arc;

use crate::bootstrap;
use crate::connection::ConnectionOptions;
use crate::error::ClientError;
use crate::pool::{BrokerInfo, BrokerPool};
use crate::request::ProtocolRequest;

/// A Kafka client backed by a [`BrokerPool`].
///
/// Construct via [`Client::builder`].
///
/// Cloning a `Client` is cheap — it shares the underlying [`BrokerPool`] via
/// an `Arc` and the connection options via a value clone.
#[derive(Clone)]
pub struct Client {
    pool: Arc<BrokerPool>,
    #[allow(dead_code)]
    options: ConnectionOptions,
}

#[bon::bon]
impl Client {
    /// Build a [`Client`] pointed at the given bootstrap address.
    #[builder(start_fn = builder, finish_fn = build)]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka".to_string())] client_id: String,
        #[builder(default = std::time::Duration::from_secs(30))]
        connect_timeout: std::time::Duration,
        #[builder(default = std::time::Duration::from_secs(30))]
        request_timeout: std::time::Duration,
        security: Option<crate::security::ClientSecurity>,
    ) -> Result<Self, ClientError> {
        let options = ConnectionOptions {
            client_id,
            connect_timeout,
            request_timeout,
            security: security.map(Box::new),
        };
        Self::start_with_options(bootstrap, options).await
    }
}

impl Client {
    async fn start_with_options(
        bootstrap: String,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let addrs = bootstrap::resolve(&bootstrap).await?;
        let pool = Arc::new(BrokerPool::new(addrs, options.clone()));
        Ok(Client { pool, options })
    }

    /// Send a request to the bootstrap broker (or any cached open connection).
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = self.pool.bootstrap_connection().await?;
        conn.send(req).await
    }

    /// Whether the pool knows a dialable address for `broker_id` (learned via
    /// [`refresh_metadata`](Client::refresh_metadata), port not `0`). Lets a
    /// caller choose between [`broker`](Client::broker) routing and the
    /// bootstrap [`send`](Client::send) without a speculative connect.
    #[must_use]
    pub fn knows_broker(&self, broker_id: i32) -> bool {
        self.pool.knows_broker(broker_id)
    }

    /// Return a [`BrokerHandle`] that routes requests to a specific broker by id.
    ///
    /// The broker must have been registered via [`refresh_metadata`] first.
    ///
    /// [`refresh_metadata`]: Client::refresh_metadata
    #[must_use]
    pub fn broker(&self, broker_id: i32) -> BrokerHandle<'_> {
        BrokerHandle {
            pool: &self.pool,
            broker_id,
        }
    }

    /// Drop the pooled connection to `broker_id` so the next request to it
    /// reconnects (to its current advertised address). Call this after a send
    /// fails so a bounced / failed-over broker isn't retried over a dead socket.
    pub fn evict_broker(&self, broker_id: i32) {
        self.pool.evict(broker_id);
    }

    /// Send a default `MetadataRequest`, parse the broker list from the response,
    /// refresh the pool's address registry, and return the typed response.
    pub async fn refresh_metadata(
        &self,
    ) -> Result<crabka_protocol::owned::metadata_response::MetadataResponse, ClientError> {
        use crabka_protocol::owned::metadata_request::MetadataRequest;
        let resp = match self.send(MetadataRequest::default()).await {
            Ok(resp) => resp,
            // The cached bootstrap connection's broker may have died (e.g. it was
            // the failed-over partition leader). A dead socket is never evicted by
            // `evict_broker` (that keys on real broker ids, not the bootstrap's
            // synthetic `-1`), so without this the producer/consumer would keep
            // refreshing over the same dead connection and stay pinned to a stale
            // leader forever. Drop the bootstrap connection and retry once so the
            // refresh re-resolves to a live bootstrap address.
            Err(ClientError::Timeout(_) | ClientError::Disconnected | ClientError::Io(_)) => {
                self.pool.evict_bootstrap();
                self.send(MetadataRequest::default()).await?
            }
            Err(e) => return Err(e),
        };
        let brokers: Vec<BrokerInfo> = resp
            .brokers
            .iter()
            .map(|b| BrokerInfo {
                id: b.node_id,
                host: b.host.clone(),
                port: b.port,
                rack: b.rack.clone(),
            })
            .collect();
        self.pool.refresh_brokers(&brokers).await;
        Ok(resp)
    }

    /// Send a single-partition `OffsetForLeaderEpoch` (`api_key=23`) via the
    /// bootstrap connection. Thin wrapper over the free
    /// [`offset_for_leader_epoch`](crate::offset_for_leader_epoch) helper used
    /// by the consumer's KIP-320 position-validation pass; `Client` does not
    /// otherwise expose its connection, so this borrows the same bootstrap
    /// connection `send` uses.
    ///
    /// # Errors
    /// Transport / version-negotiation failure, or a partition not present in
    /// the response.
    pub async fn offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<crate::offset_for_leader_epoch::EpochEndOffset, ClientError> {
        let conn = self.pool.bootstrap_connection().await?;
        crate::offset_for_leader_epoch::offset_for_leader_epoch(
            &conn,
            topic,
            partition,
            current_leader_epoch,
            leader_epoch,
        )
        .await
    }

    /// Send a single-partition `OffsetForLeaderEpoch` (`api_key=23`) to a
    /// *specific* broker by id, via [`BrokerPool::get`]. Mirrors
    /// [`offset_for_leader_epoch`](Client::offset_for_leader_epoch) but targets
    /// the partition leader instead of the bootstrap connection — KIP-320
    /// requires the validation RPC reach the partition leader, which is the
    /// only replica with the authoritative epoch→end-offset history.
    ///
    /// The broker must already be in the pool's registry (populated by
    /// [`refresh_metadata`](Client::refresh_metadata)).
    ///
    /// # Errors
    /// `Disconnected` if `broker_id` is not in the registry; transport /
    /// version-negotiation failure; or a partition not present in the response.
    pub async fn offset_for_leader_epoch_on(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<crate::offset_for_leader_epoch::EpochEndOffset, ClientError> {
        let conn = self.pool.get(broker_id).await?;
        crate::offset_for_leader_epoch::offset_for_leader_epoch(
            &conn,
            topic,
            partition,
            current_leader_epoch,
            leader_epoch,
        )
        .await
    }

    /// Close the client and all pooled connections.
    pub fn close(self) {
        if let Some(pool) = Arc::into_inner(self.pool) {
            pool.close_all();
        }
    }
}

/// A handle to a specific broker within a [`Client`]'s pool.
///
/// Obtained via [`Client::broker`].
pub struct BrokerHandle<'a> {
    pool: &'a BrokerPool,
    broker_id: i32,
}

impl BrokerHandle<'_> {
    /// Send a request to this specific broker.
    ///
    /// When the pool has no dialable address for `broker_id` — which happens
    /// when the broker advertises port `0` (a single-broker cluster whose
    /// OS-assigned port never got rewritten in metadata), so
    /// [`BrokerPool::refresh_brokers`] deliberately skipped it — fall back to
    /// the bootstrap connection. On such a cluster the bootstrap broker *is*
    /// this broker (e.g. the group coordinator a consumer routes to), so the
    /// request still reaches its intended target instead of failing
    /// `Disconnected`. A *known* broker whose connect fails is not masked: the
    /// fallback only triggers when the id was never in the registry.
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = match self.pool.get(self.broker_id).await {
            Ok(conn) => conn,
            Err(ClientError::Disconnected) if !self.pool.knows_broker(self.broker_id) => {
                self.pool.bootstrap_connection().await?
            }
            Err(e) => return Err(e),
        };
        conn.send(req).await
    }
}

#[cfg(test)]
mod bootstrap_failover_tests {
    use super::*;
    use crate::mock::MockBroker;
    use bytes::BytesMut;
    use crabka_protocol::Encode;
    use crabka_protocol::owned::api_versions_request;
    use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use crabka_protocol::owned::metadata_request;
    use crabka_protocol::owned::metadata_response::{
        FLEXIBLE_MIN as META_FLEXIBLE_MIN, MetadataResponse, MetadataResponseBroker,
    };

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
            ],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf, 0).unwrap();
        buf.to_vec()
    }

    fn metadata_v(version: i16, node_id: i32) -> Vec<u8> {
        let resp = MetadataResponse {
            brokers: vec![MetadataResponseBroker {
                node_id,
                host: "127.0.0.1".into(),
                port: 9092,
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

    fn handler(
        node_id: i32,
    ) -> impl FnMut(i16, i16, i32, &[u8]) -> Option<Vec<u8>> + Send + 'static {
        move |api_key, version, _corr, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(api_versions_v0());
            }
            if api_key == metadata_request::API_KEY {
                return Some(metadata_v(version, node_id));
            }
            None
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_metadata_fails_over_when_bootstrap_broker_dies() {
        // Two bootstrap brokers. The client pins its bootstrap connection to the
        // first; when that broker is killed, `refresh_metadata` must evict the
        // dead bootstrap connection and reconnect to the second live broker
        // instead of failing forever on the dead socket — the client-side cause
        // of the on-cluster producer stall where a failover whose killed leader
        // was also the bootstrap-pinned broker left the producer stranded.
        let a = MockBroker::start(handler(0)).await;
        let b = MockBroker::start(handler(1)).await;
        let bootstrap = format!("{},{}", a.addr, b.addr);
        let client = Client::builder()
            .bootstrap(bootstrap)
            .request_timeout(std::time::Duration::from_millis(500))
            .build()
            .await
            .expect("client connects to bootstrap A");

        // First refresh pins the bootstrap connection to broker A.
        client
            .refresh_metadata()
            .await
            .expect("first refresh succeeds via A");

        // Broker A is killed.
        a.stop();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The next refresh must transparently fail over to the live broker B,
        // not hang/error on the dead A socket.
        let md = client
            .refresh_metadata()
            .await
            .expect("refresh must fail over to live bootstrap B after A dies");
        assert2::assert!(!md.brokers.is_empty());
    }
}
