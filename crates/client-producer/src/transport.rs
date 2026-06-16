//! Transport seam for the background sender.
//!
//! The sender needs only a narrow slice of [`crabka_client_core::Client`]:
//! ship a single-partition `ProduceRequest` to a partition leader (or the
//! bootstrap connection when the leader is unknown), evict a dead broker
//! connection, ask whether a broker id is dialable, and refresh cluster
//! metadata. [`ProduceTransport`] captures exactly that surface so tests can
//! drive the sender against an in-process broker model
//! ([`MockTransport`](#tests)) with no socket — letting us reproduce
//! idempotent-sequencing hangs deterministically.
//!
//! [`ClientTransport`] is the thin production adapter over a real `Client`.

use async_trait::async_trait;

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::metadata_response::MetadataResponse;
use crabka_protocol::owned::produce_request::ProduceRequest;
use crabka_protocol::owned::produce_response::ProduceResponse;

/// The broker-facing operations the sender performs.
///
/// `send_produce` folds the `Client::broker(id).send` / bootstrap `Client::send`
/// distinction into one call: `leader = Some(id)` routes to that broker, `None`
/// uses the bootstrap connection. This mirrors the sender's existing
/// `BOOTSTRAP_LEADER` fallback while keeping the trait a clean, testable seam.
///
/// `async_trait` is used for dyn-compatibility so `SenderConfig` can hold a
/// `Box<dyn ProduceTransport>` without leaking generics across the whole crate.
#[async_trait]
pub(crate) trait ProduceTransport: Send + Sync {
    /// Send a single-partition `ProduceRequest` to `leader` (a broker id), or to
    /// the bootstrap connection when `leader` is `None`.
    async fn send_produce(
        &self,
        leader: Option<i32>,
        req: ProduceRequest,
    ) -> Result<ProduceResponse, ClientError>;

    /// Drop any pooled connection to `broker_id` so the next send reconnects.
    /// No-op for the bootstrap connection.
    fn evict_broker(&self, broker_id: i32);

    /// Whether the transport has a dialable address for `broker_id`.
    fn knows_broker(&self, broker_id: i32) -> bool;

    /// Refresh cluster metadata, re-populating the broker-address registry, and
    /// return the typed response so the sender can update its leader map.
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

    fn evict_broker(&self, broker_id: i32) {
        self.client.evict_broker(broker_id);
    }

    fn knows_broker(&self, broker_id: i32) -> bool {
        self.client.knows_broker(broker_id)
    }

    async fn refresh_metadata(&self) -> Result<MetadataResponse, ClientError> {
        self.client.refresh_metadata().await
    }
}
