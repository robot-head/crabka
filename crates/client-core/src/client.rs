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

    /// Send a default `MetadataRequest`, parse the broker list from the response,
    /// refresh the pool's address registry, and return the typed response.
    pub async fn refresh_metadata(
        &self,
    ) -> Result<crabka_protocol::owned::metadata_response::MetadataResponse, ClientError> {
        use crabka_protocol::owned::metadata_request::MetadataRequest;
        let resp = self.send(MetadataRequest::default()).await?;
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
        self.pool.refresh_brokers(&brokers);
        Ok(resp)
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
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        let conn = self.pool.get(self.broker_id).await?;
        conn.send(req).await
    }
}
