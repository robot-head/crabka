//! Top-level [`Client`] + [`ClientBuilder`]. Wraps a [`BrokerPool`] and
//! exposes a typed-request `send` API.

use std::sync::Arc;
use std::time::Duration;

use crate::bootstrap;
use crate::connection::ConnectionOptions;
use crate::error::ClientError;
use crate::pool::{BrokerInfo, BrokerPool};
use crate::request::ProtocolRequest;

/// A Kafka client backed by a [`BrokerPool`].
///
/// Construct via [`Client::builder`].
pub struct Client {
    pool: Arc<BrokerPool>,
    #[allow(dead_code)]
    options: ConnectionOptions,
}

impl Client {
    /// Create a [`ClientBuilder`] for the given bootstrap address string.
    ///
    /// `bootstrap` is a comma-separated list of `host:port` pairs.
    pub fn builder(bootstrap: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            bootstrap: bootstrap.into(),
            options: ConnectionOptions::default(),
        }
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

/// Builder for [`Client`].
///
/// Created via [`Client::builder`].
pub struct ClientBuilder {
    bootstrap: String,
    options: ConnectionOptions,
}

impl ClientBuilder {
    /// Override the client id sent in request headers (default: `"crabka"`).
    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.options.client_id = id.into();
        self
    }

    /// Override the per-request timeout (default: 30 s).
    #[must_use]
    pub fn request_timeout(mut self, t: Duration) -> Self {
        self.options.request_timeout = t;
        self
    }

    /// Override the TCP connect timeout (default: 30 s).
    #[must_use]
    pub fn connect_timeout(mut self, t: Duration) -> Self {
        self.options.connect_timeout = t;
        self
    }

    /// Resolve the bootstrap addresses and build the [`Client`].
    pub async fn build(self) -> Result<Client, ClientError> {
        let addrs = bootstrap::resolve(&self.bootstrap).await?;
        let pool = Arc::new(BrokerPool::new(addrs, self.options.clone()));
        Ok(Client {
            pool,
            options: self.options,
        })
    }
}
