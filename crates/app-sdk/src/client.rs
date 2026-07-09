//! Client construction and module accessors.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::{
    connect_client::ConnectClient,
    error::CrabkaError,
    messaging::MessagingClient,
    stubs::{AuthClient, BlobClient, DatabaseClient, QueuesClient},
};

/// Rust application SDK client.
#[derive(Debug, Clone)]
pub struct CrabkaClient {
    pub(crate) inner: Arc<ClientInner>,
}

#[derive(Debug)]
pub(crate) struct ClientInner {
    pub(crate) endpoint: String,
    pub(crate) bearer: Option<String>,
    pub(crate) connect: ConnectClient,
    pub(crate) mock: Mutex<MockState>,
}

/// In-memory backend used by `mock://` endpoints for conformance self-tests.
#[derive(Debug, Default)]
pub(crate) struct MockState {
    pub(crate) messages: Vec<MockMessage>,
    pub(crate) queue_sessions: Vec<MockQueueSession>,
}

/// In-memory message used by the mock backend.
#[derive(Debug, Clone)]
pub(crate) struct MockMessage {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) value: Bytes,
    pub(crate) headers: Vec<(String, Option<Bytes>)>,
}

/// In-memory queue session used by the mock backend.
#[derive(Debug, Clone, Default)]
pub(crate) struct MockQueueSession {
    pub(crate) id: String,
    pub(crate) delivered: Vec<MockQueueDelivery>,
}

/// Message key delivered through a mock queue session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MockQueueDelivery {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
}

impl CrabkaClient {
    /// Start building a client.
    #[must_use]
    pub fn builder() -> CrabkaClientBuilder {
        CrabkaClientBuilder::default()
    }

    /// Create a client for an endpoint with default options.
    pub fn new(endpoint: impl Into<String>) -> Result<Self, CrabkaError> {
        Self::builder().endpoint(endpoint).build()
    }

    /// Access the messaging module.
    #[must_use]
    pub fn messaging(&self) -> MessagingClient {
        MessagingClient::new(self.clone())
    }

    /// Access the queues module.
    #[must_use]
    pub fn queues(&self) -> QueuesClient {
        QueuesClient::new(self.clone())
    }

    /// Access the database module.
    #[must_use]
    pub fn database(&self) -> DatabaseClient {
        DatabaseClient::new()
    }

    /// Access the auth module.
    #[must_use]
    pub fn auth(&self) -> AuthClient {
        AuthClient::new(self.inner.bearer.clone())
    }

    /// Access the blob module.
    #[must_use]
    pub fn blob(&self) -> BlobClient {
        BlobClient::new()
    }

    pub(crate) fn is_mock(&self) -> bool {
        self.inner.endpoint.starts_with("mock://")
    }

    pub(crate) fn is_unreachable(&self) -> bool {
        self.inner.endpoint.starts_with("unreachable://")
    }
}

/// Builder for [`CrabkaClient`].
#[derive(Debug, Default)]
pub struct CrabkaClientBuilder {
    endpoint: Option<String>,
    bearer: Option<String>,
}

impl CrabkaClientBuilder {
    /// Set the gateway endpoint.
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set a bearer token.
    #[must_use]
    pub fn bearer_token(mut self, bearer: impl Into<String>) -> Self {
        self.bearer = Some(bearer.into());
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<CrabkaClient, CrabkaError> {
        let endpoint = self
            .endpoint
            .ok_or_else(|| CrabkaError::InvalidArgument("endpoint is required".into()))?;
        Ok(CrabkaClient {
            inner: Arc::new(ClientInner {
                connect: ConnectClient::new(endpoint.clone(), self.bearer.clone()),
                endpoint,
                bearer: self.bearer,
                mock: Mutex::new(MockState::default()),
            }),
        })
    }
}
