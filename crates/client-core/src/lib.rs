//! Connection management and request dispatch for Apache Kafka in Rust.
//!
//! This crate provides the first I/O-doing layer of Crabka. It wraps
//! `crabka-protocol`'s typed request/response messages in a `tokio`-based
//! TCP client that:
//!
//! - Opens one connection per broker, multiplexing requests via
//!   correlation ID.
//! - Negotiates API versions on connect.
//! - Manages a [`BrokerPool`] keyed on broker id with lazy connect.
//! - Resolves bootstrap addresses on builder.
//!
//! ## Quick start
//!
//! ```no_run
//! use crabka_client_core::Client;
//! use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = Client::builder()
//!     .bootstrap("localhost:9092")
//!     .client_id("my-app")
//!     .build()
//!     .await?;
//!
//! let resp = client.send(ApiVersionsRequest::default()).await?;
//! println!("broker supports {} APIs", resp.api_keys.len());
//!
//! client.close();
//! # Ok(())
//! # }
//! ```
//!
//! ## Scope and boundaries
//!
//! This crate is the shared transport and request-dispatch layer. It provides
//! bootstrap resolution, API-version negotiation, broker-id connection pooling,
//! typed request/response dispatch, low-level fetch helpers, and client-side
//! TLS/SASL negotiation. Higher-level semantics — batching, idempotence,
//! consumer-group heartbeats, commits, admin retries, and transactions — live in
//! the producer, consumer, and admin crates built on top of this one.
//!
//! TLS / SASL: a client-side security surface lives in [`security`] and
//! [`sasl`] — set [`ConnectionOptions::security`] (or the `Client`
//! builder's `.security(...)`) to negotiate TLS then SASL before the
//! API-versions bootstrap. `None` (the default) is plaintext.
//!
//! ## Cargo features
//!
//! - `mock` — exposes `MockBroker` beyond `#[cfg(test)]` for downstream
//!   testing.

mod bootstrap;
mod client;
mod connection;
mod error;
mod fetch;
mod offset_for_leader_epoch;
mod pool;
mod request;
pub mod sasl;
pub mod security;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use client::{BrokerHandle, Client};
pub use connection::{ClientDuplex, Connection, ConnectionOptions};
pub use error::ClientError;
pub use fetch::{
    DEFAULT_FETCH_RESPONSE_MAX_BYTES, FetchPartitionResult, FetchedHeader, FetchedRecord,
    IsolatedFetch, fetch_partition, fetch_partition_with_isolation,
    fetch_partition_with_isolation_progress,
};
#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
pub use offset_for_leader_epoch::{EpochEndOffset, offset_for_leader_epoch};
pub use pool::{BrokerInfo, BrokerPool};
pub use request::ProtocolRequest;
pub use sasl::{OutboundSaslError, SaslCredentials, outbound_sasl};
pub use security::{ClientSecurity, TlsConnectorConfig};
pub use version::ApiVersionTable;
