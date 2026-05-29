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
//! ## Out of scope
//!
//! - Producer / consumer semantics.
//! - Transactions.
//! - Partition-aware routing.
//! - TLS / SASL.
//! - Automatic mid-request retry.
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
mod pool;
mod request;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use client::{BrokerHandle, Client};
pub use connection::{ClientDuplex, Connection, ConnectionOptions};
pub use error::ClientError;
pub use fetch::{FetchedRecord, fetch_partition};
pub use pool::{BrokerInfo, BrokerPool};
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
