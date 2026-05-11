//! Connection management and request dispatch for Apache Kafka in Rust.

mod bootstrap;
mod client;
mod connection;
mod error;
mod pool;
mod request;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use client::{BrokerHandle, Client, ClientBuilder};
pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use pool::{BrokerInfo, BrokerPool};
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
