//! Connection management and request dispatch for Apache Kafka in Rust.

mod connection;
mod error;
mod request;
mod transport;
mod version;

#[cfg(any(test, feature = "mock"))]
mod mock;

pub use connection::{Connection, ConnectionOptions};
pub use error::ClientError;
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;

#[cfg(any(test, feature = "mock"))]
pub use mock::MockBroker;
