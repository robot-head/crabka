//! Connection management and request dispatch for Apache Kafka in Rust.

mod error;
mod request;
mod transport;
mod version;

pub use error::ClientError;
pub use request::ProtocolRequest;
pub use version::ApiVersionTable;
