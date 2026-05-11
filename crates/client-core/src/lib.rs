//! Connection management and request dispatch for Apache Kafka in Rust.

mod error;
mod request;

pub use error::ClientError;
pub use request::ProtocolRequest;
