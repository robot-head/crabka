//! Internal errors produced by the broker's handlers and lifecycle.
//!
//! These are NOT Kafka wire-level error codes (those live in
//! [`crate::codes`]). Conversion from `BrokerError` to a wire code
//! happens at the handler boundary.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BrokerError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("log: {0}")]
    Log(#[from] crabka_log::LogError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("unsupported api_key={api_key} version={version}")]
    UnsupportedApi { api_key: i16, version: i16 },

    #[error("partition writer for {topic}-{partition} died")]
    PartitionWriterDied { topic: String, partition: i32 },

    #[error("shutting down")]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_unsupported_api() {
        let e = BrokerError::UnsupportedApi {
            api_key: 7,
            version: 9,
        };
        assert!(e.to_string().contains("api_key=7"));
        assert!(e.to_string().contains("version=9"));
    }
}
