//! Error type for `crabka-client-core`.

use std::{net::SocketAddr, time::Duration};

use thiserror::Error;

/// Errors returned by `Client`, `Connection`, and the broker pool.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("connect to {addr}: {source}")]
    Connect {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    #[error("connection closed")]
    Disconnected,

    #[error("invalid client configuration: {0}")]
    InvalidConfig(String),

    #[error("request timed out after {0:?}")]
    Timeout(Duration),

    #[error(
        "incompatible version: broker supports {broker_min}..={broker_max}, \
         client wants {client_min}..={client_max} for api_key {api_key}"
    )]
    IncompatibleVersion {
        api_key: i16,
        broker_min: i16,
        broker_max: i16,
        client_min: i16,
        client_max: i16,
    },

    #[error("protocol error from server: {error_code}")]
    Server { error_code: i16 },

    #[error("codec: {0}")]
    Codec(#[from] crabka_protocol::ProtocolError),

    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn display_is_useful() {
        let e = ClientError::Timeout(Duration::from_secs(5));
        assert!(e.to_string() == "request timed out after 5s");
    }

    #[test]
    fn incompatible_version_displays_full_range() {
        let e = ClientError::IncompatibleVersion {
            api_key: 0,
            broker_min: 0,
            broker_max: 5,
            client_min: 7,
            client_max: 10,
        };
        assert!(e.to_string().contains("api_key 0"));
        assert!(e.to_string().contains("broker supports 0..=5"));
    }
}
