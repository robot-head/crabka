//! Error type for `crabka-client-consumer`.

use thiserror::Error;

/// Errors returned by `Consumer` and `ConsumerBuilder`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    #[error("client: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("rebalance failed: {0}")]
    RebalanceFailed(String),

    #[error("not subscribed to any topic")]
    NotSubscribed,

    #[error("commit conflict: rejoined since this poll")]
    CommitInvalid,

    #[error("coordinator unavailable")]
    CoordinatorUnavailable,

    #[error("broker error_code {0}")]
    Server(i16),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_not_subscribed() {
        let e = ConsumerError::NotSubscribed;
        assert!(e.to_string().contains("not subscribed"));
    }

    #[test]
    fn display_server_error_code() {
        let e = ConsumerError::Server(25);
        assert!(e.to_string().contains("25"));
    }
}
