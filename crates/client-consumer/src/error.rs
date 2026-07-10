//! Error type for `crabka-client-consumer`.

use thiserror::Error;

/// Errors returned by [`Consumer`](crate::Consumer).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    #[error("client: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("rebalance failed: {0}")]
    RebalanceFailed(String),

    #[error("startup failed after joining group: {0}")]
    StartupAfterJoin(Box<ConsumerError>),

    #[error("not subscribed to any topic")]
    NotSubscribed,

    #[error("illegal state: {0}")]
    IllegalState(String),

    #[error("invalid seek offset {0}: must be non-negative")]
    InvalidOffset(i64),

    #[error("commit conflict: rejoined since this poll")]
    CommitInvalid,

    #[error("coordinator unavailable")]
    CoordinatorUnavailable,

    #[error(
        "log truncation detected on {topic}-{partition}: fetch offset {fetch_offset} is past the leader's log; safe offset {safe_offset}"
    )]
    LogTruncation {
        topic: String,
        partition: i32,
        fetch_offset: i64,
        safe_offset: i64,
    },

    #[error("broker error_code {0}")]
    Server(i16),
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn display_messages() {
        for (name, error, expected) in [
            (
                "not subscribed",
                ConsumerError::NotSubscribed,
                "not subscribed to any topic",
            ),
            ("server", ConsumerError::Server(25), "broker error_code 25"),
            (
                "log truncation",
                ConsumerError::LogTruncation {
                    topic: "t".into(),
                    partition: 3,
                    fetch_offset: 100,
                    safe_offset: 42,
                },
                "log truncation detected on t-3: fetch offset 100 is past the leader's log; safe offset 42",
            ),
        ] {
            assert!(error.to_string() == expected, "case {name}");
        }
    }
}
