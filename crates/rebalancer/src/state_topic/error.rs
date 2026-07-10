//! Error types for the state-topic subsystem.

use thiserror::Error;

/// Kafka partition errors that mean the internal state topic exists but
/// this broker cannot read or write partition data for it yet.
pub(crate) fn is_transient_topic_partition_code(code: i16) -> bool {
    matches!(code, 3 | 5 | 9)
}

#[derive(Debug, Error)]
pub enum StateTopicError {
    #[error("client error: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("admin error: {0}")]
    Admin(#[from] crabka_client_admin::AdminError),

    #[error("produce returned error code {code}")]
    ProduceErrorCode { code: i16 },

    #[error("fetch returned error code {code}")]
    FetchErrorCode { code: i16 },

    #[error("malformed json: {0}")]
    MalformedJson(#[from] serde_json::Error),

    #[error("state load did not converge within timeout")]
    LoadTimeout,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn transient_topic_partition_codes_are_exact() {
        for code in [3, 5, 9] {
            assert2::assert!(is_transient_topic_partition_code(code));
        }
        for code in [0, 1, 42] {
            assert2::assert!(!is_transient_topic_partition_code(code));
        }
    }
}
