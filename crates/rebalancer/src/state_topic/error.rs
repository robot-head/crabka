//! Error types for the state-topic subsystem.

use thiserror::Error;

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
