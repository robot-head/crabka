//! Error type for the streams membership client.

/// Errors surfaced by the streams membership client.
#[derive(Debug, thiserror::Error)]
pub enum StreamsClientError {
    /// A transport failure or a dispatch failure from `crabka-client-core`.
    #[error(transparent)]
    Transport(#[from] crabka_client_core::ClientError),
    /// The topology build failed, because the node graph is bad.
    #[error("topology error: {0}")]
    Topology(#[from] crate::topology::TopologyError),
    /// The group coordinator was unavailable past the retry deadline.
    #[error("streams group coordinator unavailable")]
    CoordinatorUnavailable,
    /// The broker rejected the topology with an error from the
    /// `STREAMS_INVALID_TOPOLOGY*` family.
    #[error("invalid topology (code {code}): {message}")]
    InvalidTopology { code: i16, message: String },
    /// `GROUP_AUTHORIZATION_FAILED` / `TOPIC_AUTHORIZATION_FAILED`.
    #[error("authorization failed (code {0})")]
    Authorization(i16),
    /// `GROUP_ID_NOT_FOUND`.
    #[error("group id not found")]
    GroupIdNotFound,
    /// The membership handle has been closed.
    #[error("membership closed")]
    Closed,
    /// An unmapped broker error code.
    #[error("broker error code {0}")]
    Server(i16),
    /// A runtime failure in processing, produce, or commit.
    #[error("runtime error: {0}")]
    Runtime(String),
    /// An interactive query failed.
    #[error(transparent)]
    InteractiveQuery(#[from] crate::runtime::iq::IqError),
}
