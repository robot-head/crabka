//! Crate-wide error + ingest-edge HTTP status mapping.

/// Errors across the profiles ingest pipeline.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ProfilesError {
    #[error("unsupported content-type/format: {0}")]
    UnsupportedFormat(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("gunzip failed: {0}")]
    Gunzip(String),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("{0}")]
    Limit(crate::limits::LimitError),
    #[error("payload exceeds limit {limit} bytes")]
    TooLarge { limit: usize },
    #[error("wal codec: {0}")]
    Wal(String),
    #[error("produce failed: {0}")]
    Produce(String),
    #[error("block build failed: {0}")]
    Block(String),
    #[error("pprof: {0}")]
    Pprof(String),
    /// An unexpected server-side fault, for example a poisoned lock. The inner
    /// string is for server-side logging only. Callers must NOT show it verbatim
    /// to clients. The ingest edge maps this variant to a generic 500 message.
    #[error("internal error: {0}")]
    Internal(String),
}

impl ProfilesError {
    /// Map to the ingest-edge HTTP status.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::UnsupportedFormat(_) => 415,
            Self::Decode(_)
            | Self::Gunzip(_)
            | Self::Invalid(_)
            | Self::Pprof(_)
            | Self::TooLarge { .. } => 400,
            Self::Limit(err) => err.http_status(),
            Self::Wal(_) | Self::Produce(_) | Self::Block(_) | Self::Internal(_) => 500,
        }
    }
}

impl From<crabka_pprof::ProfileError> for ProfilesError {
    fn from(err: crabka_pprof::ProfileError) -> Self {
        Self::Pprof(err.to_string())
    }
}

impl From<crate::limits::LimitError> for ProfilesError {
    fn from(err: crate::limits::LimitError) -> Self {
        Self::Limit(err)
    }
}
