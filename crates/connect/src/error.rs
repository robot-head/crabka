//! The error type shared by the connector SPI.

/// Errors raised by [`Source`](crate::Source), [`Sink`](crate::Sink), and
/// [`Converter`](crate::Converter) implementations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// An I/O failure in communication with the external system, for example on
    /// a socket or a file.
    #[error("connect I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization or deserialization of a record payload failed. This variant
    /// wraps converter failures, for example a schema-registry serde that
    /// rejects a value, or a writer schema that is not yet resolved.
    #[error("conversion error: {0}")]
    Convert(String),

    /// A checkpoint could not be produced, or a [`SourceOffset`](crate::SourceOffset)
    /// handed back to [`Source::seek`](crate::Source::seek) does not name a
    /// position this source can resume from.
    #[error("offset error: {0}")]
    Offset(String),

    /// A transactional [`Sink`](crate::Sink) operation failed, that is, `begin`,
    /// `commit`, or `abort`. This variant also covers a transactional method
    /// driven against a sink that does not support it.
    #[error("transaction error: {0}")]
    Transaction(String),

    /// A backend error that does not map cleanly onto one of the structured
    /// variants above.
    #[error("connect backend error: {0}")]
    Backend(String),
}
