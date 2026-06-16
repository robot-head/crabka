//! The error type shared by the connector SPI.

/// Errors raised by [`Source`](crate::Source), [`Sink`](crate::Sink), and
/// [`Converter`](crate::Converter) implementations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// An I/O failure talking to the external system (socket, file, …).
    #[error("connect I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serializing or deserializing a record payload failed. Wraps converter
    /// failures, e.g. a schema-registry serde rejecting a value or a writer
    /// schema not yet resolved.
    #[error("conversion error: {0}")]
    Convert(String),

    /// A checkpoint could not be produced, or a [`SourceOffset`](crate::SourceOffset)
    /// handed back to [`Source::seek`](crate::Source::seek) does not name a
    /// position this source can resume from.
    #[error("offset error: {0}")]
    Offset(String),

    /// A transactional [`Sink`](crate::Sink) operation failed (begin / commit /
    /// abort), or transactional methods were driven against a sink that does
    /// not support them.
    #[error("transaction error: {0}")]
    Transaction(String),

    /// A backend error that does not map cleanly onto one of the structured
    /// variants above.
    #[error("connect backend error: {0}")]
    Backend(String),
}
