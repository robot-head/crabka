//! Error type for schema serdes.

/// Failures from registry I/O, framing, and (de)serialization.
#[derive(Debug, thiserror::Error)]
pub enum SchemaSerdeError {
    /// Registry HTTP/transport failure.
    #[error("registry request failed: {0}")]
    Registry(String),

    /// Registry returned a non-success status with a body.
    #[error("registry error {status}: {body}")]
    RegistryStatus { status: u16, body: String },

    /// The Confluent wire frame was malformed (bad magic, truncated id).
    #[error("malformed wire frame: {0}")]
    Wire(String),

    /// Encoding a value to its format-specific body failed.
    #[error("serialize error: {0}")]
    Serialize(String),

    /// Decoding a format-specific body into the target type failed.
    #[error("deserialize error: {0}")]
    Deserialize(String),

    /// Could not build/normalize the schema for a type.
    #[error("schema error: {0}")]
    Schema(String),

    /// The writer schema for a seen id is not cached yet; a background fetch was
    /// started. Retriable: re-deliver the record shortly.
    #[error("writer schema for id {0} pending fetch")]
    WriterSchemaPending(u32),
}
