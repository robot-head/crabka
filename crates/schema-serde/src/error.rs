//! Error type for schema serdes.

/// Failures from registry I/O, framing, and (de)serialization.
#[derive(Debug, thiserror::Error)]
pub enum SchemaSerdeError {
    /// Registry network or response-body transport failure.
    #[error("registry transport failed: {0}")]
    RegistryTransport(String),

    /// Registry returned a non-success status with a body.
    #[error("registry error {status}: {body}")]
    RegistryStatus { status: u16, body: String },

    /// Registry returned a successful response whose body was not valid JSON
    /// for the requested endpoint.
    #[error("registry response decode failed: {0}")]
    RegistryDecode(String),

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

    /// The registry could not resolve a writer schema and all of its references.
    #[error("writer schema for id {id} unavailable: {reason}")]
    WriterSchemaUnavailable { id: u32, reason: String },
}

impl SchemaSerdeError {
    pub(crate) fn is_transient_registry_failure(&self) -> bool {
        matches!(
            self,
            Self::RegistryTransport(_)
                | Self::RegistryStatus {
                    status: 429 | 500..=u16::MAX,
                    ..
                }
        )
    }
}
