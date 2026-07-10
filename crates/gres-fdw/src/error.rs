//! error

/// Top-level error type for the Kafka FDW.
#[derive(Debug, thiserror::Error)]
pub enum KafkaFdwError {
    /// A required configuration option is missing or its value is invalid.
    #[error("kafka fdw config error: {0}")]
    Config(String),
    #[error("kafka fdw error: {0}")]
    Other(String),
}
