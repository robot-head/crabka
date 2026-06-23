//! Error type for the block store.

/// Errors raised by the block store. Backend errors are stringified so public
/// errors stay stable across dependency details.
#[derive(Debug, thiserror::Error)]
pub enum BlockStoreError {
    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("datafusion error: {0}")]
    DataFusion(String),

    #[error("invalid block: {0}")]
    InvalidBlock(String),

    #[error("index snapshot serialization error: {0}")]
    Serde(String),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, BlockStoreError>;

impl From<object_store::Error> for BlockStoreError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}

impl From<parquet::errors::ParquetError> for BlockStoreError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(error.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for BlockStoreError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(error.to_string())
    }
}

impl From<serde_json::Error> for BlockStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serde(error.to_string())
    }
}
