//! Error types for the block store.

/// Block store operation failure.
#[derive(Debug, thiserror::Error)]
pub enum BlockStoreError {
    #[error("invalid block: {0}")]
    InvalidBlock(String),
    #[error("object store error: {0}")]
    ObjectStore(String),
    #[error("parquet error: {0}")]
    Parquet(String),
    #[error("datafusion error: {0}")]
    DataFusion(String),
    #[error("serialization error: {0}")]
    Serde(String),
}

impl From<object_store::Error> for BlockStoreError {
    fn from(value: object_store::Error) -> Self {
        Self::ObjectStore(value.to_string())
    }
}

impl From<parquet::errors::ParquetError> for BlockStoreError {
    fn from(value: parquet::errors::ParquetError) -> Self {
        Self::Parquet(value.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for BlockStoreError {
    fn from(value: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(value.to_string())
    }
}

impl From<serde_json::Error> for BlockStoreError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serde(value.to_string())
    }
}

/// Block store result alias.
pub type Result<T> = std::result::Result<T, BlockStoreError>;
