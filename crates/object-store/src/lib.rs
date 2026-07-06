//! `crabka-object-store` — unified object-store construction shared by Crabka's
//! KIP-405 tiered storage (`crabka-remote-storage`) and observability blockstore
//! (`crabka-blockstore`).
//!
//! Scope is the object-store access/plumbing layer only: turning a typed
//! `ObjectStoreConfig` into an `object_store::ObjectStore` handle. Data
//! representation (verbatim Kafka segment bytes vs Parquet blocks) stays in the
//! respective consumer crates.

mod build;
mod config;
mod error;

pub use build::build_object_store;
pub use config::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, GcsConfig, ObjectStoreConfig,
    S3Config,
};
pub use error::ObjectStoreError;
