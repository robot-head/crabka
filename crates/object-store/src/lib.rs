//! `crabka-object-store` — unified object-store construction shared by Crabka's
//! KIP-405 tiered storage (`crabka-remote-storage`) and observability blockstore
//! (`crabka-blockstore`).
//!
//! Scope is the object-store access/plumbing layer only: turning a typed
//! `ObjectStoreConfig` into an `object_store::ObjectStore` handle. Data
//! representation (verbatim Kafka segment bytes vs Parquet blocks) stays in the
//! respective consumer crates.
