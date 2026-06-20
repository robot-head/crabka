//! Crabka profiles ingest service: distributor (push.v1 / `/ingest` / OTLP
//! `v1development` profiles doors) -> `(tenant, series_fingerprint)`-partitioned
//! WAL, and the block-builder consumer group (samples fact table + dedup
//! per-block `SymbolDb` + `ProfileIndex`).
#![forbid(unsafe_code)]

pub mod blockbuilder;
pub mod cold_store;
pub mod compactor;
pub mod distributor;
pub mod error;
pub mod hot_store;
pub mod ingest;
pub mod limits;
pub mod query;
pub mod query_frontend;
pub mod symbolizer;
pub mod wal;
pub mod wire;

pub use blockbuilder::{BuiltSample, build_block, intern_record, object_key, run, samples_batch};
pub use error::ProfilesError;
pub use limits::{LimitError, Limits};
pub use wal::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample,
    WalSymbolSet, partition_key,
};

/// Placeholder so the crate has a test until real ingest modules land.
#[must_use]
pub fn crate_smoke() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn smoke() {
        assert!(crate_smoke());
    }

    #[test]
    fn status_codes_map() {
        assert!(ProfilesError::UnsupportedFormat("x".into()).status_code() == 415);
        assert!(ProfilesError::Decode("x".into()).status_code() == 400);
    }
}
