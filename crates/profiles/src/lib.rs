//! Crabka profiles ingest service.
//!
//! The distributor serves the push.v1, `/ingest`, and OTLP `v1development`
//! profiles doors and writes to a WAL partitioned by
//! `(tenant, series_fingerprint)`. The block-builder consumer group builds the
//! samples fact table, the deduped per-block `SymbolDb`, and the
//! `ProfileIndex`.
#![forbid(unsafe_code)]

pub mod blockbuilder;
pub mod cold_store;
pub mod compactor;
pub mod distributor;
pub mod error;
pub mod hot_store;
pub mod ids;
pub mod ingest;
pub mod limits;
pub mod metrics;
pub mod query;
pub mod query_frontend;
pub mod symbolizer;
pub mod tenant;
pub mod wal;
pub mod wire;

pub use blockbuilder::{BuiltSample, build_block, intern_record, object_key, run, samples_batch};
pub use error::ProfilesError;
pub use ids::{
    DefaultMs, EndMs, ExternalPartition, IngestBytes, IngestItems, LocalPartition, MaxValue,
    MinValue, NowMs, StartMs,
};
pub use limits::{LimitError, Limits, OverridesError, OverridesProvider};
pub use wal::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample,
    WalSymbolSet, partition_key,
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn status_codes_map() {
        for (err, want) in [
            (ProfilesError::UnsupportedFormat("x".into()), 415),
            (ProfilesError::Decode("x".into()), 400),
            (
                ProfilesError::from(LimitError::MaxSeries {
                    limit: 1,
                    observed: 2,
                }),
                429,
            ),
        ] {
            check!(err.status_code() == want);
        }
    }
}
