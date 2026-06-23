//! Crabka profiles ingest service: distributor (push.v1 / `/ingest` / OTLP
//! `v1development` profiles doors) -> `(tenant, series_fingerprint)`-partitioned
//! WAL, and the block-builder consumer group (samples fact table + dedup
//! per-block `SymbolDb` + `ProfileIndex`).
#![forbid(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cloned_ref_to_slice_refs,
    clippy::default_trait_access,
    clippy::derivable_impls,
    clippy::float_cmp,
    clippy::format_push_string,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::needless_pass_by_value,
    clippy::needless_question_mark,
    clippy::needless_raw_string_hashes,
    clippy::needless_update,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::trivially_copy_pass_by_ref,
    clippy::type_complexity,
    clippy::unnecessary_wraps,
    clippy::unreadable_literal,
    clippy::unused_async
)]

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
pub mod tenant;
pub mod wal;
pub mod wire;

pub use blockbuilder::{BuiltSample, build_block, intern_record, object_key, run, samples_batch};
pub use error::ProfilesError;
pub use limits::{LimitError, Limits, OverridesError, OverridesProvider};
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
        assert!(
            ProfilesError::from(LimitError::MaxSeries {
                limit: 1,
                observed: 2,
            })
            .status_code()
                == 429
        );
    }
}
