//! Columnar / dataframe support (opt-in via the `arrow`, `columnar`, and
//! `polars` cargo features). See `docs/superpowers/specs/2026-06-14-columnar-streams-support-design.md`.

pub mod serde;

#[cfg(feature = "polars")]
pub mod topology;
