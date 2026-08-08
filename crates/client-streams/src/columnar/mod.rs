//! Columnar and dataframe support. The `arrow`, `columnar`, and `polars` cargo
//! features opt into it.
//! See `docs/superpowers/specs/2026-06-14-columnar-streams-support-design.md`.

pub mod serde;

#[cfg(feature = "polars")]
pub mod topology;
