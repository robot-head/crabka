//! Library-native `Serde<T>` implementations for columnar/dataframe payloads.

#[cfg(feature = "arrow")]
pub mod arrow;
#[cfg(feature = "columnar")]
pub mod columnar;
#[cfg(feature = "polars")]
pub mod polars;
