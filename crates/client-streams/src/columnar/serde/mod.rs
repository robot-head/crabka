//! Library-native `Serde<T>` implementations for columnar/dataframe payloads.

#[cfg(feature = "columnar")]
pub mod columnar;
#[cfg(feature = "arrow")]
pub mod arrow;
#[cfg(feature = "polars")]
pub mod polars;
