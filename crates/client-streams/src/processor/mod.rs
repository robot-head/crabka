//! Typed Processor API + the `dyn Any`-erased execution graph (sub-project #2).

pub mod serde;

pub use serde::{BytesSerde, I64Serde, Serde, SerdeError, StringSerde};
