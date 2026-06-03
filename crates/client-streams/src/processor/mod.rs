//! Typed Processor API + the `dyn Any`-erased execution graph (sub-project #2).

pub mod erased;
pub mod record;
pub mod serde;

pub use erased::ProcessorError;
pub use record::{Record, RecordContext};
pub use serde::{BytesSerde, I64Serde, Serde, SerdeError, StringSerde};
