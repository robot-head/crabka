//! Typed Processor API + the `dyn Any`-erased execution graph (sub-project #2).

pub mod api;
pub mod erased;
pub(crate) mod node;
pub mod record;
pub mod serde;

pub use api::{Processor, ProcessorContext, ProcessorSupplier};
pub use erased::ProcessorError;
pub use record::{Record, RecordContext};
pub use serde::{BytesSerde, I64Serde, Serde, SerdeError, StringSerde};
