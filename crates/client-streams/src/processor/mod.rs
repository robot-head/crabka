//! Typed Processor API and the type-erased execution graph used by the runtime.

pub mod api;
pub mod erased;
pub(crate) mod factory;
pub mod fixed_key;
pub(crate) mod graph;
pub(crate) mod node;
pub mod punctuation;
pub mod record;
pub mod serde;

pub mod schema_serde;

pub use api::{Processor, ProcessorContext, ProcessorSupplier};
pub use erased::ProcessorError;
pub use fixed_key::{
    FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyProcessorSupplier, FixedKeyRecord,
};
pub use punctuation::{Cancellable, PunctuationType, Punctuator};
pub use record::{Record, RecordContext};
pub use serde::{
    BytesSerde, Changed, Consumed, DefaultSerde, I64Serde, Produced, Serde, SerdeError, SerdeRole,
    StringSerde,
};
