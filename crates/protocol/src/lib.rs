//! Kafka wire protocol codec.

mod codec;
mod error;
pub mod primitives;
pub mod tagged_fields;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
