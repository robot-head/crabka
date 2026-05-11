//! Kafka wire protocol codec.

mod arbitrary_impls;
mod codec;
mod error;
pub mod owned;
pub mod primitives;
pub mod tagged_fields;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
