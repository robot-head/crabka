//! Kafka wire protocol codec.

mod codec;
mod error;
pub mod primitives;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
