//! Kafka wire-protocol compression codecs.

mod codec_type;
mod error;

pub use codec_type::CompressionType;
pub use error::CompressionError;
