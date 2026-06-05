//! Pluggable record codec. v1 ships `RawCodec` (identity, opaque bytes).
//! The deferred Schema Registry component adds a `SchemaRegistryCodec`
//! that implements this same trait — front-ends/cores never change.

use bytes::Bytes;

/// Encodes/decodes record values on the way to/from Kafka.
pub trait RecordCodec: Send + Sync + 'static {
    fn encode_value(&self, topic: &str, value: Bytes) -> Bytes;
    fn decode_value(&self, topic: &str, value: Bytes) -> Bytes;
}

/// Identity codec — opaque pass-through. The only codec in P0–P2.
#[derive(Debug, Clone, Copy, Default)]
pub struct RawCodec;

impl RecordCodec for RawCodec {
    fn encode_value(&self, _topic: &str, value: Bytes) -> Bytes {
        value
    }
    fn decode_value(&self, _topic: &str, value: Bytes) -> Bytes {
        value
    }
}
