//! The converter layer: bridge a connector's typed payload `T` to and from the
//! raw [`Bytes`] that travel on the Kafka wire.
//!
//! Sources and sinks work in the connector's domain types. The runtime applies
//! a [`Converter`] to each of a record's key and value to cross the wire
//! boundary. Two converters ship here:
//!
//! - [`ByteIdentity`][]: `T = Bytes`, a byte-for-byte passthrough that preserves
//!   exact wire bytes. That is the Kafka-compatibility constraint that always
//!   matters.
//! - [`SchemaConverter`][]: bridges the Confluent schema-registry serdes from
//!   `crabka-schema-serde`, so a connector can read and write typed Avro,
//!   Protobuf, and JSON records.

use std::sync::Arc;

use bytes::Bytes;
use crabka_schema_serde::format::{SchemaDeserializer, SchemaSerializer};

use crate::error::ConnectError;

/// Bridges a connector's typed payload `T` to and from wire [`Bytes`].
///
/// The runtime passes `topic` to both directions, so a schema-aware converter
/// can derive its registry subject, `<topic>-key` or `<topic>-value`. A
/// byte-oriented converter ignores it. This mirrors Kafka Connect's `Converter`.
pub trait Converter<T>: Send + Sync + 'static {
    /// Encode `value` to its on-wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Convert`] if the value cannot be serialized.
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, ConnectError>;

    /// Decode on-wire `bytes` back into `T`.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Convert`] if the bytes cannot be deserialized.
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, ConnectError>;
}

/// A byte-for-byte passthrough converter: `T = Bytes`.
///
/// `serialize` clones the bytes through unchanged, which is a cheap `Bytes`
/// refcount bump, and `deserialize` copies the input into an owned `Bytes`. Use
/// this for connectors that move opaque records and do not touch the payload,
/// where exact wire bytes are the point.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteIdentity;

impl Converter<Bytes> for ByteIdentity {
    fn serialize(&self, _topic: &str, value: &Bytes) -> Result<Bytes, ConnectError> {
        Ok(value.clone())
    }

    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<Bytes, ConnectError> {
        Ok(Bytes::copy_from_slice(bytes))
    }
}

/// A schema-registry-backed converter. It serializes with a
/// [`SchemaSerializer`] and deserializes with a [`SchemaDeserializer`] from
/// `crabka-schema-serde`.
///
/// Pair the serializer and the deserializer of one format serde, for example
/// those of an `AvroSerde`, to read and write typed records framed in the
/// Confluent wire format `magic | schema_id | body`. A schema-serde error
/// becomes a [`ConnectError::Convert`].
pub struct SchemaConverter<T> {
    serializer: Arc<dyn SchemaSerializer<T>>,
    deserializer: Arc<dyn SchemaDeserializer<T>>,
}

impl<T> SchemaConverter<T> {
    /// Build a converter from a schema serializer and deserializer for `T`.
    pub fn new(
        serializer: Arc<dyn SchemaSerializer<T>>,
        deserializer: Arc<dyn SchemaDeserializer<T>>,
    ) -> Self {
        Self {
            serializer,
            deserializer,
        }
    }
}

impl<T: Send + Sync + 'static> Converter<T> for SchemaConverter<T> {
    fn serialize(&self, topic: &str, value: &T) -> Result<Bytes, ConnectError> {
        self.serializer
            .serialize(topic, value)
            .map_err(|e| ConnectError::Convert(e.to_string()))
    }

    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, ConnectError> {
        self.deserializer
            .deserialize(topic, bytes)
            .map_err(|e| ConnectError::Convert(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_schema_serde::SchemaSerdeError;

    use super::*;

    /// A serde whose output encodes its inputs, so a test can prove that
    /// [`SchemaConverter`] forwards to it and does not fabricate a value.
    struct FakeSerde;

    impl SchemaSerializer<String> for FakeSerde {
        fn serialize(&self, topic: &str, value: &String) -> Result<Bytes, SchemaSerdeError> {
            Ok(Bytes::from(format!("{topic}:{value}")))
        }
    }

    impl SchemaDeserializer<String> for FakeSerde {
        fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<String, SchemaSerdeError> {
            String::from_utf8(bytes.to_vec()).map_err(|e| SchemaSerdeError::Wire(e.to_string()))
        }
    }

    #[test]
    fn schema_converter_serialize_forwards_to_inner_serde() {
        let conv = SchemaConverter::new(Arc::new(FakeSerde), Arc::new(FakeSerde));
        let out = conv.serialize("topic", &"hello".to_owned()).unwrap();
        // A non-empty, input-derived payload — not the empty `Bytes::default()`.
        check!(out == Bytes::from_static(b"topic:hello"));
    }

    #[test]
    fn schema_converter_deserialize_forwards_to_inner_serde() {
        let conv = SchemaConverter::new(Arc::new(FakeSerde), Arc::new(FakeSerde));
        let value = conv.deserialize("topic", b"world").unwrap();
        check!(value == "world");
    }

    #[test]
    fn byte_identity_serialize_preserves_bytes() {
        let c = ByteIdentity;
        let v = Bytes::from_static(b"\x00\x01\xff payload");
        let out = c.serialize("t", &v).unwrap();
        check!(out == v);
    }

    #[test]
    fn byte_identity_round_trips() {
        let c = ByteIdentity;
        let original = b"\xde\xad\xbe\xef";
        let decoded = c.deserialize("t", original).unwrap();
        let encoded = c.serialize("t", &decoded).unwrap();
        check!(encoded.as_ref() == original);
    }
}
