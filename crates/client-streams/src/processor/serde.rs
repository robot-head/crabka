//! `Serde<T>`: typed (de)serialization at source/sink boundaries.

use bytes::Bytes;

/// Failure to deserialize bytes into `T`.
#[derive(Debug, thiserror::Error)]
#[error("deserialization error: {0}")]
pub struct SerdeError(pub String);

/// Serialize a `T` to bytes and back. Used by source nodes (deserialize) and
/// sink/repartition nodes (serialize).
#[diagnostic::on_unimplemented(
    message = "the type `{Self}` is not a valid serializer/deserializer for `{T}`",
    label = "does not implement `Serde<{T}>`",
    note = "implement `Serde<{T}>` for `{Self}` or verify that the type parameter `{T}` matches the deserialized type of this serde"
)]
pub trait Serde<T>: Send + Sync + 'static {
    fn serialize(&self, value: &T) -> Bytes;
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerdeError>;
}

/// Associate a concrete Serde type with its target deserialized type to enable automatic type inference.
#[diagnostic::on_unimplemented(
    message = "the serde type `{Self}` is not associated with any target model type",
    label = "does not implement `SerdeAssociate`",
    note = "implement `SerdeAssociate` for `{Self}` and define `type Target = ...;` to map this Serde to its model type"
)]
pub trait SerdeAssociate {
    type Target: Send + Sync + 'static;
}

/// The key + value [`Serde`]s used to **read** a topic into the topology.
///
/// A source node (and [`TopologyTestDriver::pipe_input`]) deserializes incoming
/// bytes with these. Pairing the two serdes into one named argument keeps their
/// roles visible at the call site — `Consumed::with(keySerde, valueSerde)` reads
/// the same as Kafka Streams' `Consumed`.
///
/// [`TopologyTestDriver::pipe_input`]: crate::TopologyTestDriver::pipe_input
#[derive(Debug, Clone, Copy)]
pub struct Consumed<KS, VS> {
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
}

impl<KS, VS> Consumed<KS, VS> {
    /// Pair a key serde with a value serde (key first, mirroring
    /// `Consumed.with(keySerde, valueSerde)`).
    #[must_use]
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self {
            key_serde,
            value_serde,
        }
    }
}

/// The key + value [`Serde`]s used to **write** a topic from the topology.
///
/// A sink node serializes outgoing records with these;
/// [`TopologyTestDriver::read_output`] uses them to deserialize what a sink
/// wrote. `Produced::with(keySerde, valueSerde)` reads the same as Kafka
/// Streams' `Produced`.
///
/// [`TopologyTestDriver::read_output`]: crate::TopologyTestDriver::read_output
#[derive(Debug, Clone, Copy)]
pub struct Produced<KS, VS> {
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
}

impl<KS, VS> Produced<KS, VS> {
    /// Pair a key serde with a value serde (key first, mirroring
    /// `Produced.with(keySerde, valueSerde)`).
    #[must_use]
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self {
            key_serde,
            value_serde,
        }
    }
}

/// A clonable [`Serde<T>`] over a type-erased `Arc<dyn Serde<T>>`. Lets a serde
/// captured behind an `Arc` (e.g. a `KTable`'s stored key/value serde) be passed
/// to `add_source`/`add_sink` (which want a `Serde<T> + Clone` value) or boxed
/// into a processor field, without naming the concrete serde type.
pub(crate) struct SerdeArc<T>(pub(crate) std::sync::Arc<dyn Serde<T>>);

impl<T> Clone for SerdeArc<T> {
    fn clone(&self) -> Self {
        SerdeArc(std::sync::Arc::clone(&self.0))
    }
}

impl<T: Send + Sync + 'static> Serde<T> for SerdeArc<T> {
    fn serialize(&self, value: &T) -> Bytes {
        self.0.serialize(value)
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerdeError> {
        self.0.deserialize(bytes)
    }
}

impl<T: Send + Sync + 'static> SerdeAssociate for SerdeArc<T> {
    type Target = T;
}

/// Identity serde for raw `Bytes`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerde;
impl Serde<Bytes> for BytesSerde {
    fn serialize(&self, value: &Bytes) -> Bytes {
        value.clone()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Bytes, SerdeError> {
        Ok(Bytes::copy_from_slice(bytes))
    }
}

impl SerdeAssociate for BytesSerde {
    type Target = Bytes;
}

/// UTF-8 `String` serde.
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSerde;
impl Serde<String> for StringSerde {
    fn serialize(&self, value: &String) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<String, SerdeError> {
        String::from_utf8(bytes.to_vec()).map_err(|e| SerdeError(e.to_string()))
    }
}

impl SerdeAssociate for StringSerde {
    type Target = String;
}

/// Big-endian 8-byte `i64` serde (matches the JVM `Serdes.Long()`).
#[derive(Debug, Clone, Copy, Default)]
pub struct I64Serde;
impl Serde<i64> for I64Serde {
    fn serialize(&self, value: &i64) -> Bytes {
        Bytes::copy_from_slice(&value.to_be_bytes())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<i64, SerdeError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| SerdeError(format!("expected 8 bytes, got {}", bytes.len())))?;
        Ok(i64::from_be_bytes(arr))
    }
}

impl SerdeAssociate for I64Serde {
    type Target = i64;
}

/// Mapping trait for types that have a default associated [`Serde`].
///
/// If a type does not implement this trait, using `.stream_default` or other default
/// methods will trigger a compiler error indicating that the default serde is not found.
///
/// # Examples
///
/// ```compile_fail
/// use crabka_client_streams::{StreamsBuilder, KStream};
///
/// struct CustomType;
///
/// let builder = StreamsBuilder::new();
/// // Fails because CustomType does not implement DefaultSerde:
/// let stream: KStream<CustomType, String> = builder.stream_default(["topic"]);
/// ```
#[diagnostic::on_unimplemented(
    message = "the type `{Self}` does not have a default Serde implementation",
    label = "no default Serde association found for this type",
    note = "implement `DefaultSerde` for `{Self}` or specify/override the serde explicitly using `.with_key_serde()` or `.with_value_serde()`"
)]
pub trait DefaultSerde: Send + Sync + 'static + Sized {
    type Serde: SerdeAssociate<Target = Self> + Serde<Self> + Clone + Default + 'static;
}

impl DefaultSerde for String {
    type Serde = StringSerde;
}

impl DefaultSerde for i64 {
    type Serde = I64Serde;
}

impl DefaultSerde for bytes::Bytes {
    type Serde = BytesSerde;
}

impl<KS, VS> From<(KS, VS)> for Consumed<KS, VS> {
    fn from((key_serde, value_serde): (KS, VS)) -> Self {
        Self::with(key_serde, value_serde)
    }
}

impl<KS, VS> From<(KS, VS)> for Produced<KS, VS> {
    fn from((key_serde, value_serde): (KS, VS)) -> Self {
        Self::with(key_serde, value_serde)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn string_serde_round_trips() {
        let s = StringSerde;
        let b = s.serialize(&"héllo".to_string());
        check!(s.deserialize(&b).unwrap() == "héllo");
    }

    #[test]
    fn i64_serde_is_big_endian_8_bytes() {
        let s = I64Serde;
        let b = s.serialize(&1i64);
        check!(b.as_ref() == [0, 0, 0, 0, 0, 0, 0, 1]);
        check!(s.deserialize(&b).unwrap() == 1);
        check!(s.deserialize(&[0, 1]).is_err());
    }

    #[test]
    fn bytes_serde_is_identity() {
        let s = BytesSerde;
        let b = s.serialize(&bytes::Bytes::from_static(b"xy"));
        check!(s.deserialize(&b).unwrap() == bytes::Bytes::from_static(b"xy"));
    }
}
