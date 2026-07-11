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
    fn serialize(&self, topic: &str, value: &T) -> Bytes;
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, SerdeError>;

    /// Pre-register any per-topic state (e.g. a schema-registry subject) before
    /// processing begins. Called once per `(topic, role)` at topology wiring
    /// time. Default: no-op (stateless serdes need nothing).
    fn prepare(&self, _topic: &str, _role: SerdeRole) {}
}

/// Whether a serde is wired for the record key or value — passed to
/// [`Serde::prepare`] so schema serdes can derive `<topic>-key` / `<topic>-value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerdeRole {
    Key,
    Value,
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
    fn serialize(&self, topic: &str, value: &T) -> Bytes {
        self.0.serialize(topic, value)
    }
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<T, SerdeError> {
        self.0.deserialize(topic, bytes)
    }
    fn prepare(&self, topic: &str, role: SerdeRole) {
        self.0.prepare(topic, role);
    }
}

impl<T: Send + Sync + 'static> SerdeAssociate for SerdeArc<T> {
    type Target = T;
}

/// Identity serde for raw `Bytes`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerde;
impl Serde<Bytes> for BytesSerde {
    fn serialize(&self, _topic: &str, value: &Bytes) -> Bytes {
        value.clone()
    }
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<Bytes, SerdeError> {
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
    fn serialize(&self, _topic: &str, value: &String) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<String, SerdeError> {
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
    fn serialize(&self, _topic: &str, value: &i64) -> Bytes {
        Bytes::copy_from_slice(&value.to_be_bytes())
    }
    fn deserialize(&self, _topic: &str, bytes: &[u8]) -> Result<i64, SerdeError> {
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
/// If a type does not implement this trait, using `.stream` or other ergonomic
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
/// let stream: KStream<CustomType, String> = builder.stream(["topic"]);
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

/// Write a Kafka signed/zigzag varint (same encoding as `ByteUtils.writeVarint` in the JVM).
///
/// The value is zigzag-encoded (`n << 1 ^ n >> 31` for 32-bit equivalent) then
/// written as a base-128 little-endian varint.  Values ≤ 63 encode to one byte.
fn write_varint(n: usize, buf: &mut Vec<u8>) {
    // Zigzag encode: treat as i64 equivalent — all inputs here are non-negative
    // lengths, so the zigzag is simply n << 1 (sign bit = 0).
    let mut v = (n as u64) << 1;
    loop {
        if v < 0x80 {
            buf.push(v.to_le_bytes()[0]);
            break;
        }
        buf.push(v.to_le_bytes()[0] | 0x80);
        v >>= 7;
    }
}

/// Read a Kafka signed/zigzag varint.
///
/// Returns `(decoded_value, bytes_consumed)`.
fn read_varint(bytes: &[u8]) -> Result<(usize, usize), SerdeError> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut consumed = 0;
    for &b in bytes {
        consumed += 1;
        let low7 = u64::from(b & 0x7F);
        value |= low7 << shift;
        shift += 7;
        if b & 0x80 == 0 {
            // Zigzag decode: (value >>> 1) ^ -(value & 1)
            let decoded = (value >> 1) ^ ((value & 1).wrapping_neg());
            let n = usize::try_from(decoded)
                .map_err(|_| SerdeError(format!("varint value {decoded} out of usize range")))?;
            return Ok((n, consumed));
        }
        if shift >= 64 {
            return Err(SerdeError("varint overflow".to_string()));
        }
    }
    Err(SerdeError("truncated varint".to_string()))
}

/// [`Serde`] for `Change<V>` — byte-compatible with the JVM
/// `ChangedSerializer`/`ChangedDeserializer` used on repartition topics when a
/// `KGroupedTable` re-keys.
///
/// # Wire format
///
/// | Case | Layout |
/// |------|--------|
/// | new only | `new_bytes ++ [0x01]` |
/// | old only | `old_bytes ++ [0x00]` |
/// | both     | `varint(new_len) ++ new_bytes ++ old_bytes ++ [0x02]` |
///
/// The `varint` is a Kafka signed/zigzag varint (`ByteUtils.writeVarint` / `readVarint`).
#[derive(Debug, Clone, Copy)]
pub struct Changed<S> {
    inner: S,
}

impl<S> Changed<S> {
    /// Wrap an inner serde to produce a `Changed<V>` serde.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<V, S> Serde<crate::dsl::processors::change::Change<V>> for Changed<S>
where
    V: Send + Sync + 'static,
    S: Serde<V>,
{
    fn serialize(&self, topic: &str, value: &crate::dsl::processors::change::Change<V>) -> Bytes {
        match (&value.new, &value.old) {
            (Some(new), Some(old)) => {
                let new_bytes = self.inner.serialize(topic, new);
                let old_bytes = self.inner.serialize(topic, old);
                let mut buf = Vec::with_capacity(1 + new_bytes.len() + old_bytes.len() + 1);
                write_varint(new_bytes.len(), &mut buf);
                buf.extend_from_slice(&new_bytes);
                buf.extend_from_slice(&old_bytes);
                buf.push(0x02);
                Bytes::from(buf)
            }
            (Some(new), None) => {
                let new_bytes = self.inner.serialize(topic, new);
                let mut buf = Vec::with_capacity(new_bytes.len() + 1);
                buf.extend_from_slice(&new_bytes);
                buf.push(0x01);
                Bytes::from(buf)
            }
            (None, Some(old)) => {
                let old_bytes = self.inner.serialize(topic, old);
                let mut buf = Vec::with_capacity(old_bytes.len() + 1);
                buf.extend_from_slice(&old_bytes);
                buf.push(0x00);
                Bytes::from(buf)
            }
            // The repartition-map only ever forwards combined, subtract-only, or
            // add-only changes — never a both-`None` change, and the JVM
            // `ChangedSerializer` likewise rejects an all-null change. Fail loud
            // rather than silently emit an unsymmetric encoding.
            (None, None) => unreachable!("Changed serde never carries a both-None change"),
        }
    }

    fn deserialize(
        &self,
        topic: &str,
        bytes: &[u8],
    ) -> Result<crate::dsl::processors::change::Change<V>, SerdeError> {
        if bytes.is_empty() {
            return Err(SerdeError("empty bytes for Change".to_string()));
        }
        let flag = bytes[bytes.len() - 1];
        let payload = &bytes[..bytes.len() - 1];
        match flag {
            0x00 => {
                let old = self.inner.deserialize(topic, payload)?;
                Ok(crate::dsl::processors::change::Change {
                    old: Some(old),
                    new: None,
                })
            }
            0x01 => {
                let new = self.inner.deserialize(topic, payload)?;
                Ok(crate::dsl::processors::change::Change {
                    old: None,
                    new: Some(new),
                })
            }
            0x02 => {
                let (new_len, consumed) = read_varint(payload)?;
                let after_varint = &payload[consumed..];
                if after_varint.len() < new_len {
                    return Err(SerdeError(format!(
                        "not enough bytes for new value: need {new_len}, have {}",
                        after_varint.len()
                    )));
                }
                let new_bytes = &after_varint[..new_len];
                let old_bytes = &after_varint[new_len..];
                let new = self.inner.deserialize(topic, new_bytes)?;
                let old = self.inner.deserialize(topic, old_bytes)?;
                Ok(crate::dsl::processors::change::Change {
                    old: Some(old),
                    new: Some(new),
                })
            }
            other => Err(SerdeError(format!("unknown Change flag: 0x{other:02x}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn string_serde_round_trips() {
        let s = StringSerde;
        let b = s.serialize("t", &"héllo".to_string());
        check!(s.deserialize("t", &b).unwrap() == "héllo");
    }

    #[test]
    fn i64_serde_is_big_endian_8_bytes() {
        let s = I64Serde;
        let b = s.serialize("t", &1i64);
        check!(b.as_ref() == [0, 0, 0, 0, 0, 0, 0, 1]);
        check!(s.deserialize("t", &b).unwrap() == 1);
        check!(s.deserialize("t", &[0, 1]).is_err());
    }

    #[test]
    fn bytes_serde_is_identity() {
        let s = BytesSerde;
        let b = s.serialize("t", &bytes::Bytes::from_static(b"xy"));
        check!(s.deserialize("t", &b).unwrap() == bytes::Bytes::from_static(b"xy"));
    }

    fn hex(b: &[u8]) -> String {
        b.iter().fold(String::new(), |mut s, x| {
            use std::fmt::Write as _;
            write!(s, "{x:02x}").unwrap();
            s
        })
    }

    fn golden() -> serde_json::Value {
        let raw = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/testdata/kgrouped_table/changed_bytes.json"
        ))
        .expect("read changed_bytes golden");
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn changed_long_matches_jvm_bytes() {
        let g = golden();
        let s = Changed::new(I64Serde);
        let both = crate::dsl::processors::change::Change {
            old: Some(2i64),
            new: Some(6i64),
        };
        let new_only = crate::dsl::processors::change::Change {
            old: None,
            new: Some(5i64),
        };
        let old_only = crate::dsl::processors::change::Change {
            old: Some(4i64),
            new: None,
        };
        check!(hex(&s.serialize("topic", &both)) == g["both"].as_str().unwrap());
        check!(hex(&s.serialize("topic", &new_only)) == g["new_only"].as_str().unwrap());
        check!(hex(&s.serialize("topic", &old_only)) == g["old_only"].as_str().unwrap());
    }

    #[test]
    fn changed_round_trips() {
        let s = Changed::new(I64Serde);
        for c in [
            crate::dsl::processors::change::Change {
                old: Some(2i64),
                new: Some(6i64),
            },
            crate::dsl::processors::change::Change {
                old: None,
                new: Some(5i64),
            },
            crate::dsl::processors::change::Change {
                old: Some(4i64),
                new: None,
            },
        ] {
            let bytes = s.serialize("topic", &c);
            let back: crate::dsl::processors::change::Change<i64> =
                s.deserialize("topic", &bytes).unwrap();
            check!(back == c);
        }
    }
}
