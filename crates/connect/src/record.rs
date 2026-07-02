//! The record and offset types that flow across the connector SPI.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A single record crossing the connector boundary, parameterised over its key
/// and value payload types.
///
/// A [`Source`](crate::Source) yields these from `poll`; a [`Sink`](crate::Sink)
/// consumes them in `put`. For a byte-identity connector `K` and `V` are
/// [`Bytes`]; for a schema-aware connector they are the connector's domain
/// types, bridged to the wire by a [`Converter`](crate::Converter).
///
/// Both `key` and `value` are optional: a `None` key matches Kafka's null key,
/// and a `None` value is a tombstone (KIP-87 / compacted-topic delete).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectRecord<K, V> {
    /// The record key, or `None` for a null key.
    pub key: Option<K>,
    /// The record value, or `None` for a tombstone.
    pub value: Option<V>,
    /// The record timestamp in epoch milliseconds, or `None` when the source
    /// cannot surface one (the producer fills wall-clock time, matching
    /// `RecordProducer::send` in the streams runtime).
    pub timestamp: Option<i64>,
    /// Per-record headers, in declaration order. Header keys may repeat, so
    /// this is a `Vec`, not a map (matching Kafka's `RecordHeaders`).
    pub headers: Vec<Header>,
}

impl<K, V> ConnectRecord<K, V> {
    /// A record carrying just a key and value, with no timestamp or headers.
    pub fn new(key: Option<K>, value: Option<V>) -> Self {
        Self {
            key,
            value,
            timestamp: None,
            headers: Vec::new(),
        }
    }

    /// Set the record timestamp (epoch millis).
    #[must_use]
    pub fn with_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.timestamp = Some(timestamp_ms);
        self
    }

    /// Append a header.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: Option<Bytes>) -> Self {
        self.headers.push(Header {
            key: key.into(),
            value,
        });
        self
    }
}

/// A single Kafka record header: a string key and an opaque optional value.
///
/// Mirrors `crabka_protocol::records::RecordHeader` and the producer's `Header`;
/// kept here so the connector SPI does not pull in the wire-record crates.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Header {
    /// The header key. Not required to be unique within a record.
    pub key: String,
    /// The header value, or `None` for a null-valued header.
    pub value: Option<Bytes>,
}

/// A connector-defined source position: *which* stream, and *where* in it.
///
/// The framework treats both maps as opaque — it persists a checkpoint verbatim
/// and hands it back to [`Source::seek`](crate::Source::seek) on restart; only
/// the connector interprets the contents. Mirrors Kafka Connect's split of a
/// `SourceRecord` into a `sourcePartition` and a `sourceOffset`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct SourceOffset {
    /// Identifies the stream this position belongs to — e.g. a database table,
    /// a file path, a log shard. Mirrors Kafka Connect's `sourcePartition`.
    pub partition: OffsetMap,
    /// The position within that stream — e.g. a log sequence number, a byte
    /// offset, a row id. Mirrors Kafka Connect's `sourceOffset`.
    pub position: OffsetMap,
}

impl SourceOffset {
    /// A source offset from a stream-identifying `partition` and a `position`
    /// within it.
    #[must_use]
    pub fn new(partition: OffsetMap, position: OffsetMap) -> Self {
        Self {
            partition,
            position,
        }
    }
}

/// A connector offset map: structured, JSON-serialisable key/value pairs.
/// Ordered (`BTreeMap`) so a serialised checkpoint is byte-stable.
pub type OffsetMap = BTreeMap<String, OffsetValue>;

/// A scalar value in an [`OffsetMap`]. The variants cover the JSON-compatible
/// primitives Kafka Connect offsets are built from, so a checkpoint round-trips
/// through any JSON-based offset store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OffsetValue {
    /// A missing / explicitly-null position component.
    Null,
    /// A boolean flag.
    Bool(bool),
    /// A 64-bit signed integer (LSN, byte offset, row id, …).
    Long(i64),
    /// A 64-bit float.
    Double(f64),
    /// A UTF-8 string (GTID, snapshot name, opaque cursor token, …).
    String(String),
}

impl From<i64> for OffsetValue {
    fn from(v: i64) -> Self {
        OffsetValue::Long(v)
    }
}

impl From<bool> for OffsetValue {
    fn from(v: bool) -> Self {
        OffsetValue::Bool(v)
    }
}

impl From<f64> for OffsetValue {
    fn from(v: f64) -> Self {
        OffsetValue::Double(v)
    }
}

impl From<String> for OffsetValue {
    fn from(v: String) -> Self {
        OffsetValue::String(v)
    }
}

impl From<&str> for OffsetValue {
    fn from(v: &str) -> Self {
        OffsetValue::String(v.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn builder_sets_timestamp_and_headers() {
        let r = ConnectRecord::new(
            Some(Bytes::from_static(b"k")),
            Some(Bytes::from_static(b"v")),
        )
        .with_timestamp(42)
        .with_header("h", Some(Bytes::from_static(b"hv")));
        check!(r.timestamp == Some(42));
        check!(r.headers.len() == 1);
        check!(r.headers[0].key == "h");
        check!(r.headers[0].value == Some(Bytes::from_static(b"hv")));
    }

    #[test]
    fn tombstone_value_is_none() {
        let r: ConnectRecord<Bytes, Bytes> =
            ConnectRecord::new(Some(Bytes::from_static(b"k")), None);
        check!(r.value.is_none());
    }

    #[test]
    fn source_offset_round_trips_through_json() {
        let mut partition = OffsetMap::new();
        partition.insert("table".into(), "public.orders".into());
        let mut position = OffsetMap::new();
        position.insert("lsn".into(), 12_345_i64.into());
        position.insert("snapshot".into(), OffsetValue::Bool(false));
        let offset = SourceOffset::new(partition, position);

        let json = serde_json::to_string(&offset).expect("serialize");
        let back: SourceOffset = serde_json::from_str(&json).expect("deserialize");
        check!(back == offset);
    }

    #[test]
    fn offset_value_from_conversions() {
        check!(OffsetValue::from(7_i64) == OffsetValue::Long(7));
        check!(OffsetValue::from(true) == OffsetValue::Bool(true));
        check!(OffsetValue::from(1.5_f64) == OffsetValue::Double(1.5));
        check!(OffsetValue::from("owned".to_owned()) == OffsetValue::String("owned".into()));
        check!(OffsetValue::from("borrowed") == OffsetValue::String("borrowed".into()));
    }

    #[test]
    fn offset_map_is_byte_stable_across_insertion_order() {
        // BTreeMap ordering makes a serialized checkpoint deterministic
        // regardless of the order the connector recorded components.
        let mut a = OffsetMap::new();
        a.insert("b".into(), 2_i64.into());
        a.insert("a".into(), 1_i64.into());
        let mut b = OffsetMap::new();
        b.insert("a".into(), 1_i64.into());
        b.insert("b".into(), 2_i64.into());
        check!(serde_json::to_string(&a).unwrap() == serde_json::to_string(&b).unwrap());
    }
}
