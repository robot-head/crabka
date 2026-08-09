//! Bridge between [`MetadataRecord`] and the Kafka `Record` wire type.
//!
//! Broker-only observers fetch `__cluster_metadata` as Kafka
//! record batches (Component B). Each [`MetadataRecord`] maps to exactly
//! one [`Record`]: `key = None`, `value = wincode(MetadataRecord)`. The
//! enum variant itself is the record type + version, so no separate type
//! tag is carried. This wire surface is crabka-private (clients never
//! fetch `__cluster_metadata`), so it only needs to be stable and
//! round-trippable, and not byte-identical to Apache Kafka's `ApiMessage`
//! framing.

use bytes::Bytes;
use crabka_protocol::records::Record;
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as _, Serialize as _};

use crate::records::MetadataRecord;

/// Error decoding a `Record` back into a [`MetadataRecord`].
#[derive(Debug, thiserror::Error)]
pub enum KafkaRecordError {
    #[error("metadata record has no value payload")]
    MissingValue,
    #[error("wincode decode failed: {0}")]
    Decode(String),
    #[error("wincode encode failed: {0}")]
    Encode(String),
}

/// Encode one [`MetadataRecord`] as a Kafka [`Record`].
///
/// # Errors
/// Returns [`KafkaRecordError::Encode`] if wincode serialization fails. In
/// practice this cannot happen for `MetadataRecord`.
pub fn to_kafka_record(rec: &MetadataRecord) -> Result<Record, KafkaRecordError> {
    let payload = <SerdeCompat<MetadataRecord>>::serialize(rec)
        .map_err(|e| KafkaRecordError::Encode(e.to_string()))?;
    Ok(Record {
        value: Some(Bytes::from(payload)),
        ..Default::default()
    })
}

/// Decode a Kafka [`Record`] back into a [`MetadataRecord`].
///
/// # Errors
/// - [`KafkaRecordError::MissingValue`] if the record carries no value.
/// - [`KafkaRecordError::Decode`] if the value is not a valid
///   wincode-encoded `MetadataRecord`.
pub fn from_kafka_record(rec: &Record) -> Result<MetadataRecord, KafkaRecordError> {
    let value = rec.value.as_ref().ok_or(KafkaRecordError::MissingValue)?;
    <SerdeCompat<MetadataRecord>>::deserialize(value)
        .map_err(|e| KafkaRecordError::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {

    use uuid::Uuid;

    use super::*;
    use crate::records::{MetadataRecord, TopicRecord};

    fn sample_topic() -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::from_u128(0x1234_5678_9abc_def0),
            partitions: 6,
            replication_factor: 3,
        })
    }

    #[test]
    fn round_trips_through_kafka_record() {
        let rec = sample_topic();
        let kafka = to_kafka_record(&rec).expect("encode");
        let back = from_kafka_record(&kafka).expect("decode");
        assert2::assert!(rec == back);
    }

    #[test]
    fn key_is_none_and_value_is_present() {
        let kafka = to_kafka_record(&sample_topic()).expect("encode");
        assert2::assert!(kafka.key == None);
        assert2::assert!(kafka.value.is_some());
    }

    #[test]
    fn record_key_is_none_even_when_default_record_has_key() {
        let kafka = to_kafka_record(&sample_topic()).expect("encode");
        assert2::assert!(kafka.key == None);
    }

    #[test]
    fn missing_value_is_an_error() {
        let empty = Record::default();
        assert2::assert!(matches!(
            from_kafka_record(&empty),
            Err(KafkaRecordError::MissingValue)
        ));
    }

    #[test]
    fn encoding_is_stable_across_calls() {
        // Stability guard: the same record must always produce the same
        // value bytes, so an observer fetching twice sees identical frames.
        let rec = sample_topic();
        let a = to_kafka_record(&rec).expect("encode");
        let b = to_kafka_record(&rec).expect("encode");
        assert2::assert!(a.value == b.value);
    }
}
