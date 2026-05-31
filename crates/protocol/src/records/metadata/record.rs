//! Dispatch enum over the generated KIP-631 record types, keyed by the `KRaft`
//! metadata record apiKey (a namespace distinct from RPC apiKeys). Encodes and
//! decodes through the value envelope. Unknown apiKeys decode to `Unknown` so a
//! forward-compatible reader never chokes.
//!
//! Multi-version records (`RegisterBroker`, `Partition`,
//! `BrokerRegistrationChange`, …) must re-encode at the same apiVersion they
//! were decoded at for a faithful byte round-trip. So
//! [`KraftMetadataRecord::decode_value`] returns the decoded apiVersion
//! alongside the record, and [`KraftMetadataRecord::encode_value`] takes the
//! target apiVersion as an argument.

use bytes::{Bytes, BytesMut};

use crate::owned::begin_transaction_record::BeginTransactionRecord;
use crate::owned::broker_registration_change_record::BrokerRegistrationChangeRecord;
use crate::owned::end_transaction_record::EndTransactionRecord;
use crate::owned::feature_level_record::FeatureLevelRecord;
use crate::owned::no_op_record::NoOpRecord;
use crate::owned::partition_record::PartitionRecord;
use crate::owned::register_broker_record::RegisterBrokerRecord;
use crate::owned::register_controller_record::RegisterControllerRecord;
use crate::owned::remove_topic_record::RemoveTopicRecord;
use crate::owned::topic_record::TopicRecord;
use crate::records::metadata::envelope::{decode_value_header, encode_value};
use crate::{Decode, Encode, ProtocolError};

/// A single `KRaft` metadata record (the value of one Kafka `Record`).
#[derive(Debug, Clone, PartialEq)]
pub enum KraftMetadataRecord {
    RegisterBroker(RegisterBrokerRecord),         // apiKey 0
    Topic(TopicRecord),                           // apiKey 1
    Partition(PartitionRecord),                   // apiKey 2
    RemoveTopic(RemoveTopicRecord),               // apiKey 3
    BeginTransaction(BeginTransactionRecord),     // apiKey 4
    EndTransaction(EndTransactionRecord),         // apiKey 5
    NoOp(NoOpRecord),                             // apiKey 6
    RegisterController(RegisterControllerRecord), // apiKey 7
    BrokerRegistrationChange(BrokerRegistrationChangeRecord), // apiKey 8
    FeatureLevel(FeatureLevelRecord),             // apiKey 12
    /// A record this build does not model. Body is the post-envelope bytes.
    Unknown {
        api_key: u32,
        api_version: u32,
        body: Bytes,
    },
}

/// Widen a record apiVersion to the envelope's unsigned representation. Negative
/// versions are not meaningful for metadata records, so they clamp to 0.
fn api_version_to_u32(version: i16) -> u32 {
    u32::try_from(version).unwrap_or(0)
}

/// Narrow the envelope's apiVersion to the `i16` the generated codecs take.
///
/// # Errors
/// Returns [`ProtocolError::SchemaMismatch`] if the declared version does not
/// fit in `i16` (no real metadata record version does).
fn api_version_to_i16(version: u32) -> Result<i16, ProtocolError> {
    i16::try_from(version).map_err(|_| ProtocolError::SchemaMismatch("metadata record apiVersion"))
}

impl KraftMetadataRecord {
    /// The fixed metadata-record apiKey this variant encodes as. The apiVersion
    /// is a runtime value (carried by the envelope), not part of this mapping.
    #[must_use]
    pub fn api_key(&self) -> u32 {
        match self {
            Self::RegisterBroker(_) => 0,
            Self::Topic(_) => 1,
            Self::Partition(_) => 2,
            Self::RemoveTopic(_) => 3,
            Self::BeginTransaction(_) => 4,
            Self::EndTransaction(_) => 5,
            Self::NoOp(_) => 6,
            Self::RegisterController(_) => 7,
            Self::BrokerRegistrationChange(_) => 8,
            Self::FeatureLevel(_) => 12,
            Self::Unknown { api_key, .. } => *api_key,
        }
    }

    /// Encode this record to its value bytes (envelope + body) at `api_version`.
    ///
    /// For a faithful round-trip, pass the apiVersion returned by
    /// [`Self::decode_value`]. For freshly built records, pass the version you
    /// intend to write (`FeatureLevelRecord` is always apiVersion 0).
    ///
    /// # Errors
    /// Propagates a [`ProtocolError`] from the underlying message encoder.
    pub fn encode_value(&self, api_version: i16) -> Result<Bytes, ProtocolError> {
        let api_key = self.api_key();
        let v = api_version;
        let mut body = BytesMut::new();
        match self {
            Self::RegisterBroker(r) => r.encode(&mut body, v)?,
            Self::Topic(r) => r.encode(&mut body, v)?,
            Self::Partition(r) => r.encode(&mut body, v)?,
            Self::RemoveTopic(r) => r.encode(&mut body, v)?,
            Self::BeginTransaction(r) => r.encode(&mut body, v)?,
            Self::EndTransaction(r) => r.encode(&mut body, v)?,
            Self::NoOp(r) => r.encode(&mut body, v)?,
            Self::RegisterController(r) => r.encode(&mut body, v)?,
            Self::BrokerRegistrationChange(r) => r.encode(&mut body, v)?,
            Self::FeatureLevel(r) => r.encode(&mut body, v)?,
            Self::Unknown { body: raw, .. } => {
                return Ok(encode_value(api_key, api_version_to_u32(api_version), raw));
            }
        }
        Ok(encode_value(
            api_key,
            api_version_to_u32(api_version),
            &body,
        ))
    }

    /// Decode one record from its value bytes, returning the record and the
    /// apiVersion the envelope declared (needed to re-encode byte-identically).
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] if the envelope or body cannot be decoded.
    pub fn decode_value(value: &[u8]) -> Result<(Self, i16), ProtocolError> {
        let mut cur: &[u8] = value;
        let hdr = decode_value_header(&mut cur)
            .map_err(|_| ProtocolError::SchemaMismatch("metadata record envelope"))?;
        let v = api_version_to_i16(hdr.api_version)?;
        let rec = match hdr.api_key {
            0 => Self::RegisterBroker(RegisterBrokerRecord::decode(&mut cur, v)?),
            1 => Self::Topic(TopicRecord::decode(&mut cur, v)?),
            2 => Self::Partition(PartitionRecord::decode(&mut cur, v)?),
            3 => Self::RemoveTopic(RemoveTopicRecord::decode(&mut cur, v)?),
            4 => Self::BeginTransaction(BeginTransactionRecord::decode(&mut cur, v)?),
            5 => Self::EndTransaction(EndTransactionRecord::decode(&mut cur, v)?),
            6 => Self::NoOp(NoOpRecord::decode(&mut cur, v)?),
            7 => Self::RegisterController(RegisterControllerRecord::decode(&mut cur, v)?),
            8 => {
                Self::BrokerRegistrationChange(BrokerRegistrationChangeRecord::decode(&mut cur, v)?)
            }
            12 => Self::FeatureLevel(FeatureLevelRecord::decode(&mut cur, v)?),
            other => Self::Unknown {
                api_key: other,
                api_version: hdr.api_version,
                body: Bytes::copy_from_slice(cur),
            },
        };
        Ok((rec, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn feature_level_record_value_roundtrips_through_dispatch() {
        let rec = KraftMetadataRecord::FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".to_string(),
            feature_level: 25,
            ..Default::default()
        });
        let value = rec.encode_value(0).expect("encode");
        let (decoded, ver) = KraftMetadataRecord::decode_value(&value).expect("decode");
        assert!(decoded == rec);
        assert!(ver == 0);
        // Re-encode at the decoded version is byte-identical.
        assert!(decoded.encode_value(ver).expect("re-encode") == value);
    }

    #[test]
    fn unknown_api_key_decodes_to_unknown_arm() {
        use crate::records::metadata::envelope::encode_value;
        // apiKey 99 is not modeled.
        let value = encode_value(99, 0, &[0xAB, 0xCD]);
        let (decoded, ver) = KraftMetadataRecord::decode_value(&value).expect("decode");
        assert!(ver == 0);
        match &decoded {
            KraftMetadataRecord::Unknown {
                api_key,
                api_version,
                body,
            } => {
                assert!(*api_key == 99);
                assert!(*api_version == 0);
                assert!(body.as_ref() == &[0xAB, 0xCD]);
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
        // Unknown re-encodes byte-identically too.
        assert!(decoded.encode_value(ver).expect("re-encode") == value);
    }
}
