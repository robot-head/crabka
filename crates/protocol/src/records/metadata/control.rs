//! `KRaft` control-record framing. Control records live in a batch with the
//! control bit set; the record key is `version (i16) + type (i16)` and the
//! value is the generated message body.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    Decode, Encode, ProtocolError,
    owned::{
        k_raft_version_record::KRaftVersionRecord, leader_change_message::LeaderChangeMessage,
        snapshot_footer_record::SnapshotFooterRecord, snapshot_header_record::SnapshotHeaderRecord,
        voters_record::VotersRecord,
    },
    records::{Attributes, Record, RecordBatch},
};

/// `KRaft` control record types (the i16 written after the i16 version in the
/// key).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ControlRecordType {
    LeaderChange = 2,
    SnapshotHeader = 3,
    SnapshotFooter = 4,
    KRaftVersion = 5,
    Voters = 6,
}

impl TryFrom<i16> for ControlRecordType {
    type Error = ProtocolError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::LeaderChange),
            3 => Ok(Self::SnapshotHeader),
            4 => Ok(Self::SnapshotFooter),
            5 => Ok(Self::KRaftVersion),
            6 => Ok(Self::Voters),
            _ => Err(ProtocolError::InvalidValue("unknown control record type")),
        }
    }
}

/// A decoded `KRaft` control record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRecord {
    LeaderChange(LeaderChangeMessage),
    SnapshotHeader(SnapshotHeaderRecord),
    SnapshotFooter(SnapshotFooterRecord),
    KRaftVersion(KRaftVersionRecord),
    Voters(VotersRecord),
}

/// Control record key version (Kafka writes 0).
const CONTROL_KEY_VERSION: i16 = 0;

/// Build a control record key: `version(i16) + type(i16)`.
#[must_use]
pub fn control_record_key(ty: ControlRecordType) -> Bytes {
    let mut key = BytesMut::with_capacity(4);
    key.put_i16(CONTROL_KEY_VERSION);
    key.put_i16(ty as i16);
    key.freeze()
}

/// Decode a control-record key.
///
/// # Errors
/// Returns a [`ProtocolError`] when the key is truncated, uses an unsupported
/// key version, names an unknown record type, or contains trailing bytes.
pub fn decode_control_record_key(key: &[u8]) -> Result<ControlRecordType, ProtocolError> {
    let mut cur = key;
    if cur.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 4 - cur.remaining(),
        });
    }
    let version = cur.get_i16();
    if version != CONTROL_KEY_VERSION {
        return Err(ProtocolError::InvalidValue(
            "unsupported control record key version",
        ));
    }
    let record_type = ControlRecordType::try_from(cur.get_i16())?;
    if cur.has_remaining() {
        return Err(ProtocolError::InvalidValue(
            "trailing bytes after control record key",
        ));
    }
    Ok(record_type)
}

impl ControlRecord {
    /// The Kafka control-record type encoded in this record's key.
    #[must_use]
    pub fn record_type(&self) -> ControlRecordType {
        match self {
            Self::LeaderChange(_) => ControlRecordType::LeaderChange,
            Self::SnapshotHeader(_) => ControlRecordType::SnapshotHeader,
            Self::SnapshotFooter(_) => ControlRecordType::SnapshotFooter,
            Self::KRaftVersion(_) => ControlRecordType::KRaftVersion,
            Self::Voters(_) => ControlRecordType::Voters,
        }
    }

    /// Encode this record's key and value exactly as Kafka writes them.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] if the embedded record version is outside
    /// the generated message's supported range.
    pub fn encode_key_value(&self) -> Result<(Bytes, Bytes), ProtocolError> {
        let mut value = BytesMut::new();
        match self {
            Self::LeaderChange(record) => record.encode(&mut value, record.version)?,
            Self::SnapshotHeader(record) => record.encode(&mut value, record.version)?,
            Self::SnapshotFooter(record) => record.encode(&mut value, record.version)?,
            Self::KRaftVersion(record) => record.encode(&mut value, record.version)?,
            Self::Voters(record) => record.encode(&mut value, record.version)?,
        }
        Ok((control_record_key(self.record_type()), value.freeze()))
    }

    /// Decode a Kafka control-record key and value.
    ///
    /// The first `i16` in every supported control-record value is its schema
    /// version. The generated codec for that version must consume the complete
    /// value.
    ///
    /// # Errors
    /// Returns a [`ProtocolError`] for malformed keys, truncated values,
    /// unsupported record versions, or trailing value bytes.
    pub fn decode(key: &[u8], value: &[u8]) -> Result<Self, ProtocolError> {
        let record_type = decode_control_record_key(key)?;
        if value.len() < 2 {
            return Err(ProtocolError::UnexpectedEof {
                needed: 2 - value.len(),
            });
        }
        let version = i16::from_be_bytes([value[0], value[1]]);
        let mut cur = value;
        let record = match record_type {
            ControlRecordType::LeaderChange => {
                Self::LeaderChange(LeaderChangeMessage::decode(&mut cur, version)?)
            }
            ControlRecordType::SnapshotHeader => {
                Self::SnapshotHeader(SnapshotHeaderRecord::decode(&mut cur, version)?)
            }
            ControlRecordType::SnapshotFooter => {
                Self::SnapshotFooter(SnapshotFooterRecord::decode(&mut cur, version)?)
            }
            ControlRecordType::KRaftVersion => {
                Self::KRaftVersion(KRaftVersionRecord::decode(&mut cur, version)?)
            }
            ControlRecordType::Voters => Self::Voters(VotersRecord::decode(&mut cur, version)?),
        };
        if !cur.is_empty() {
            return Err(ProtocolError::InvalidValue(
                "trailing bytes after control record value",
            ));
        }
        Ok(record)
    }
}

/// Encode a typed control record as a single-record control batch.
///
/// # Errors
/// Returns a [`ProtocolError`] if the record's embedded version is unsupported.
pub fn encode_typed_control_batch(
    base_offset: i64,
    record: &ControlRecord,
) -> Result<Bytes, ProtocolError> {
    let (key, value) = record.encode_key_value()?;
    Ok(encode_control_batch(base_offset, key, value))
}

/// Encode a single-record control batch at `base_offset` with the control bit
/// set, returning the full v2 `RecordBatch` bytes (CRC computed by the encoder).
///
/// # Panics
/// Panics only if the underlying record-batch encoder fails, which cannot happen
/// for an uncompressed in-range single-record batch.
#[must_use]
pub fn encode_control_batch(base_offset: i64, key: Bytes, value: Bytes) -> Bytes {
    let batch = RecordBatch {
        base_offset,
        attributes: Attributes::default().with_control(true),
        records: vec![Record {
            key: Some(key),
            value: Some(value),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut out = BytesMut::new();
    batch
        .encode(&mut out)
        .expect("control batch encodes (no compression, in-range)");
    out.freeze()
}

#[cfg(test)]
mod tests {

    use bytes::Buf;

    use super::*;
    use crate::{
        UnknownTaggedFields,
        owned::voters_record::{Endpoint, KRaftVersionFeature, Voter},
        primitives::uuid::Uuid,
    };

    #[test]
    fn control_key_is_version_then_type() {
        let cases = [
            (ControlRecordType::LeaderChange, 2),
            (ControlRecordType::SnapshotHeader, 3),
            (ControlRecordType::SnapshotFooter, 4),
            (ControlRecordType::KRaftVersion, 5),
            (ControlRecordType::Voters, 6),
        ];
        for (record_type, number) in cases {
            let key = control_record_key(record_type);
            let mut cur: &[u8] = &key;
            assert2::assert!((cur.get_i16(), cur.get_i16()) == (0, number));
            assert2::assert!(decode_control_record_key(&key).expect("decode key") == record_type);
        }
    }

    #[test]
    fn kip853_control_records_are_byte_exact_and_roundtrip() {
        let version_record = ControlRecord::KRaftVersion(KRaftVersionRecord {
            version: 0,
            k_raft_version: 1,
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        });
        let (version_key, version_value) = version_record.encode_key_value().expect("encode");
        assert2::assert!(&version_key[..] == &[0, 0, 0, 5]);
        assert2::assert!(&version_value[..] == &[0, 0, 0, 1, 0]);
        assert2::assert!(
            ControlRecord::decode(&version_key, &version_value).expect("decode") == version_record
        );

        let voters_record = ControlRecord::Voters(VotersRecord {
            version: 0,
            voters: vec![Voter {
                voter_id: 7,
                voter_directory_id: Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                endpoints: vec![Endpoint {
                    name: "CONTROLLER".to_string(),
                    host: "localhost".to_string(),
                    port: 9_093,
                    ..Default::default()
                }],
                k_raft_version_feature: KRaftVersionFeature {
                    min_supported_version: 0,
                    max_supported_version: 1,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let (voters_key, voters_value) = voters_record.encode_key_value().expect("encode");
        let expected_value = hex::decode(concat!(
            "0000",                             // record version
            "02",                               // one voter (compact array)
            "00000007",                         // voter id
            "000102030405060708090a0b0c0d0e0f", // directory id
            "02",                               // one endpoint (compact array)
            "0b434f4e54524f4c4c4552",           // CONTROLLER
            "0a6c6f63616c686f7374",             // localhost
            "2385",                             // port 9093
            "00",                               // endpoint tagged fields
            "00000001",                         // supported versions 0..=1
            "00",                               // feature tagged fields
            "00",                               // voter tagged fields
            "00",                               // record tagged fields
        ))
        .expect("valid hex");
        assert2::assert!(&voters_key[..] == &[0, 0, 0, 6]);
        assert2::assert!(&voters_value[..] == expected_value);
        assert2::assert!(
            ControlRecord::decode(&voters_key, &voters_value).expect("decode") == voters_record
        );
    }

    #[test]
    fn malformed_control_records_are_rejected() {
        let bad_keys: &[&[u8]] = &[
            &[0, 0, 0],       // truncated
            &[0, 1, 0, 5],    // unsupported key version
            &[0, 0, 0, 7],    // unknown type
            &[0, 0, 0, 5, 0], // trailing byte
        ];
        for key in bad_keys {
            assert2::assert!(decode_control_record_key(key).is_err());
        }

        let key = control_record_key(ControlRecordType::KRaftVersion);
        assert2::assert!(ControlRecord::decode(&key, &[0]).is_err());
        assert2::assert!(ControlRecord::decode(&key, &[0, 0, 0, 1, 0, 0]).is_err());
    }

    #[test]
    fn control_batch_sets_control_bit() {
        let key = control_record_key(ControlRecordType::LeaderChange);
        let batch = encode_control_batch(42, key, bytes::Bytes::from_static(b"\x00\x00"));
        let mut cur: &[u8] = &batch;
        let decoded = RecordBatch::decode(&mut cur).expect("control batch decodes");
        assert2::assert!(decoded.base_offset == 42);
        assert2::assert!(cur.is_empty());
        // magic byte at offset 16, attributes i16 at offset 21..23; control bit = 0x20.
        let attrs = i16::from_be_bytes([batch[21], batch[22]]);
        assert2::assert!(attrs & 0x20 != 0);
    }
}
