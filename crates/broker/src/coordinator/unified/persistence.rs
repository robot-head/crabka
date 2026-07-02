//! Wire-byte codecs for the `__consumer_offsets` internal topic.
//!
//! The topic carries two kinds of records, discriminated by the first
//! `i16` of the key:
//!
//! - **`OffsetCommit`** (key version `0` or `1`) — one record per
//!   `(group_id, topic, partition)` committed offset.
//! - **`GroupMetadata`** (key version `2`) — one record per group state
//!   snapshot, written at the end of every successful rebalance.
//!
//! Field layouts mirror Apache Kafka's
//! `clients/src/main/resources/common/message/OffsetCommitValue.json` and
//! `GroupMetadataValue.json` (with the legacy non-flexible encoding —
//! `__consumer_offsets` records are NOT flexible).

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::BrokerError;

/// Discriminator returned by [`parse_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    /// `(group_id, topic, partition)` — what offset was committed.
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: i32,
    },
    /// Just `group_id` — value carries the whole `GroupMetadataValue`.
    GroupMetadata { group_id: String },
    /// KIP-848 next-gen consumer group record types (versions 3, 5–8).
    NextGen(crate::coordinator::unified::persistence_next_gen::NextGenKey),
    /// KIP-932 share-group record types (versions 9–13).
    Share(crate::coordinator::unified::share::persistence::ShareGroupKey),
    /// KIP-1071 streams-group record types (versions 15–21).
    Streams(crate::coordinator::unified::streams::persistence::StreamsGroupKey),
}

pub fn parse_key(mut buf: &[u8]) -> Result<Key, BrokerError> {
    if buf.remaining() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets key too short"),
        ));
    }
    let version = buf.get_i16();
    match version {
        0 | 1 => {
            let group_id = get_string(&mut buf)?;
            let topic = get_string(&mut buf)?;
            let partition = get_i32(&mut buf)?;
            Ok(Key::OffsetCommit {
                group_id,
                topic,
                partition,
            })
        }
        2 => {
            let group_id = get_string(&mut buf)?;
            Ok(Key::GroupMetadata { group_id })
        }
        3 | 5 | 6 | 7 | 8 => Ok(Key::NextGen(
            crate::coordinator::unified::persistence_next_gen::parse_key(version, buf)?,
        )),
        9..=14 => Ok(Key::Share(
            crate::coordinator::unified::share::persistence::parse_share_key(version, buf)?,
        )),
        15..=21 => Ok(Key::Streams(
            crate::coordinator::unified::streams::persistence::parse_streams_key(version, buf)?,
        )),
        _ => Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("unknown __consumer_offsets key version"),
        )),
    }
}

/// Encode a [`Key`] back to its `__consumer_offsets` wire bytes, symmetric to
/// [`parse_key`]. The k0/k1 `OffsetCommit` and k2 `GroupMetadata` variants are
/// encoded here; the next-gen / share / streams families delegate to their own
/// `encode_*` helpers.
#[must_use]
pub fn encode_key(key: &Key) -> Bytes {
    match key {
        Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } => OffsetCommitValue::encode_key(group_id, topic, *partition),
        Key::GroupMetadata { group_id } => GroupMetadataValue::encode_key(group_id),
        Key::NextGen(k) => crate::coordinator::unified::persistence_next_gen::encode_key(k),
        Key::Share(k) => crate::coordinator::unified::share::persistence::encode_share_key(k),
        Key::Streams(k) => crate::coordinator::unified::streams::persistence::encode_streams_key(k),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitValue {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
}

impl OffsetCommitValue {
    /// Encode an `OffsetCommit` key (version 1).
    #[must_use]
    pub fn encode_key(group_id: &str, topic: &str, partition: i32) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // key version
        put_string(&mut buf, group_id);
        put_string(&mut buf, topic);
        buf.put_i32(partition);
        buf.freeze()
    }

    /// Encode an `OffsetCommit` value (version 3).
    #[must_use]
    pub fn encode_value(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // value version
        buf.put_i64(self.offset);
        buf.put_i32(self.leader_epoch);
        put_string(&mut buf, &self.metadata);
        buf.put_i64(self.commit_timestamp_ms);
        buf.freeze()
    }

    pub fn decode_value(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let version = get_i16(&mut buf)?;
        if !(0..=3).contains(&version) {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue("unknown OffsetCommitValue version"),
            ));
        }
        let offset = get_i64(&mut buf)?;
        let leader_epoch = if version >= 3 { get_i32(&mut buf)? } else { -1 };
        let metadata = get_string(&mut buf)?;
        let commit_timestamp_ms = get_i64(&mut buf)?;
        // Older versions carried an `expire_timestamp_ms` after commit_timestamp_ms;
        // ignore any trailing bytes.
        Ok(Self {
            offset,
            leader_epoch,
            metadata,
            commit_timestamp_ms,
        })
    }
}

// The k2 `GroupMetadata` *write* codec (`encode_key`/`encode_value`) and the
// `current_state_timestamp_ms` field are wire-faithful but not yet exercised
// in production: classic group membership is in-memory and only committed
// offsets (k0/k1) are persisted today. Bootstrap *decodes* k2 records, and the
// migration code writes them on conversion. Retained per the
// B1 merge; allow until those slices wire the write path.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct GroupMetadataValue {
    pub protocol_type: String,
    pub generation: i32,
    pub protocol_name: Option<String>,
    pub leader: Option<String>,
    pub current_state_timestamp_ms: i64,
    pub members: Vec<MemberMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberMetadata {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub subscription: Bytes,
    pub assignment: Bytes,
}

#[allow(dead_code)] // k2 write codec; see GroupMetadataValue note above.
impl GroupMetadataValue {
    /// Encode a `GroupMetadata` key (version 2).
    #[must_use]
    pub fn encode_key(group_id: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(2);
        put_string(&mut buf, group_id);
        buf.freeze()
    }

    /// Encode a `GroupMetadata` value (version 3).
    #[must_use]
    pub fn encode_value(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // value version
        put_string(&mut buf, &self.protocol_type);
        buf.put_i32(self.generation);
        put_nullable_string(&mut buf, self.protocol_name.as_deref());
        put_nullable_string(&mut buf, self.leader.as_deref());
        buf.put_i64(self.current_state_timestamp_ms);
        let n = i32::try_from(self.members.len()).expect("members fit in i32");
        buf.put_i32(n);
        for m in &self.members {
            put_string(&mut buf, &m.member_id);
            put_nullable_string(&mut buf, m.group_instance_id.as_deref());
            put_string(&mut buf, &m.client_id);
            put_string(&mut buf, &m.client_host);
            buf.put_i32(m.rebalance_timeout_ms);
            buf.put_i32(m.session_timeout_ms);
            put_bytes(&mut buf, &m.subscription);
            put_bytes(&mut buf, &m.assignment);
        }
        buf.freeze()
    }

    pub fn decode_value(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let version = get_i16(&mut buf)?;
        if !(0..=3).contains(&version) {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue("unknown GroupMetadataValue version"),
            ));
        }
        let protocol_type = get_string(&mut buf)?;
        let generation = get_i32(&mut buf)?;
        let protocol_name = get_nullable_string(&mut buf)?;
        let leader = get_nullable_string(&mut buf)?;
        let current_state_timestamp_ms = if version >= 2 { get_i64(&mut buf)? } else { -1 };
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative i32 fits in usize");
        let mut members = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let member_id = get_string(&mut buf)?;
            let group_instance_id = if version >= 3 {
                get_nullable_string(&mut buf)?
            } else {
                None
            };
            let client_id = get_string(&mut buf)?;
            let client_host = get_string(&mut buf)?;
            let rebalance_timeout_ms = if version >= 1 { get_i32(&mut buf)? } else { 0 };
            let session_timeout_ms = get_i32(&mut buf)?;
            let subscription = get_bytes(&mut buf)?;
            let assignment = get_bytes(&mut buf)?;
            members.push(MemberMetadata {
                member_id,
                group_instance_id,
                client_id,
                client_host,
                rebalance_timeout_ms,
                session_timeout_ms,
                subscription,
                assignment,
            });
        }
        Ok(Self {
            protocol_type,
            generation,
            protocol_name,
            leader,
            current_state_timestamp_ms,
            members,
        })
    }
}

// ── primitives (non-flexible Kafka encoding) ───────────────────────────────

pub(crate) fn get_i16(buf: &mut &[u8]) -> Result<i16, BrokerError> {
    if buf.remaining() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets buf < i16"),
        ));
    }
    Ok(buf.get_i16())
}

pub(crate) fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> {
    if buf.remaining() < 4 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets buf < i32"),
        ));
    }
    Ok(buf.get_i32())
}

pub(crate) fn get_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> {
    if buf.remaining() < 8 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets buf < i64"),
        ));
    }
    Ok(buf.get_i64())
}

pub(crate) fn get_string(buf: &mut &[u8]) -> Result<String, BrokerError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("STRING with negative length"),
        ));
    }
    let n = usize::try_from(len).expect("non-negative i16 fits in usize");
    if buf.remaining() < n {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("STRING shorter than declared"),
        ));
    }
    let mut out = vec![0u8; n];
    buf.copy_to_slice(&mut out);
    String::from_utf8(out).map_err(|_| {
        BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidValue(
            "STRING not valid UTF-8",
        ))
    })
}

pub(crate) fn get_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, BrokerError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let n = usize::try_from(len).expect("non-negative i16 fits in usize");
    if buf.remaining() < n {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("NULLABLE_STRING shorter than declared"),
        ));
    }
    let mut out = vec![0u8; n];
    buf.copy_to_slice(&mut out);
    String::from_utf8(out).map(Some).map_err(|_| {
        BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidValue(
            "NULLABLE_STRING not valid UTF-8",
        ))
    })
}

pub(crate) fn get_bytes(buf: &mut &[u8]) -> Result<Bytes, BrokerError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Ok(Bytes::new());
    }
    let n = usize::try_from(len).expect("non-negative i32 fits in usize");
    if buf.remaining() < n {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("BYTES shorter than declared"),
        ));
    }
    let mut out = vec![0u8; n];
    buf.copy_to_slice(&mut out);
    Ok(Bytes::from(out))
}

pub(crate) fn put_string<B: BufMut>(buf: &mut B, s: &str) {
    let n = i16::try_from(s.len()).expect("string < 32k");
    buf.put_i16(n);
    buf.put_slice(s.as_bytes());
}

pub(crate) fn put_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        None => buf.put_i16(-1),
        Some(s) => put_string(buf, s),
    }
}

pub(crate) fn put_bytes<B: BufMut>(buf: &mut B, b: &Bytes) {
    let n = i32::try_from(b.len()).expect("bytes < 2GiB");
    buf.put_i32(n);
    buf.put_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn offset_commit_round_trip() {
        let v = OffsetCommitValue {
            offset: 42,
            leader_epoch: 0,
            metadata: "meta".into(),
            commit_timestamp_ms: 1_000_000,
        };
        let encoded = v.encode_value();
        let decoded = OffsetCommitValue::decode_value(&encoded).unwrap();
        assert!(decoded == v);
    }

    #[test]
    fn group_metadata_round_trip() {
        let v = GroupMetadataValue {
            protocol_type: "consumer".into(),
            generation: 5,
            protocol_name: Some("range".into()),
            leader: Some("m1".into()),
            current_state_timestamp_ms: 12_345,
            members: vec![MemberMetadata {
                member_id: "m1".into(),
                group_instance_id: None,
                client_id: "test-client".into(),
                client_host: "127.0.0.1".into(),
                rebalance_timeout_ms: 60_000,
                session_timeout_ms: 30_000,
                subscription: Bytes::from_static(b"sub"),
                assignment: Bytes::from_static(b"asgn"),
            }],
        };
        let encoded = v.encode_value();
        let decoded = GroupMetadataValue::decode_value(&encoded).unwrap();
        assert!(decoded == v);
    }

    #[test]
    fn parse_key_offset_commit_v1() {
        let key = OffsetCommitValue::encode_key("grp", "topic", 7);
        assert!(
            parse_key(&key).unwrap()
                == Key::OffsetCommit {
                    group_id: "grp".to_string(),
                    topic: "topic".to_string(),
                    partition: 7,
                }
        );
    }

    #[test]
    fn parse_key_group_metadata_v2() {
        let key = GroupMetadataValue::encode_key("grp");
        match parse_key(&key).unwrap() {
            Key::GroupMetadata { group_id } => assert!(group_id == "grp"),
            k @ (Key::OffsetCommit { .. } | Key::NextGen(_) | Key::Share(_) | Key::Streams(_)) => {
                panic!("expected GroupMetadata, got {k:?}")
            }
        }
    }
}
