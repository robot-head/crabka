//! KIP-932 share-group record types persisted in `__consumer_offsets`.
//!
//! Wire encoding mirrors `persistence_next_gen` (the KIP-848 consumer
//! next-gen codecs): a leading `i16` key-version discriminator on keys and an
//! `i16(0)` version preamble on values. Share-group records reuse the same
//! length-prefixed array / nullable-string leaf encoders but drop the
//! consumer-only fields (`instance_id`, `server_assignor`,
//! `subscribed_topic_regex`, `rebalance_timeout_ms`, and the
//! revocation/pending assignment machinery).
//!
//! Key versions 9–13 are free; the consumer next-gen keys use 3,5,6,7,8.

use bytes::{BufMut, Bytes, BytesMut};
use crabka_protocol::{ProtocolError, primitives::uuid::Uuid};

use crate::{
    coordinator::unified::persistence::{
        get_bytes, get_i16, get_i32, get_nullable_string, get_string, put_bytes,
        put_nullable_string, put_string,
    },
    error::BrokerError,
};

pub const KEY_SHARE_GROUP_METADATA: i16 = 9;
pub const KEY_SHARE_MEMBER_METADATA: i16 = 10;
pub const KEY_SHARE_TARGET_ASSIGNMENT_METADATA: i16 = 11;
pub const KEY_SHARE_TARGET_ASSIGNMENT_MEMBER: i16 = 12;
pub const KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT: i16 = 13;
pub const KEY_SHARE_GROUP_STATE_PARTITION_METADATA: i16 = 14;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareGroupKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
    StatePartitionMetadata { group_id: String },
}

#[must_use]
pub fn encode_share_key(key: &ShareGroupKey) -> Bytes {
    let mut buf = BytesMut::new();
    match key {
        ShareGroupKey::GroupMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_GROUP_METADATA);
            put_string(&mut buf, group_id);
        }
        ShareGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_SHARE_MEMBER_METADATA);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        ShareGroupKey::TargetAssignmentMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_TARGET_ASSIGNMENT_METADATA);
            put_string(&mut buf, group_id);
        }
        ShareGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_SHARE_TARGET_ASSIGNMENT_MEMBER);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        ShareGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        ShareGroupKey::StatePartitionMetadata { group_id } => {
            buf.put_i16(KEY_SHARE_GROUP_STATE_PARTITION_METADATA);
            put_string(&mut buf, group_id);
        }
    }
    buf.freeze()
}

pub fn parse_share_key(version: i16, mut buf: &[u8]) -> Result<ShareGroupKey, BrokerError> {
    let key = match version {
        KEY_SHARE_GROUP_METADATA => ShareGroupKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_SHARE_MEMBER_METADATA => ShareGroupKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_SHARE_TARGET_ASSIGNMENT_METADATA => ShareGroupKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_SHARE_TARGET_ASSIGNMENT_MEMBER => ShareGroupKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT => ShareGroupKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_SHARE_GROUP_STATE_PARTITION_METADATA => ShareGroupKey::StatePartitionMetadata {
            group_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown share-group key version",
            )));
        }
    };
    Ok(key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupMetadataValue {
    pub epoch: i32,
}

impl ShareGroupMetadataValue {
    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)] // wire-encode API mirrors the other *Value::encode signatures
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        Ok(Self {
            epoch: get_i32(&mut buf)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberMetadataValue {
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
}

impl ShareGroupMemberMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        let n = i32::try_from(self.subscribed_topic_names.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subscribed_topic_names {
            put_string(&mut buf, s);
        }
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let rack_id = get_nullable_string(&mut buf)?;
        let client_id = get_string(&mut buf)?;
        let client_host = get_string(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subscribed_topic_names = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subscribed_topic_names.push(get_string(&mut buf)?);
        }
        Ok(Self {
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupTargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl ShareGroupTargetAssignmentMetadataValue {
    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)] // wire-encode API mirrors the other *Value::encode signatures
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.assignment_epoch);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        Ok(Self {
            assignment_epoch: get_i32(&mut buf)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupTargetAssignmentMemberValue {
    pub topic_partitions: Vec<(Uuid, Vec<i32>)>,
}

impl ShareGroupTargetAssignmentMemberValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        encode_topic_partitions(&mut buf, &self.topic_partitions);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let topic_partitions = decode_topic_partitions(&mut buf)?;
        Ok(Self { topic_partitions })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub assigned_partitions: Vec<(Uuid, Vec<i32>)>,
}

impl ShareGroupCurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        encode_topic_partitions(&mut buf, &self.assigned_partitions);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let assigned_partitions = decode_topic_partitions(&mut buf)?;
        Ok(Self {
            member_epoch,
            assigned_partitions,
        })
    }
}

/// KIP-932 `ShareGroupStatePartitionMetadata` (key v14): tracks which
/// `(topic_id, partition)` share-states a group has initialized, plus a set
/// of topic ids whose share-state is being deleted. Lets the group
/// coordinator skip re-initializing partitions across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupStatePartitionMetadataValue {
    pub initialized: Vec<(uuid::Uuid, Vec<i32>)>,
    pub deleting: Vec<uuid::Uuid>,
}

impl ShareGroupStatePartitionMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.initialized.len()).expect("fits");
        buf.put_i32(n);
        for (topic_id, partitions) in &self.initialized {
            buf.put_slice(topic_id.as_bytes());
            let pn = i32::try_from(partitions.len()).expect("fits");
            buf.put_i32(pn);
            for p in partitions {
                buf.put_i32(*p);
            }
        }
        let dn = i32::try_from(self.deleting.len()).expect("fits");
        buf.put_i32(dn);
        for topic_id in &self.deleting {
            buf.put_slice(topic_id.as_bytes());
        }
        buf.freeze()
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut initialized = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let topic_id = get_uuid(&mut buf)?;
            let pn = get_i32(&mut buf)?;
            let pcap = usize::try_from(pn.max(0)).expect("non-negative");
            let mut partitions = Vec::with_capacity(pcap);
            for _ in 0..pn.max(0) {
                partitions.push(get_i32(&mut buf)?);
            }
            initialized.push((topic_id, partitions));
        }
        let dn = get_i32(&mut buf)?;
        let dcap = usize::try_from(dn.max(0)).expect("non-negative");
        let mut deleting = Vec::with_capacity(dcap);
        for _ in 0..dn.max(0) {
            deleting.push(get_uuid(&mut buf)?);
        }
        Ok(Self {
            initialized,
            deleting,
        })
    }
}

fn get_uuid(buf: &mut &[u8]) -> Result<uuid::Uuid, BrokerError> {
    if buf.len() < 16 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "topic_id not 16 bytes",
        )));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&buf[..16]);
    bytes::Buf::advance(buf, 16);
    Ok(uuid::Uuid::from_bytes(arr))
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[(Uuid, Vec<i32>)]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for (topic_id, partitions) in items {
        put_bytes(buf, &Bytes::copy_from_slice(&topic_id.0));
        let pn = i32::try_from(partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in partitions {
            buf.put_i32(*p);
        }
    }
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<(Uuid, Vec<i32>)>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        let id_bytes = get_bytes(buf)?;
        if id_bytes.len() != 16 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "topic_id not 16 bytes",
            )));
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&id_bytes);
        let topic_id = Uuid(arr);
        let pn = get_i32(buf)?;
        let pcap = usize::try_from(pn.max(0)).expect("non-negative");
        let mut partitions = Vec::with_capacity(pcap);
        for _ in 0..pn.max(0) {
            partitions.push(get_i32(buf)?);
        }
        out.push((topic_id, partitions));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {

    use super::*;

    /// Split a freshly encoded key into its leading version and the remaining
    /// body, mirroring how `__consumer_offsets` keys are dispatched on the
    /// leading `i16`.
    fn peek_version(buf: &[u8]) -> (i16, &[u8]) {
        let mut r = buf;
        let v = bytes::Buf::get_i16(&mut r);
        (v, r)
    }

    #[test]
    fn group_metadata_round_trip() {
        let key = ShareGroupKey::GroupMetadata {
            group_id: "g1".into(),
        };
        let bytes = encode_share_key(&key);
        let (ver, body) = peek_version(&bytes);
        assert2::assert!(ver == KEY_SHARE_GROUP_METADATA);
        assert2::assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupMetadataValue { epoch: 7 };
        assert2::assert!(ShareGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_round_trip() {
        let key = ShareGroupKey::MemberMetadata {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert2::assert!(ver == KEY_SHARE_MEMBER_METADATA);
        assert2::assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupMemberMetadataValue {
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
        };
        assert2::assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_null_rack_round_trip() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec![],
        };
        assert2::assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let key = ShareGroupKey::TargetAssignmentMetadata {
            group_id: "g1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert2::assert!(ver == KEY_SHARE_TARGET_ASSIGNMENT_METADATA);
        assert2::assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert2::assert!(
            ShareGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v
        );
    }

    #[test]
    fn target_assignment_member_round_trip() {
        let key = ShareGroupKey::TargetAssignmentMember {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert2::assert!(ver == KEY_SHARE_TARGET_ASSIGNMENT_MEMBER);
        assert2::assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupTargetAssignmentMemberValue {
            topic_partitions: vec![(Uuid([1; 16]), vec![0, 1, 2]), (Uuid([2; 16]), vec![])],
        };
        assert2::assert!(ShareGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_round_trip() {
        let key = ShareGroupKey::CurrentMemberAssignment {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert2::assert!(ver == KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT);
        assert2::assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            assigned_partitions: vec![(Uuid([3; 16]), vec![0, 1])],
        };
        assert2::assert!(ShareGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn state_partition_metadata_round_trip() {
        let key = ShareGroupKey::StatePartitionMetadata {
            group_id: "g1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert2::assert!(ver == KEY_SHARE_GROUP_STATE_PARTITION_METADATA);
        assert2::assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupStatePartitionMetadataValue {
            initialized: vec![
                (uuid::Uuid::from_bytes([1; 16]), vec![0, 1, 2]),
                (uuid::Uuid::from_bytes([2; 16]), vec![]),
            ],
            deleting: vec![uuid::Uuid::from_bytes([9; 16])],
        };
        assert2::assert!(ShareGroupStatePartitionMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn state_partition_metadata_empty_round_trip() {
        let v = ShareGroupStatePartitionMetadataValue::default();
        assert2::assert!(ShareGroupStatePartitionMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn unknown_key_version_rejected() {
        assert2::assert!(parse_share_key(99, &[]).is_err());
    }
}
