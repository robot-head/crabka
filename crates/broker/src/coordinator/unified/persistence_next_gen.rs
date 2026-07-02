//! KIP-848 record types persisted in `__consumer_offsets`. Wire encoding
//! matches the Apache Kafka reference implementation; values carry a
//! version preamble.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_protocol::{ProtocolError, primitives::uuid::Uuid};

use crate::{
    coordinator::unified::persistence::{
        get_bytes, get_i16, get_i32, get_nullable_string, get_string, put_bytes,
        put_nullable_string, put_string,
    },
    error::BrokerError,
};

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextGenKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
}

pub fn parse_key(version: i16, mut buf: &[u8]) -> Result<NextGenKey, BrokerError> {
    let key = match version {
        KEY_GROUP_METADATA => NextGenKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_MEMBER_METADATA => NextGenKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_TARGET_ASSIGNMENT_METADATA => NextGenKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_TARGET_ASSIGNMENT_MEMBER => NextGenKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_CURRENT_MEMBER_ASSIGNMENT => NextGenKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown next-gen key version",
            )));
        }
    };
    Ok(key)
}

#[must_use]
pub fn encode_key(key: &NextGenKey) -> Bytes {
    let mut buf = BytesMut::new();
    match key {
        NextGenKey::GroupMetadata { group_id } => {
            buf.put_i16(KEY_GROUP_METADATA);
            put_string(&mut buf, group_id);
        }
        NextGenKey::MemberMetadata {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_MEMBER_METADATA);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        NextGenKey::TargetAssignmentMetadata { group_id } => {
            buf.put_i16(KEY_TARGET_ASSIGNMENT_METADATA);
            put_string(&mut buf, group_id);
        }
        NextGenKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_TARGET_ASSIGNMENT_MEMBER);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        NextGenKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => {
            buf.put_i16(KEY_CURRENT_MEMBER_ASSIGNMENT);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
    }
    buf.freeze()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupMetadataValue {
    pub epoch: i32,
}

impl GroupMetadataValue {
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

/// Classic-protocol sub-state for a member hosted inside an upgraded consumer
/// group (KIP-848 migration). Mirrors Kafka's
/// `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`; lets a downgrade
/// restore the classic member losslessly after a coordinator failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicMemberMetadata {
    pub session_timeout_ms: i32,
    pub supported_protocols: Vec<(String, Bytes)>,
    pub last_synced_assignment: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    /// KIP-848 v1+ `subscribed_topic_regex`. `None` = exact-name
    /// subscription only. The reconciler unions regex matches with
    /// `subscribed_topic_names` against the current metadata image.
    pub subscribed_topic_regex: Option<String>,
    pub server_assignor: Option<String>,
    pub rebalance_timeout_ms: i32,
    /// `Some` iff this is a hosted classic member; `None` for a native
    /// consumer-protocol member.
    pub classic: Option<ClassicMemberMetadata>,
}

impl MemberMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.instance_id.as_deref());
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        let n = i32::try_from(self.subscribed_topic_names.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subscribed_topic_names {
            put_string(&mut buf, s);
        }
        put_nullable_string(&mut buf, self.subscribed_topic_regex.as_deref());
        put_nullable_string(&mut buf, self.server_assignor.as_deref());
        buf.put_i32(self.rebalance_timeout_ms);
        match &self.classic {
            None => buf.put_i8(0),
            Some(c) => {
                buf.put_i8(1);
                buf.put_i32(c.session_timeout_ms);
                let pn = i32::try_from(c.supported_protocols.len()).expect("fits");
                buf.put_i32(pn);
                for (name, meta) in &c.supported_protocols {
                    put_string(&mut buf, name);
                    put_bytes(&mut buf, meta);
                }
                put_bytes(&mut buf, &c.last_synced_assignment);
            }
        }
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let instance_id = get_nullable_string(&mut buf)?;
        let rack_id = get_nullable_string(&mut buf)?;
        let client_id = get_string(&mut buf)?;
        let client_host = get_string(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subscribed_topic_names = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subscribed_topic_names.push(get_string(&mut buf)?);
        }
        let subscribed_topic_regex = get_nullable_string(&mut buf)?;
        let server_assignor = get_nullable_string(&mut buf)?;
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let classic = {
            if buf.remaining() < 1 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "missing classic-presence byte",
                )));
            }
            if buf.get_i8() == 0 {
                None
            } else {
                let session_timeout_ms = get_i32(&mut buf)?;
                let pn = get_i32(&mut buf)?;
                let pcap = usize::try_from(pn.max(0)).expect("non-negative");
                let mut supported_protocols = Vec::with_capacity(pcap);
                for _ in 0..pn.max(0) {
                    let name = get_string(&mut buf)?;
                    let meta = get_bytes(&mut buf)?;
                    supported_protocols.push((name, meta));
                }
                let last_synced_assignment = get_bytes(&mut buf)?;
                Some(ClassicMemberMetadata {
                    session_timeout_ms,
                    supported_protocols,
                    last_synced_assignment,
                })
            }
        };
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            server_assignor,
            rebalance_timeout_ms,
            classic,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl TargetAssignmentMetadataValue {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedTopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TargetAssignmentMemberValue {
    pub topic_partitions: Vec<AssignedTopicPartitions>,
}

impl TargetAssignmentMemberValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.topic_partitions.len()).expect("fits");
        buf.put_i32(n);
        for tp in &self.topic_partitions {
            put_bytes(&mut buf, &Bytes::copy_from_slice(&tp.topic_id.0));
            let pn = i32::try_from(tp.partitions.len()).expect("fits");
            buf.put_i32(pn);
            for p in &tp.partitions {
                buf.put_i32(*p);
            }
        }
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut topic_partitions = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let id_bytes = get_bytes(&mut buf)?;
            if id_bytes.len() != 16 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "topic_id not 16 bytes",
                )));
            }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&id_bytes);
            let topic_id = Uuid(arr);
            let pn = get_i32(&mut buf)?;
            let pcap = usize::try_from(pn.max(0)).expect("non-negative");
            let mut partitions = Vec::with_capacity(pcap);
            for _ in 0..pn.max(0) {
                partitions.push(get_i32(&mut buf)?);
            }
            topic_partitions.push(AssignedTopicPartitions {
                topic_id,
                partitions,
            });
        }
        Ok(Self { topic_partitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberAssignmentState {
    Stable = 0,
    UnreleasedPartitions = 1,
    UnrevokedPartitions = 2,
}

impl MemberAssignmentState {
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            0 => Ok(Self::Stable),
            1 => Ok(Self::UnreleasedPartitions),
            2 => Ok(Self::UnrevokedPartitions),
            _ => Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown MemberAssignmentState",
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: MemberAssignmentState,
    pub assigned_partitions: Vec<AssignedTopicPartitions>,
    pub partitions_pending_revocation: Vec<AssignedTopicPartitions>,
}

impl CurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state as i8);
        encode_topic_partitions(&mut buf, &self.assigned_partitions);
        encode_topic_partitions(&mut buf, &self.partitions_pending_revocation);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let previous_member_epoch = get_i32(&mut buf)?;
        if buf.remaining() < 1 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "missing state byte",
            )));
        }
        let state = MemberAssignmentState::from_i8(buf.get_i8())?;
        let assigned_partitions = decode_topic_partitions(&mut buf)?;
        let partitions_pending_revocation = decode_topic_partitions(&mut buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            assigned_partitions,
            partitions_pending_revocation,
        })
    }
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[AssignedTopicPartitions]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for tp in items {
        put_bytes(buf, &Bytes::copy_from_slice(&tp.topic_id.0));
        let pn = i32::try_from(tp.partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
    }
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<AssignedTopicPartitions>, BrokerError> {
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
        out.push(AssignedTopicPartitions {
            topic_id,
            partitions,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn group_metadata_value_roundtrip() {
        let v = GroupMetadataValue { epoch: 7 };
        assert!(GroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_roundtrip() {
        let v = MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: None,
        };
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_with_regex_roundtrip() {
        // KIP-848 v1+: `subscribed_topic_regex` survives encode/decode
        // so bootstrap replay can hydrate the regex subscription
        // without waiting for a heartbeat.
        let v = MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["audit".into()],
            subscribed_topic_regex: Some("^orders-.*".into()),
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: None,
        };
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_metadata_roundtrip() {
        let v = TargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(TargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_member_roundtrip() {
        let v = TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([1; 16]),
                partitions: vec![0, 1, 2],
            }],
        };
        assert!(TargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_roundtrip() {
        let v = CurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: MemberAssignmentState::Stable,
            assigned_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([2; 16]),
                partitions: vec![0, 1],
            }],
            partitions_pending_revocation: vec![],
        };
        assert!(CurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_round_trips_classic_block() {
        use bytes::Bytes;
        let v = MemberMetadataValue {
            instance_id: Some("inst-a".into()),
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["t1".into(), "t2".into()],
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: Some(ClassicMemberMetadata {
                session_timeout_ms: 30_000,
                supported_protocols: vec![("range".into(), Bytes::from_static(b"meta"))],
                last_synced_assignment: Bytes::from_static(b"asn"),
            }),
        };
        let decoded = MemberMetadataValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);

        let mut native = v.clone();
        native.classic = None;
        assert!(MemberMetadataValue::decode(&native.encode()).unwrap() == native);
    }

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_key(99, &[]).is_err());
    }

    #[test]
    fn key_roundtrip_member_metadata() {
        let k = NextGenKey::MemberMetadata {
            group_id: "g".into(),
            member_id: "m".into(),
        };
        let kb = encode_key(&k);
        let mut r = &kb[..];
        let v = bytes::Buf::get_i16(&mut r);
        assert!(parse_key(v, r).unwrap() == k);
    }
}
