//! KIP-1071 streams-group record types persisted in `__consumer_offsets`.
//!
//! Wire encoding mirrors [`persistence_next_gen`](crate::coordinator::unified::persistence_next_gen)
//! (the KIP-848 consumer next-gen codecs) and the KIP-932 share analog
//! ([`super::super::share::persistence`]): a leading `i16` key-version
//! discriminator on keys and an `i16(0)` version preamble on values. Streams
//! records reuse the same length-prefixed array / nullable-string / uuid leaf
//! encoders but model *tasks* — `(subtopology, partition)` grouped by
//! active/standby/warmup role — rather than topic partitions.
//!
//! Key versions 15–21 are allocated to streams. The earlier ranges are taken:
//! 0,1 offset-commit; 2 classic group; 3,5,6,7,8 consumer next-gen; 9–14 share.
//!
//! This module is intentionally self-contained: it defines its own value
//! structs and represents the assignment-by-role as
//! `BTreeMap<String, Vec<i32>>` (subtopology id → partitions) rather than
//! importing the in-memory state model.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crabka_protocol::ProtocolError;
use crabka_protocol::records::{Record, RecordBatch};

use crate::coordinator::unified::persistence::{
    get_bytes, get_i16, get_i32, get_nullable_string, get_string, put_bytes, put_nullable_string,
    put_string,
};
use crate::error::BrokerError;

pub const KEY_STREAMS_GROUP_METADATA: i16 = 15;
pub const KEY_STREAMS_MEMBER_METADATA: i16 = 16;
pub const KEY_STREAMS_TOPOLOGY: i16 = 17;
pub const KEY_STREAMS_PARTITION_METADATA: i16 = 18;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_METADATA: i16 = 19;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER: i16 = 20;
pub const KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT: i16 = 21;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsGroupKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    Topology { group_id: String },
    PartitionMetadata { group_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
}

#[must_use]
pub fn encode_group_metadata_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_GROUP_METADATA);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_member_metadata_key(group_id: &str, member_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_MEMBER_METADATA);
    put_string(&mut buf, group_id);
    put_string(&mut buf, member_id);
    buf.freeze()
}

#[must_use]
pub fn encode_topology_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_TOPOLOGY);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_partition_metadata_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_PARTITION_METADATA);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_target_assignment_metadata_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_TARGET_ASSIGNMENT_METADATA);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_target_assignment_member_key(group_id: &str, member_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER);
    put_string(&mut buf, group_id);
    put_string(&mut buf, member_id);
    buf.freeze()
}

#[must_use]
pub fn encode_current_member_assignment_key(group_id: &str, member_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT);
    put_string(&mut buf, group_id);
    put_string(&mut buf, member_id);
    buf.freeze()
}

/// Encode a [`StreamsGroupKey`] (leading `i16` key version) for dispatch.
#[must_use]
pub fn encode_streams_key(key: &StreamsGroupKey) -> Bytes {
    match key {
        StreamsGroupKey::GroupMetadata { group_id } => encode_group_metadata_key(group_id),
        StreamsGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => encode_member_metadata_key(group_id, member_id),
        StreamsGroupKey::Topology { group_id } => encode_topology_key(group_id),
        StreamsGroupKey::PartitionMetadata { group_id } => encode_partition_metadata_key(group_id),
        StreamsGroupKey::TargetAssignmentMetadata { group_id } => {
            encode_target_assignment_metadata_key(group_id)
        }
        StreamsGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => encode_target_assignment_member_key(group_id, member_id),
        StreamsGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => encode_current_member_assignment_key(group_id, member_id),
    }
}

pub fn parse_streams_key(version: i16, mut buf: &[u8]) -> Result<StreamsGroupKey, BrokerError> {
    let key = match version {
        KEY_STREAMS_GROUP_METADATA => StreamsGroupKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_MEMBER_METADATA => StreamsGroupKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_TOPOLOGY => StreamsGroupKey::Topology {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_PARTITION_METADATA => StreamsGroupKey::PartitionMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_TARGET_ASSIGNMENT_METADATA => StreamsGroupKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER => StreamsGroupKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT => StreamsGroupKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown streams-group key version",
            )));
        }
    };
    Ok(key)
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// Key v15 value: the streams group epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamsGroupMetadataValue {
    pub epoch: i32,
}

impl StreamsGroupMetadataValue {
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

/// A member's advertised host/port endpoint for interactive-query routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsEndpoint {
    pub host: String,
    pub port: u32,
}

/// Key v16 value: a streams group member's static metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub process_id: String,
    pub user_endpoint: Option<StreamsEndpoint>,
    pub client_tags: Vec<(String, String)>,
    pub rebalance_timeout_ms: i32,
    pub topology_epoch: i32,
}

impl StreamsGroupMemberMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.instance_id.as_deref());
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        put_string(&mut buf, &self.process_id);
        // user_endpoint: a single i8 presence flag, then host + port if present.
        match &self.user_endpoint {
            Some(ep) => {
                buf.put_i8(1);
                put_string(&mut buf, &ep.host);
                buf.put_u32(ep.port);
            }
            None => buf.put_i8(0),
        }
        let n = i32::try_from(self.client_tags.len()).expect("fits");
        buf.put_i32(n);
        for (k, v) in &self.client_tags {
            put_string(&mut buf, k);
            put_string(&mut buf, v);
        }
        buf.put_i32(self.rebalance_timeout_ms);
        buf.put_i32(self.topology_epoch);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let instance_id = get_nullable_string(&mut buf)?;
        let rack_id = get_nullable_string(&mut buf)?;
        let client_id = get_string(&mut buf)?;
        let client_host = get_string(&mut buf)?;
        let process_id = get_string(&mut buf)?;
        let user_endpoint = if get_i8(&mut buf)? == 0 {
            None
        } else {
            let host = get_string(&mut buf)?;
            let port = get_u32(&mut buf)?;
            Some(StreamsEndpoint { host, port })
        };
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut client_tags = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let k = get_string(&mut buf)?;
            let v = get_string(&mut buf)?;
            client_tags.push((k, v));
        }
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let topology_epoch = get_i32(&mut buf)?;
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            process_id,
            user_endpoint,
            client_tags,
            rebalance_timeout_ms,
            topology_epoch,
        })
    }
}

/// An internal/changelog/repartition topic referenced by a subtopology, with
/// the partition count, replication factor, and per-topic config overrides
/// the coordinator should materialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<(String, String)>,
}

impl StoredTopicInfo {
    fn encode_into(&self, buf: &mut BytesMut) {
        put_string(buf, &self.name);
        buf.put_i32(self.partitions);
        buf.put_i16(self.replication_factor);
        let n = i32::try_from(self.topic_configs.len()).expect("fits");
        buf.put_i32(n);
        for (k, v) in &self.topic_configs {
            put_string(buf, k);
            put_string(buf, v);
        }
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let name = get_string(buf)?;
        let partitions = get_i32(buf)?;
        let replication_factor = get_i16(buf)?;
        let n = get_i32(buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut topic_configs = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let k = get_string(buf)?;
            let v = get_string(buf)?;
            topic_configs.push((k, v));
        }
        Ok(Self {
            name,
            partitions,
            replication_factor,
            topic_configs,
        })
    }
}

/// A copartition group: indices (into the enclosing subtopology's topic lists)
/// of topics that must be copartitioned with one another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCopartitionGroup {
    pub source_topics: Vec<i16>,
    pub source_topic_regex: Vec<i16>,
    pub repartition_source_topics: Vec<i16>,
}

impl StoredCopartitionGroup {
    fn encode_into(&self, buf: &mut BytesMut) {
        encode_i16_list(buf, &self.source_topics);
        encode_i16_list(buf, &self.source_topic_regex);
        encode_i16_list(buf, &self.repartition_source_topics);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        Ok(Self {
            source_topics: decode_i16_list(buf)?,
            source_topic_regex: decode_i16_list(buf)?,
            repartition_source_topics: decode_i16_list(buf)?,
        })
    }
}

/// One subtopology of a streams topology: its source topics (exact + regex),
/// the repartition sinks/sources it produces/consumes, its changelog topics,
/// and any copartition constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub repartition_sink_topics: Vec<String>,
    pub state_changelog_topics: Vec<StoredTopicInfo>,
    pub repartition_source_topics: Vec<StoredTopicInfo>,
    pub copartition_groups: Vec<StoredCopartitionGroup>,
}

impl StoredSubtopology {
    fn encode_into(&self, buf: &mut BytesMut) {
        put_string(buf, &self.subtopology_id);
        encode_string_list(buf, &self.source_topics);
        encode_string_list(buf, &self.source_topic_regex);
        encode_string_list(buf, &self.repartition_sink_topics);
        let scn = i32::try_from(self.state_changelog_topics.len()).expect("fits");
        buf.put_i32(scn);
        for t in &self.state_changelog_topics {
            t.encode_into(buf);
        }
        let rsn = i32::try_from(self.repartition_source_topics.len()).expect("fits");
        buf.put_i32(rsn);
        for t in &self.repartition_source_topics {
            t.encode_into(buf);
        }
        let cgn = i32::try_from(self.copartition_groups.len()).expect("fits");
        buf.put_i32(cgn);
        for cg in &self.copartition_groups {
            cg.encode_into(buf);
        }
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let subtopology_id = get_string(buf)?;
        let source_topics = decode_string_list(buf)?;
        let source_topic_regex = decode_string_list(buf)?;
        let repartition_sink_topics = decode_string_list(buf)?;
        let scn = get_i32(buf)?;
        let mut state_changelog_topics = Vec::with_capacity(usize::try_from(scn.max(0)).unwrap());
        for _ in 0..scn.max(0) {
            state_changelog_topics.push(StoredTopicInfo::decode_from(buf)?);
        }
        let rsn = get_i32(buf)?;
        let mut repartition_source_topics =
            Vec::with_capacity(usize::try_from(rsn.max(0)).unwrap());
        for _ in 0..rsn.max(0) {
            repartition_source_topics.push(StoredTopicInfo::decode_from(buf)?);
        }
        let cgn = get_i32(buf)?;
        let mut copartition_groups = Vec::with_capacity(usize::try_from(cgn.max(0)).unwrap());
        for _ in 0..cgn.max(0) {
            copartition_groups.push(StoredCopartitionGroup::decode_from(buf)?);
        }
        Ok(Self {
            subtopology_id,
            source_topics,
            source_topic_regex,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
            copartition_groups,
        })
    }
}

/// Key v17 value: the group's resolved topology (epoch + subtopologies).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTopologyValue {
    pub epoch: i32,
    pub subtopologies: Vec<StoredSubtopology>,
}

impl StreamsGroupTopologyValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        let n = i32::try_from(self.subtopologies.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subtopologies {
            s.encode_into(&mut buf);
        }
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let epoch = get_i32(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subtopologies = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subtopologies.push(StoredSubtopology::decode_from(&mut buf)?);
        }
        Ok(Self {
            epoch,
            subtopologies,
        })
    }
}

/// A topic the group consumes/produces, paired with its uuid and partition
/// count as seen in the metadata image when the assignment was computed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsTopicMeta {
    pub topic_name: String,
    pub topic_id: uuid::Uuid,
    pub num_partitions: i32,
}

/// Key v18 value: per-topic partition metadata snapshot for the group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupPartitionMetadataValue {
    pub topics: Vec<StreamsTopicMeta>,
}

impl StreamsGroupPartitionMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.topics.len()).expect("fits");
        buf.put_i32(n);
        for t in &self.topics {
            put_string(&mut buf, &t.topic_name);
            put_bytes(&mut buf, &Bytes::copy_from_slice(t.topic_id.as_bytes()));
            buf.put_i32(t.num_partitions);
        }
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut topics = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let topic_name = get_string(&mut buf)?;
            let id_bytes = get_bytes(&mut buf)?;
            if id_bytes.len() != 16 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "topic_id not 16 bytes",
                )));
            }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&id_bytes);
            let topic_id = uuid::Uuid::from_bytes(arr);
            let num_partitions = get_i32(&mut buf)?;
            topics.push(StreamsTopicMeta {
                topic_name,
                topic_id,
                num_partitions,
            });
        }
        Ok(Self { topics })
    }
}

/// Key v19 value: the target-assignment epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamsGroupTargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl StreamsGroupTargetAssignmentMetadataValue {
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

/// Key v20 value: a member's target task assignment, by role. Each role maps a
/// subtopology id to the partitions of that subtopology assigned to the member.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTargetAssignmentMemberValue {
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
}

impl StreamsGroupTargetAssignmentMemberValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        encode_task_map(&mut buf, &self.active);
        encode_task_map(&mut buf, &self.standby);
        encode_task_map(&mut buf, &self.warmup);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let active = decode_task_map(&mut buf)?;
        let standby = decode_task_map(&mut buf)?;
        let warmup = decode_task_map(&mut buf)?;
        Ok(Self {
            active,
            standby,
            warmup,
        })
    }
}

/// Key v21 value: a member's current (in-flight) task assignment plus the
/// reconciliation epochs/state and any active tasks pending revocation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: i8,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
    pub active_pending_revocation: BTreeMap<String, Vec<i32>>,
}

impl StreamsGroupCurrentMemberAssignmentValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state);
        encode_task_map(&mut buf, &self.active);
        encode_task_map(&mut buf, &self.standby);
        encode_task_map(&mut buf, &self.warmup);
        encode_task_map(&mut buf, &self.active_pending_revocation);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let previous_member_epoch = get_i32(&mut buf)?;
        let state = get_i8(&mut buf)?;
        let active = decode_task_map(&mut buf)?;
        let standby = decode_task_map(&mut buf)?;
        let warmup = decode_task_map(&mut buf)?;
        let active_pending_revocation = decode_task_map(&mut buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            active,
            standby,
            warmup,
            active_pending_revocation,
        })
    }
}

// ---------------------------------------------------------------------------
// Leaf encode/decode helpers
// ---------------------------------------------------------------------------

/// Encode a role's task assignment: `i32` count of subtopologies, then per
/// entry `string subtopology_id` + `i32` partition count + that many `i32`
/// partitions. Decoded symmetrically by [`decode_task_map`].
fn encode_task_map(buf: &mut BytesMut, map: &BTreeMap<String, Vec<i32>>) {
    let n = i32::try_from(map.len()).expect("fits");
    buf.put_i32(n);
    for (subtopology_id, partitions) in map {
        put_string(buf, subtopology_id);
        let pn = i32::try_from(partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in partitions {
            buf.put_i32(*p);
        }
    }
}

fn decode_task_map(buf: &mut &[u8]) -> Result<BTreeMap<String, Vec<i32>>, BrokerError> {
    let n = get_i32(buf)?;
    let mut map = BTreeMap::new();
    for _ in 0..n.max(0) {
        let subtopology_id = get_string(buf)?;
        let pn = get_i32(buf)?;
        let pcap = usize::try_from(pn.max(0)).expect("non-negative");
        let mut partitions = Vec::with_capacity(pcap);
        for _ in 0..pn.max(0) {
            partitions.push(get_i32(buf)?);
        }
        map.insert(subtopology_id, partitions);
    }
    Ok(map)
}

fn encode_string_list(buf: &mut BytesMut, items: &[String]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for s in items {
        put_string(buf, s);
    }
}

fn decode_string_list(buf: &mut &[u8]) -> Result<Vec<String>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        out.push(get_string(buf)?);
    }
    Ok(out)
}

fn encode_i16_list(buf: &mut BytesMut, items: &[i16]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for v in items {
        buf.put_i16(*v);
    }
}

fn decode_i16_list(buf: &mut &[u8]) -> Result<Vec<i16>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        out.push(get_i16(buf)?);
    }
    Ok(out)
}

fn get_i8(buf: &mut &[u8]) -> Result<i8, BrokerError> {
    if buf.remaining() < 1 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "missing i8",
        )));
    }
    Ok(buf.get_i8())
}

fn get_u32(buf: &mut &[u8]) -> Result<u32, BrokerError> {
    if buf.remaining() < 4 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "missing u32",
        )));
    }
    Ok(buf.get_u32())
}

// ---------------------------------------------------------------------------
// PendingStreamsRecords — collects mutations for one group-state transition
// and encodes them as a single RecordBatch ready for OffsetsLog::append.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct PendingStreamsRecords {
    pub group_metadata: Option<StreamsGroupMetadataValue>,
    /// `Some(value)` writes the record; `None` writes a tombstone (null value).
    pub member_metadata: Vec<(String, Option<StreamsGroupMemberMetadataValue>)>,
    pub topology: Option<StreamsGroupTopologyValue>,
    pub partition_metadata: Option<StreamsGroupPartitionMetadataValue>,
    pub target_metadata: Option<StreamsGroupTargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<StreamsGroupTargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<StreamsGroupCurrentMemberAssignmentValue>)>,
}

impl PendingStreamsRecords {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.topology.is_none()
            && self.partition_metadata.is_none()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
    }

    #[must_use]
    pub fn into_batch(self, group_id: &str, now_ms: i64) -> RecordBatch {
        let mut records: Vec<Record> = Vec::new();
        let mut push = |key: Bytes, value: Option<Bytes>| {
            let delta = i32::try_from(records.len()).expect("batch size fits i32");
            records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(key),
                value,
                ..Default::default()
            });
        };

        if let Some(v) = self.group_metadata {
            push(encode_group_metadata_key(group_id), Some(v.encode()));
        }
        for (member_id, v) in self.member_metadata {
            push(
                encode_member_metadata_key(group_id, &member_id),
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.topology {
            push(encode_topology_key(group_id), Some(v.encode()));
        }
        if let Some(v) = self.partition_metadata {
            push(encode_partition_metadata_key(group_id), Some(v.encode()));
        }
        if let Some(v) = self.target_metadata {
            push(
                encode_target_assignment_metadata_key(group_id),
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            push(
                encode_target_assignment_member_key(group_id, &member_id),
                v.map(|x| x.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            push(
                encode_current_member_assignment_key(group_id, &member_id),
                v.map(|x| x.encode()),
            );
        }

        let last_delta = i32::try_from(records.len().saturating_sub(1)).unwrap_or(0);
        RecordBatch {
            max_timestamp: now_ms,
            records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{assert, check};

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
        let kb = encode_group_metadata_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_GROUP_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::GroupMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupMetadataValue { epoch: 7 };
        assert!(StreamsGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_round_trip() {
        let kb = encode_member_metadata_key("g1", "m1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_MEMBER_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::MemberMetadata {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let v = StreamsGroupMemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            process_id: "p-uuid".into(),
            user_endpoint: Some(StreamsEndpoint {
                host: "host-a".into(),
                port: 8080,
            }),
            client_tags: vec![("zone".into(), "a".into()), ("tier".into(), "hot".into())],
            rebalance_timeout_ms: 60_000,
            topology_epoch: 3,
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_null_optionals_round_trip() {
        let v = StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            process_id: "p-uuid".into(),
            user_endpoint: None,
            client_tags: vec![],
            rebalance_timeout_ms: 45_000,
            topology_epoch: 0,
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn topology_round_trip() {
        let kb = encode_topology_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TOPOLOGY);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::Topology {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTopologyValue {
            epoch: 2,
            subtopologies: vec![
                StoredSubtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in-a".into(), "in-b".into()],
                    source_topic_regex: vec!["^orders-.*".into()],
                    repartition_sink_topics: vec!["rp-1".into()],
                    state_changelog_topics: vec![StoredTopicInfo {
                        name: "store-changelog".into(),
                        partitions: 4,
                        replication_factor: 3,
                        topic_configs: vec![("cleanup.policy".into(), "compact".into())],
                    }],
                    repartition_source_topics: vec![StoredTopicInfo {
                        name: "rp-1".into(),
                        partitions: 4,
                        replication_factor: 3,
                        topic_configs: vec![],
                    }],
                    copartition_groups: vec![StoredCopartitionGroup {
                        source_topics: vec![0, 1],
                        source_topic_regex: vec![0],
                        repartition_source_topics: vec![0],
                    }],
                },
                StoredSubtopology {
                    subtopology_id: "1".into(),
                    source_topics: vec![],
                    source_topic_regex: vec![],
                    repartition_sink_topics: vec![],
                    state_changelog_topics: vec![],
                    repartition_source_topics: vec![],
                    copartition_groups: vec![],
                },
            ],
        };
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn topology_empty_round_trip() {
        let v = StreamsGroupTopologyValue::default();
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn partition_metadata_round_trip() {
        let kb = encode_partition_metadata_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_PARTITION_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::PartitionMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupPartitionMetadataValue {
            topics: vec![
                StreamsTopicMeta {
                    topic_name: "in-a".into(),
                    topic_id: uuid::Uuid::from_bytes([1; 16]),
                    num_partitions: 6,
                },
                StreamsTopicMeta {
                    topic_name: "in-b".into(),
                    topic_id: uuid::Uuid::from_bytes([2; 16]),
                    num_partitions: 0,
                },
            ],
        };
        assert!(StreamsGroupPartitionMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let kb = encode_target_assignment_metadata_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
        };
        assert!(StreamsGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn target_assignment_member_round_trip() {
        let kb = encode_target_assignment_member_key("g1", "m1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMember {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1, 2]);
        active.insert("1".to_string(), vec![]);
        let mut standby = BTreeMap::new();
        standby.insert("0".to_string(), vec![3]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby,
            warmup: BTreeMap::new(),
        };
        assert!(StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_round_trip() {
        let kb = encode_current_member_assignment_key("g1", "m1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::CurrentMemberAssignment {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1]);
        let mut pending = BTreeMap::new();
        pending.insert("0".to_string(), vec![2]);
        let v = StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: 1,
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: pending,
        };
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn task_map_multi_subtopology_empty_partitions_round_trip() {
        // A task map with several subtopologies, some carrying no partitions,
        // must survive encode/decode unchanged.
        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1, 2, 3]);
        active.insert("1".to_string(), vec![]);
        active.insert("2".to_string(), vec![7]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active: active.clone(),
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
        };
        let decoded = StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);
    }

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_streams_key(99, &[]).is_err());
    }

    #[test]
    fn pending_records_into_batch_emits_one_record_per_key() {
        let mut pending = PendingStreamsRecords {
            group_metadata: Some(StreamsGroupMetadataValue { epoch: 1 }),
            topology: Some(StreamsGroupTopologyValue::default()),
            ..Default::default()
        };
        pending.member_metadata.push(("m1".into(), None)); // tombstone
        let batch = pending.into_batch("g1", 123);
        // group_metadata + topology + one member tombstone = 3 records.
        check!(batch.records.len() == 3);
        check!(batch.max_timestamp == 123);
        check!(batch.last_offset_delta == 2);
        // The tombstone record carries a null value.
        let tombstone = batch.records.iter().find(|r| r.value.is_none()).unwrap();
        assert!(tombstone.key.is_some());
    }
}
