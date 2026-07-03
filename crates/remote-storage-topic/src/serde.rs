//! Binary wire format for `__remote_log_metadata` events.
//!
//! Records are encoded through
//! [`crabka_protocol::RemoteLogMetadataRecord`], which produces
//! bytes byte-identical to the JVM `RemoteLogMetadataSerde`
//! (`AbstractApiMessageSerde` envelope, all three header fields written
//! as unsigned varints: `frameVersion(uvarint)=1 | apiKey(uvarint) |
//! apiVersion(uvarint) | flexible-message-body`).
//!
//! apiKey mapping:
//! - 0 = `RemoteLogSegmentMetadataRecord`  → [`MetadataEvent::AddSegment`]
//! - 1 = `RemoteLogSegmentMetadataUpdateRecord` → [`MetadataEvent::UpdateSegment`]
//! - 2 = `RemotePartitionDeleteMetadataRecord` → [`MetadataEvent::PartitionDelete`]

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use crabka_ids::LeaderEpoch;
use crabka_protocol::{
    RemoteLogMetadataRecord,
    owned::{
        remote_log_segment_metadata_record::{
            RemoteLogSegmentIdEntry as SegIdEntry, RemoteLogSegmentMetadataRecord,
            SegmentLeaderEpochEntry, TopicIdPartitionEntry as TpEntry,
        },
        remote_log_segment_metadata_update_record::{
            RemoteLogSegmentIdEntry as SegIdEntryUpd, RemoteLogSegmentMetadataUpdateRecord,
            TopicIdPartitionEntry as TpEntryUpd,
        },
        remote_partition_delete_metadata_record::{
            RemotePartitionDeleteMetadataRecord, TopicIdPartitionEntry as TpEntryDel,
        },
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use crabka_remote_storage::{
    CustomMetadata, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemotePartitionDeleteState,
    TopicIdPartition,
};

use crate::error::CodecError;

/// One of the three event variants the topic carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataEvent {
    /// A new segment is starting to copy (`CopySegmentStarted`).
    AddSegment(RemoteLogSegmentMetadata),
    /// Lifecycle transition for an existing segment.
    UpdateSegment(RemoteLogSegmentMetadataUpdate),
    /// Partition-delete lifecycle.
    PartitionDelete(RemotePartitionDeleteMetadata),
}

impl MetadataEvent {
    /// Encode this event into freshly-allocated [`Bytes`] using the JVM
    /// `RemoteLogMetadataSerde` wire format.
    ///
    /// # Panics
    ///
    /// Only panics if the generated codec rejects the version (impossible
    /// for version 0, which every record uses).
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let record = match self {
            Self::AddSegment(md) => RemoteLogMetadataRecord::SegmentMetadata(to_proto_add(md)),
            Self::UpdateSegment(u) => {
                RemoteLogMetadataRecord::SegmentMetadataUpdate(to_proto_update(u))
            }
            Self::PartitionDelete(d) => {
                RemoteLogMetadataRecord::PartitionDelete(to_proto_partition_delete(d))
            }
        };
        record
            .encode_value()
            .expect("RemoteLogMetadataRecord::encode_value must not fail for version 0")
    }

    /// Decode an event from `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] for any malformed input: truncated envelope,
    /// unknown or unsupported apiKey (apiKey 3 = `SegmentMetadataSnapshot`
    /// never appears on the topic), out-of-range state byte, or domain
    /// invariant violations reported by [`RemoteLogSegmentMetadata::new`].
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let record = RemoteLogMetadataRecord::decode_value(bytes)
            .map_err(|e| CodecError::Protocol(e.to_string()))?;
        match record {
            RemoteLogMetadataRecord::SegmentMetadata(r) => Ok(Self::AddSegment(from_proto_add(r)?)),
            RemoteLogMetadataRecord::SegmentMetadataUpdate(r) => {
                Ok(Self::UpdateSegment(from_proto_update(r)?))
            }
            RemoteLogMetadataRecord::PartitionDelete(r) => {
                Ok(Self::PartitionDelete(from_proto_partition_delete(r)?))
            }
            RemoteLogMetadataRecord::SegmentMetadataSnapshot(_) => Err(CodecError::Protocol(
                "apiKey 3 (SegmentMetadataSnapshot) must not appear on __remote_log_metadata"
                    .into(),
            )),
            RemoteLogMetadataRecord::Unknown { api_key, .. } => Err(CodecError::Protocol(format!(
                "unknown __remote_log_metadata apiKey {api_key}"
            ))),
        }
    }
}

// ───────────────────────── domain → protocol ─────────────────────────

fn domain_uuid_to_proto(u: uuid::Uuid) -> ProtoUuid {
    ProtoUuid(*u.as_bytes())
}

fn proto_uuid_to_domain(u: ProtoUuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(u.0)
}

fn segment_state_to_i8(s: RemoteLogSegmentState) -> i8 {
    match s {
        RemoteLogSegmentState::CopySegmentStarted => 0,
        RemoteLogSegmentState::CopySegmentFinished => 1,
        RemoteLogSegmentState::DeleteSegmentStarted => 2,
        RemoteLogSegmentState::DeleteSegmentFinished => 3,
    }
}

fn i8_to_segment_state(v: i8) -> Result<RemoteLogSegmentState, CodecError> {
    match v {
        0 => Ok(RemoteLogSegmentState::CopySegmentStarted),
        1 => Ok(RemoteLogSegmentState::CopySegmentFinished),
        2 => Ok(RemoteLogSegmentState::DeleteSegmentStarted),
        3 => Ok(RemoteLogSegmentState::DeleteSegmentFinished),
        other => Err(CodecError::UnknownState(
            other.cast_unsigned(),
            "RemoteLogSegmentState",
        )),
    }
}

fn partition_state_to_i8(s: RemotePartitionDeleteState) -> i8 {
    match s {
        RemotePartitionDeleteState::DeletePartitionMarked => 0,
        RemotePartitionDeleteState::DeletePartitionStarted => 1,
        RemotePartitionDeleteState::DeletePartitionFinished => 2,
    }
}

fn i8_to_partition_state(v: i8) -> Result<RemotePartitionDeleteState, CodecError> {
    match v {
        0 => Ok(RemotePartitionDeleteState::DeletePartitionMarked),
        1 => Ok(RemotePartitionDeleteState::DeletePartitionStarted),
        2 => Ok(RemotePartitionDeleteState::DeletePartitionFinished),
        other => Err(CodecError::UnknownState(
            other.cast_unsigned(),
            "RemotePartitionDeleteState",
        )),
    }
}

fn custom_metadata_to_bytes(cm: Option<&CustomMetadata>) -> Option<Bytes> {
    cm.map(|c| Bytes::from(c.0.clone()))
}

fn bytes_to_custom_metadata(b: Option<Bytes>) -> Option<CustomMetadata> {
    b.map(|b| CustomMetadata(b.to_vec()))
}

fn tp_to_proto_add(tp: &TopicIdPartition) -> TpEntry {
    TpEntry {
        name: tp.topic.clone(),
        id: domain_uuid_to_proto(tp.topic_id),
        partition: tp.partition,
        ..Default::default()
    }
}

fn proto_tp_add_to_domain(tp: TpEntry) -> TopicIdPartition {
    TopicIdPartition::new(proto_uuid_to_domain(tp.id), tp.name, tp.partition)
}

fn seg_id_to_proto_add(id: &RemoteLogSegmentId) -> SegIdEntry {
    SegIdEntry {
        topic_id_partition: tp_to_proto_add(&id.topic_id_partition),
        id: domain_uuid_to_proto(id.id),
        ..Default::default()
    }
}

fn proto_seg_id_add_to_domain(id: SegIdEntry) -> RemoteLogSegmentId {
    RemoteLogSegmentId::new(
        proto_tp_add_to_domain(id.topic_id_partition),
        proto_uuid_to_domain(id.id),
    )
}

fn epochs_to_proto(epochs: &BTreeMap<LeaderEpoch, i64>) -> Vec<SegmentLeaderEpochEntry> {
    epochs
        .iter()
        .map(|(&epoch, &offset)| SegmentLeaderEpochEntry {
            // Wire boundary: unwrap `LeaderEpoch` to the raw `int32` the
            // JVM `RemoteLogMetadataSerde` writes.
            leader_epoch: epoch.0,
            offset,
            ..Default::default()
        })
        .collect()
}

fn proto_epochs_to_domain(entries: Vec<SegmentLeaderEpochEntry>) -> BTreeMap<LeaderEpoch, i64> {
    entries
        .into_iter()
        // Wire boundary: wrap the raw `int32` back into `LeaderEpoch`.
        .map(|e| (LeaderEpoch(e.leader_epoch), e.offset))
        .collect()
}

fn to_proto_add(md: &RemoteLogSegmentMetadata) -> RemoteLogSegmentMetadataRecord {
    RemoteLogSegmentMetadataRecord {
        remote_log_segment_id: seg_id_to_proto_add(md.remote_log_segment_id()),
        start_offset: md.start_offset(),
        end_offset: md.end_offset(),
        broker_id: md.broker_id(),
        max_timestamp_ms: md.max_timestamp_ms(),
        event_timestamp_ms: md.event_timestamp_ms(),
        segment_leader_epochs: epochs_to_proto(md.segment_leader_epochs()),
        segment_size_in_bytes: md.segment_size_in_bytes(),
        custom_metadata: custom_metadata_to_bytes(md.custom_metadata()),
        remote_log_segment_state: segment_state_to_i8(md.state()),
        txn_index_empty: md.txn_index_empty(),
        ..Default::default()
    }
}

fn from_proto_add(
    r: RemoteLogSegmentMetadataRecord,
) -> Result<RemoteLogSegmentMetadata, CodecError> {
    let id = proto_seg_id_add_to_domain(r.remote_log_segment_id);
    let state = i8_to_segment_state(r.remote_log_segment_state)?;
    let segment_leader_epochs = proto_epochs_to_domain(r.segment_leader_epochs);
    let custom = bytes_to_custom_metadata(r.custom_metadata);
    let mut md = RemoteLogSegmentMetadata::new(
        id,
        r.start_offset,
        r.end_offset,
        r.max_timestamp_ms,
        r.broker_id,
        r.event_timestamp_ms,
        r.segment_size_in_bytes,
        state,
        segment_leader_epochs,
    )
    .map_err(|e| CodecError::Domain(e.to_string()))?;
    if let Some(c) = custom {
        md = md.with_custom_metadata(c);
    }
    md = md.with_txn_index_empty(r.txn_index_empty);
    Ok(md)
}

// ── Update record ──

fn tp_to_proto_upd(tp: &TopicIdPartition) -> TpEntryUpd {
    TpEntryUpd {
        name: tp.topic.clone(),
        id: domain_uuid_to_proto(tp.topic_id),
        partition: tp.partition,
        ..Default::default()
    }
}

fn proto_tp_upd_to_domain(tp: TpEntryUpd) -> TopicIdPartition {
    TopicIdPartition::new(proto_uuid_to_domain(tp.id), tp.name, tp.partition)
}

fn seg_id_to_proto_upd(id: &RemoteLogSegmentId) -> SegIdEntryUpd {
    SegIdEntryUpd {
        topic_id_partition: tp_to_proto_upd(&id.topic_id_partition),
        id: domain_uuid_to_proto(id.id),
        ..Default::default()
    }
}

fn proto_seg_id_upd_to_domain(id: SegIdEntryUpd) -> RemoteLogSegmentId {
    RemoteLogSegmentId::new(
        proto_tp_upd_to_domain(id.topic_id_partition),
        proto_uuid_to_domain(id.id),
    )
}

fn to_proto_update(u: &RemoteLogSegmentMetadataUpdate) -> RemoteLogSegmentMetadataUpdateRecord {
    RemoteLogSegmentMetadataUpdateRecord {
        remote_log_segment_id: seg_id_to_proto_upd(&u.remote_log_segment_id),
        broker_id: u.broker_id,
        event_timestamp_ms: u.event_timestamp_ms,
        custom_metadata: custom_metadata_to_bytes(u.custom_metadata.as_ref()),
        remote_log_segment_state: segment_state_to_i8(u.state),
        ..Default::default()
    }
}

fn from_proto_update(
    r: RemoteLogSegmentMetadataUpdateRecord,
) -> Result<RemoteLogSegmentMetadataUpdate, CodecError> {
    let remote_log_segment_id = proto_seg_id_upd_to_domain(r.remote_log_segment_id);
    let state = i8_to_segment_state(r.remote_log_segment_state)?;
    Ok(RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id,
        event_timestamp_ms: r.event_timestamp_ms,
        custom_metadata: bytes_to_custom_metadata(r.custom_metadata),
        state,
        broker_id: r.broker_id,
    })
}

// ── PartitionDelete record ──

fn tp_to_proto_del(tp: &TopicIdPartition) -> TpEntryDel {
    TpEntryDel {
        name: tp.topic.clone(),
        id: domain_uuid_to_proto(tp.topic_id),
        partition: tp.partition,
        ..Default::default()
    }
}

fn proto_tp_del_to_domain(tp: TpEntryDel) -> TopicIdPartition {
    TopicIdPartition::new(proto_uuid_to_domain(tp.id), tp.name, tp.partition)
}

fn to_proto_partition_delete(
    d: &RemotePartitionDeleteMetadata,
) -> RemotePartitionDeleteMetadataRecord {
    RemotePartitionDeleteMetadataRecord {
        topic_id_partition: tp_to_proto_del(&d.topic_id_partition),
        broker_id: d.broker_id,
        event_timestamp_ms: d.event_timestamp_ms,
        remote_partition_delete_state: partition_state_to_i8(d.state),
        ..Default::default()
    }
}

fn from_proto_partition_delete(
    r: RemotePartitionDeleteMetadataRecord,
) -> Result<RemotePartitionDeleteMetadata, CodecError> {
    let topic_id_partition = proto_tp_del_to_domain(r.topic_id_partition);
    let state = i8_to_partition_state(r.remote_partition_delete_state)?;
    Ok(RemotePartitionDeleteMetadata {
        topic_id_partition,
        state,
        event_timestamp_ms: r.event_timestamp_ms,
        broker_id: r.broker_id,
    })
}

// ───────────────────────── varint + reader ─────────────────────────
//
// These helpers are retained because `snapshot.rs` uses them for its own
// envelope framing (format-version, committed-offsets, entry-length prefixes).
// They are NOT part of the MetadataEvent wire format.

/// Unsigned LEB128 — 7 data bits per byte, MSB is the continuation flag.
/// Encodes 1 byte for values < 128.
#[allow(clippy::cast_possible_truncation)] // varint encode-byte by design
pub(crate) fn write_uvarint(mut v: u64, buf: &mut BytesMut) {
    while v >= 0x80 {
        buf.put_u8(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

pub(crate) fn read_uvarint(r: &mut Reader<'_>) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    for shift in (0..10).map(|i| i * 7) {
        let byte = r.read_u8()?;
        result |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(CodecError::LengthOverflow(u64::MAX))
}

pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn read_u8(&mut self) -> Result<u8, CodecError> {
        let &b = self
            .buf
            .get(self.pos)
            .ok_or(CodecError::UnexpectedEof(self.pos))?;
        self.pos += 1;
        Ok(b)
    }

    pub(crate) fn read_i32(&mut self) -> Result<i32, CodecError> {
        let bytes: [u8; 4] = self
            .read_n(4)?
            .try_into()
            .expect("read_n returned exact length");
        Ok(i32::from_be_bytes(bytes))
    }

    pub(crate) fn read_i64(&mut self) -> Result<i64, CodecError> {
        let bytes: [u8; 8] = self
            .read_n(8)?
            .try_into()
            .expect("read_n returned exact length");
        Ok(i64::from_be_bytes(bytes))
    }

    pub(crate) fn read_n(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(CodecError::LengthOverflow(n as u64))?;
        if end > self.buf.len() {
            return Err(CodecError::UnexpectedEof(self.pos));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(0xCAFE_BABE), "orders-📦", 7)
    }

    fn seg_id(id: u128) -> RemoteLogSegmentId {
        RemoteLogSegmentId::new(tp(), Uuid::from_u128(id))
    }

    fn add(id: u128, start: i64, end: i64, custom: Option<Vec<u8>>) -> RemoteLogSegmentMetadata {
        let mut md = RemoteLogSegmentMetadata::new(
            seg_id(id),
            start,
            end,
            end + 1,
            42,
            123,
            4096,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([
                (LeaderEpoch(0), start),
                (LeaderEpoch(1), start + 10),
                (LeaderEpoch(2), start + 20),
            ]),
        )
        .unwrap();
        if let Some(c) = custom {
            md = md.with_custom_metadata(CustomMetadata(c));
        }
        md
    }

    #[test]
    fn round_trip_add_with_custom_metadata() {
        let event = MetadataEvent::AddSegment(add(1, 0, 99, Some(vec![1, 2, 3, 4])));
        let bytes = event.encode();
        let back = MetadataEvent::decode(&bytes).expect("decodes");
        assert!(back == event);
    }

    #[test]
    fn round_trip_add_without_custom_metadata() {
        let event = MetadataEvent::AddSegment(add(2, 100, 199, None));
        let bytes = event.encode();
        assert!(MetadataEvent::decode(&bytes).unwrap() == event);
    }

    #[test]
    fn round_trip_update_finish() {
        let event = MetadataEvent::UpdateSegment(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(3),
            event_timestamp_ms: 999,
            custom_metadata: Some(CustomMetadata(vec![9, 8, 7])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 13,
        });
        let bytes = event.encode();
        assert!(MetadataEvent::decode(&bytes).unwrap() == event);
    }

    #[test]
    fn round_trip_update_no_custom_metadata() {
        let event = MetadataEvent::UpdateSegment(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(4),
            event_timestamp_ms: 1,
            custom_metadata: None,
            state: RemoteLogSegmentState::DeleteSegmentStarted,
            broker_id: 0,
        });
        let bytes = event.encode();
        assert!(MetadataEvent::decode(&bytes).unwrap() == event);
    }

    #[test]
    fn round_trip_partition_delete_each_state() {
        for state in [
            RemotePartitionDeleteState::DeletePartitionMarked,
            RemotePartitionDeleteState::DeletePartitionStarted,
            RemotePartitionDeleteState::DeletePartitionFinished,
        ] {
            let event = MetadataEvent::PartitionDelete(RemotePartitionDeleteMetadata {
                topic_id_partition: tp(),
                state,
                event_timestamp_ms: 500,
                broker_id: 1,
            });
            let bytes = event.encode();
            assert!(MetadataEvent::decode(&bytes).unwrap() == event);
        }
    }

    #[test]
    fn add_round_trips_txn_index_empty_true() {
        let md = add(5, 0, 49, None).with_txn_index_empty(true);
        let event = MetadataEvent::AddSegment(md);
        let bytes = event.encode();
        let back = MetadataEvent::decode(&bytes).expect("decodes");
        assert!(back == event);
        if let MetadataEvent::AddSegment(ref md) = back {
            assert!(md.txn_index_empty());
        }
    }

    #[test]
    fn truncated_buffer_is_rejected() {
        let bytes = MetadataEvent::AddSegment(add(1, 0, 1, None))
            .encode()
            .to_vec();
        let err = MetadataEvent::decode(&bytes[..bytes.len() - 5]).unwrap_err();
        assert!(matches!(err, CodecError::Protocol(_)));
    }

    #[test]
    fn unknown_segment_state_is_rejected() {
        // Build a SegmentMetadata record with an out-of-range state byte (7),
        // encode it through the protocol envelope, then assert decode → Err.
        use crabka_protocol::owned::remote_log_segment_metadata_record::{
            RemoteLogSegmentMetadataRecord, SegmentLeaderEpochEntry,
        };
        // Provide a minimal valid epoch list so domain construction doesn't fail first.
        let rec = RemoteLogSegmentMetadataRecord {
            segment_leader_epochs: vec![SegmentLeaderEpochEntry {
                leader_epoch: 0,
                offset: 0,
                ..Default::default()
            }],
            remote_log_segment_state: 7,
            ..Default::default()
        };
        let bytes = crabka_protocol::RemoteLogMetadataRecord::SegmentMetadata(rec)
            .encode_value()
            .unwrap();
        let err = MetadataEvent::decode(&bytes).unwrap_err();
        assert!(matches!(err, CodecError::UnknownState(7, _)));
    }

    #[test]
    fn snapshot_apikey_is_rejected_on_topic() {
        use crabka_protocol::owned::remote_log_segment_metadata_snapshot_record::RemoteLogSegmentMetadataSnapshotRecord;
        let bytes = crabka_protocol::RemoteLogMetadataRecord::SegmentMetadataSnapshot(
            RemoteLogSegmentMetadataSnapshotRecord::default(),
        )
        .encode_value()
        .unwrap();
        let err = MetadataEvent::decode(&bytes).unwrap_err();
        assert!(matches!(err, CodecError::Protocol(_)));
    }
}
