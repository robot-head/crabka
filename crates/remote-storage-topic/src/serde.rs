//! Binary wire format for `__remote_log_metadata` events.
//!
//! Hand-rolled (no `serde` / `protobuf` / `bincode`) so the on-wire
//! bytes are obvious in tests and the crate stays dependency-light.
//! Versioned: every record begins with [`WIRE_VERSION`]; future
//! breaking changes bump the version and add a new decoder arm.
//!
//! Layout:
//!
//! ```text
//! event := u8 version
//!        | u8 tag (0 = AddSegment, 1 = UpdateSegment, 2 = PartitionDelete)
//!        | payload (tag-dependent)
//!
//! string   := uvarint length + UTF-8 bytes
//! uuid     := 16 raw bytes
//! i32      := 4 bytes big-endian
//! i64      := 8 bytes big-endian
//! opt<T>   := 0u8 (None) | 1u8 + T
//! bytes    := uvarint length + raw bytes
//! state-segment    := 0=CopyStarted, 1=CopyFinished, 2=DeleteStarted, 3=DeleteFinished
//! state-partition  := 0=DeleteMarked, 1=DeleteStarted, 2=DeleteFinished
//! ```

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crabka_remote_storage::{
    CustomMetadata, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemoteLogSegmentState, RemotePartitionDeleteMetadata, RemotePartitionDeleteState,
    TopicIdPartition,
};

use crate::error::CodecError;

/// Version byte at the head of every encoded event.
pub const WIRE_VERSION: u8 = 0;

const TAG_ADD: u8 = 0;
const TAG_UPDATE: u8 = 1;
const TAG_PARTITION_DELETE: u8 = 2;

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
    /// Encode this event into freshly-allocated [`Bytes`].
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(64);
        buf.put_u8(WIRE_VERSION);
        match self {
            Self::AddSegment(md) => {
                buf.put_u8(TAG_ADD);
                encode_add(md, &mut buf);
            }
            Self::UpdateSegment(u) => {
                buf.put_u8(TAG_UPDATE);
                encode_update(u, &mut buf);
            }
            Self::PartitionDelete(d) => {
                buf.put_u8(TAG_PARTITION_DELETE);
                encode_partition_delete(d, &mut buf);
            }
        }
        buf.freeze()
    }

    /// Decode an event from `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] for any malformed input — short buffer,
    /// unknown version/tag/state byte, oversize length prefix,
    /// non-UTF-8 string, or a payload that violates the domain
    /// invariants checked by [`RemoteLogSegmentMetadata::new`].
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut r = Reader::new(bytes);
        let version = r.read_u8()?;
        if version != WIRE_VERSION {
            return Err(CodecError::UnsupportedVersion(version));
        }
        let tag = r.read_u8()?;
        match tag {
            TAG_ADD => Ok(Self::AddSegment(decode_add(&mut r)?)),
            TAG_UPDATE => Ok(Self::UpdateSegment(decode_update(&mut r)?)),
            TAG_PARTITION_DELETE => Ok(Self::PartitionDelete(decode_partition_delete(&mut r)?)),
            other => Err(CodecError::UnknownTag(other)),
        }
    }
}

// ───────────────────────── encoders ─────────────────────────

fn encode_add(md: &RemoteLogSegmentMetadata, buf: &mut BytesMut) {
    encode_segment_id(md.remote_log_segment_id(), buf);
    buf.put_i64(md.start_offset());
    buf.put_i64(md.end_offset());
    buf.put_i64(md.max_timestamp_ms());
    buf.put_i32(md.broker_id());
    buf.put_i64(md.event_timestamp_ms());
    buf.put_i32(md.segment_size_in_bytes());
    encode_opt_bytes(md.custom_metadata().map(|c| c.0.as_slice()), buf);
    buf.put_u8(encode_segment_state(md.state()));
    encode_epoch_map(md.segment_leader_epochs(), buf);
}

fn encode_update(u: &RemoteLogSegmentMetadataUpdate, buf: &mut BytesMut) {
    encode_segment_id(&u.remote_log_segment_id, buf);
    buf.put_i64(u.event_timestamp_ms);
    encode_opt_bytes(u.custom_metadata.as_ref().map(|c| c.0.as_slice()), buf);
    buf.put_u8(encode_segment_state(u.state));
    buf.put_i32(u.broker_id);
}

fn encode_partition_delete(d: &RemotePartitionDeleteMetadata, buf: &mut BytesMut) {
    encode_tp(&d.topic_id_partition, buf);
    buf.put_u8(encode_partition_state(d.state));
    buf.put_i64(d.event_timestamp_ms);
    buf.put_i32(d.broker_id);
}

fn encode_segment_id(id: &RemoteLogSegmentId, buf: &mut BytesMut) {
    encode_tp(&id.topic_id_partition, buf);
    buf.put_slice(id.id.as_bytes());
}

fn encode_tp(tp: &TopicIdPartition, buf: &mut BytesMut) {
    buf.put_slice(tp.topic_id.as_bytes());
    encode_string(&tp.topic, buf);
    buf.put_i32(tp.partition);
}

fn encode_string(s: &str, buf: &mut BytesMut) {
    write_uvarint(s.len() as u64, buf);
    buf.put_slice(s.as_bytes());
}

fn encode_opt_bytes(b: Option<&[u8]>, buf: &mut BytesMut) {
    match b {
        None => buf.put_u8(0),
        Some(slice) => {
            buf.put_u8(1);
            write_uvarint(slice.len() as u64, buf);
            buf.put_slice(slice);
        }
    }
}

fn encode_epoch_map(map: &BTreeMap<i32, i64>, buf: &mut BytesMut) {
    write_uvarint(map.len() as u64, buf);
    for (&epoch, &offset) in map {
        buf.put_i32(epoch);
        buf.put_i64(offset);
    }
}

fn encode_segment_state(s: RemoteLogSegmentState) -> u8 {
    match s {
        RemoteLogSegmentState::CopySegmentStarted => 0,
        RemoteLogSegmentState::CopySegmentFinished => 1,
        RemoteLogSegmentState::DeleteSegmentStarted => 2,
        RemoteLogSegmentState::DeleteSegmentFinished => 3,
    }
}

fn encode_partition_state(s: RemotePartitionDeleteState) -> u8 {
    match s {
        RemotePartitionDeleteState::DeletePartitionMarked => 0,
        RemotePartitionDeleteState::DeletePartitionStarted => 1,
        RemotePartitionDeleteState::DeletePartitionFinished => 2,
    }
}

// ───────────────────────── decoders ─────────────────────────

fn decode_add(r: &mut Reader<'_>) -> Result<RemoteLogSegmentMetadata, CodecError> {
    let id = decode_segment_id(r)?;
    let start_offset = r.read_i64()?;
    let end_offset = r.read_i64()?;
    let max_timestamp_ms = r.read_i64()?;
    let broker_id = r.read_i32()?;
    let event_timestamp_ms = r.read_i64()?;
    let segment_size_in_bytes = r.read_i32()?;
    let custom_metadata = decode_opt_bytes(r)?;
    let state = decode_segment_state(r.read_u8()?)?;
    let segment_leader_epochs = decode_epoch_map(r)?;
    let mut md = RemoteLogSegmentMetadata::new(
        id,
        start_offset,
        end_offset,
        max_timestamp_ms,
        broker_id,
        event_timestamp_ms,
        segment_size_in_bytes,
        state,
        segment_leader_epochs,
    )
    .map_err(|e| CodecError::Domain(e.to_string()))?;
    if let Some(bytes) = custom_metadata {
        md = md.with_custom_metadata(CustomMetadata(bytes));
    }
    Ok(md)
}

fn decode_update(r: &mut Reader<'_>) -> Result<RemoteLogSegmentMetadataUpdate, CodecError> {
    let remote_log_segment_id = decode_segment_id(r)?;
    let event_timestamp_ms = r.read_i64()?;
    let custom_metadata = decode_opt_bytes(r)?.map(CustomMetadata);
    let state = decode_segment_state(r.read_u8()?)?;
    let broker_id = r.read_i32()?;
    Ok(RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id,
        event_timestamp_ms,
        custom_metadata,
        state,
        broker_id,
    })
}

fn decode_partition_delete(
    r: &mut Reader<'_>,
) -> Result<RemotePartitionDeleteMetadata, CodecError> {
    let topic_id_partition = decode_tp(r)?;
    let state = decode_partition_state(r.read_u8()?)?;
    let event_timestamp_ms = r.read_i64()?;
    let broker_id = r.read_i32()?;
    Ok(RemotePartitionDeleteMetadata {
        topic_id_partition,
        state,
        event_timestamp_ms,
        broker_id,
    })
}

fn decode_segment_id(r: &mut Reader<'_>) -> Result<RemoteLogSegmentId, CodecError> {
    let topic_id_partition = decode_tp(r)?;
    let mut uuid_bytes = [0u8; 16];
    r.read_exact(&mut uuid_bytes)?;
    Ok(RemoteLogSegmentId::new(
        topic_id_partition,
        Uuid::from_bytes(uuid_bytes),
    ))
}

fn decode_tp(r: &mut Reader<'_>) -> Result<TopicIdPartition, CodecError> {
    let mut uuid_bytes = [0u8; 16];
    r.read_exact(&mut uuid_bytes)?;
    let topic = decode_string(r)?;
    let partition = r.read_i32()?;
    Ok(TopicIdPartition::new(
        Uuid::from_bytes(uuid_bytes),
        topic,
        partition,
    ))
}

fn decode_string(r: &mut Reader<'_>) -> Result<String, CodecError> {
    let len = decode_len(r, "string")?;
    let bytes = r.read_n(len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidUtf8("string"))
}

fn decode_opt_bytes(r: &mut Reader<'_>) -> Result<Option<Vec<u8>>, CodecError> {
    match r.read_u8()? {
        0 => Ok(None),
        1 => {
            let len = decode_len(r, "bytes")?;
            Ok(Some(r.read_n(len)?.to_vec()))
        }
        other => Err(CodecError::UnknownTag(other)),
    }
}

fn decode_epoch_map(r: &mut Reader<'_>) -> Result<BTreeMap<i32, i64>, CodecError> {
    let len = decode_len(r, "epoch-map")?;
    let mut out = BTreeMap::new();
    for _ in 0..len {
        let epoch = r.read_i32()?;
        let offset = r.read_i64()?;
        out.insert(epoch, offset);
    }
    Ok(out)
}

fn decode_segment_state(byte: u8) -> Result<RemoteLogSegmentState, CodecError> {
    match byte {
        0 => Ok(RemoteLogSegmentState::CopySegmentStarted),
        1 => Ok(RemoteLogSegmentState::CopySegmentFinished),
        2 => Ok(RemoteLogSegmentState::DeleteSegmentStarted),
        3 => Ok(RemoteLogSegmentState::DeleteSegmentFinished),
        other => Err(CodecError::UnknownState(other, "RemoteLogSegmentState")),
    }
}

fn decode_partition_state(byte: u8) -> Result<RemotePartitionDeleteState, CodecError> {
    match byte {
        0 => Ok(RemotePartitionDeleteState::DeletePartitionMarked),
        1 => Ok(RemotePartitionDeleteState::DeletePartitionStarted),
        2 => Ok(RemotePartitionDeleteState::DeletePartitionFinished),
        other => Err(CodecError::UnknownState(
            other,
            "RemotePartitionDeleteState",
        )),
    }
}

fn decode_len(r: &mut Reader<'_>, _ctx: &'static str) -> Result<usize, CodecError> {
    let raw = read_uvarint(r)?;
    usize::try_from(raw).map_err(|_| CodecError::LengthOverflow(raw))
}

// ───────────────────────── varint + reader ─────────────────────────

/// Unsigned LEB128 — 7 data bits per byte, MSB is the continuation
/// flag. Encodes 1 byte for values < 128.
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

    fn read_exact(&mut self, dst: &mut [u8]) -> Result<(), CodecError> {
        let n = dst.len();
        let src = self.read_n(n)?;
        dst.copy_from_slice(src);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

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
            BTreeMap::from([(0, start), (1, start + 10), (2, start + 20)]),
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
        assert_eq!(back, event);
    }

    #[test]
    fn round_trip_add_without_custom_metadata() {
        let event = MetadataEvent::AddSegment(add(2, 100, 199, None));
        let bytes = event.encode();
        assert_eq!(MetadataEvent::decode(&bytes).unwrap(), event);
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
        assert_eq!(MetadataEvent::decode(&bytes).unwrap(), event);
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
        assert_eq!(MetadataEvent::decode(&bytes).unwrap(), event);
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
            assert_eq!(MetadataEvent::decode(&bytes).unwrap(), event);
        }
    }

    #[test]
    fn unknown_version_is_rejected() {
        let mut bytes = MetadataEvent::AddSegment(add(1, 0, 1, None))
            .encode()
            .to_vec();
        bytes[0] = WIRE_VERSION + 1;
        let err = MetadataEvent::decode(&bytes).unwrap_err();
        assert!(matches!(err, CodecError::UnsupportedVersion(_)));
    }

    #[test]
    fn unknown_tag_is_rejected() {
        let bytes = vec![WIRE_VERSION, 99];
        let err = MetadataEvent::decode(&bytes).unwrap_err();
        assert!(matches!(err, CodecError::UnknownTag(99)));
    }

    #[test]
    fn truncated_buffer_is_rejected() {
        let bytes = MetadataEvent::AddSegment(add(1, 0, 1, None))
            .encode()
            .to_vec();
        let err = MetadataEvent::decode(&bytes[..bytes.len() - 5]).unwrap_err();
        assert!(matches!(err, CodecError::UnexpectedEof(_)));
    }

    #[test]
    fn unknown_segment_state_is_rejected() {
        // Build a minimal add prefix, then overwrite the state byte to junk.
        // The state byte appears in two layouts (Add carries the
        // epoch map after it, Update / PartitionDelete don't); test
        // the decoder directly rather than locating it by offset.
        let err = decode_segment_state(7).unwrap_err();
        assert!(matches!(err, CodecError::UnknownState(7, _)));
    }

    #[test]
    fn varint_round_trip_at_boundaries() {
        for v in [
            0u64,
            1,
            127,
            128,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            let mut buf = BytesMut::new();
            write_uvarint(v, &mut buf);
            let bytes = buf.freeze();
            let mut r = Reader::new(&bytes);
            assert_eq!(read_uvarint(&mut r).unwrap(), v);
        }
    }

    #[test]
    fn empty_string_round_trips() {
        let mut buf = BytesMut::new();
        encode_string("", &mut buf);
        let bytes = buf.freeze();
        let mut r = Reader::new(&bytes);
        assert_eq!(decode_string(&mut r).unwrap(), "");
    }

    #[test]
    fn empty_epoch_map_is_rejected_at_construction_not_codec() {
        // The codec is happy with an empty map; the domain constructor
        // (RemoteLogSegmentMetadata::new) is what rejects it. Confirm a
        // payload built by encoding the bytes directly fails at decode
        // through CodecError::Domain rather than panicking.
        let mut buf = BytesMut::new();
        buf.put_u8(WIRE_VERSION);
        buf.put_u8(TAG_ADD);
        encode_segment_id(&seg_id(1), &mut buf);
        buf.put_i64(0);
        buf.put_i64(0);
        buf.put_i64(0);
        buf.put_i32(0);
        buf.put_i64(0);
        buf.put_i32(0);
        buf.put_u8(0); // no custom metadata
        buf.put_u8(0); // state = CopySegmentStarted
        write_uvarint(0, &mut buf); // empty epoch map
        let err = MetadataEvent::decode(&buf).unwrap_err();
        assert!(matches!(err, CodecError::Domain(_)));
    }
}
