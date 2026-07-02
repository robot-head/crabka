//! Zero-copy capture of a `ProduceRequest`'s per-partition `records` fields.
//!
//! The produce hot path normally decodes each partition's `records` field
//! into an owned `RecordBatch` (a full copy + parse). For the verbatim
//! passthrough fast path the broker instead wants the producer's *exact*
//! wire bytes as a refcounted [`Bytes`] slice of the request frame, so it
//! can append them without re-encoding.
//!
//! [`produce_record_slices`] walks the `ProduceRequest` body over a
//! [`Bytes`] cursor and, for every `(topic, partition)` in wire order,
//! returns the records field as a zero-copy `Bytes::slice` (via
//! `Buf::copy_to_bytes`, which on `Bytes` is a refcount bump, not a copy).
//!
//! The walk mirrors the generated `ProduceRequest::decode` field order
//! byte-for-byte (see `generated/ProduceRequest.owned.rs`); it is exercised
//! against the generated encoder in the tests below so the two cannot drift.

use bytes::{Buf, Bytes};

use crate::ProtocolError;
use crate::primitives::array::get_array_len;
use crate::primitives::fixed::{get_i16, get_i32};
use crate::primitives::string_bytes::{
    get_compact_nullable_string_owned, get_nullable_string_owned,
};
use crate::primitives::uuid::{Uuid, get_uuid};
use crate::primitives::varint::get_uvarint;
use crate::tagged_fields::read_tagged_fields;

/// `ProduceRequest`: flexible (KIP-482 tagged fields / compact encodings)
/// at version 9 and above. Mirrors `is_flexible` in the generated code.
const FLEXIBLE_MIN: i16 = 9;

/// The verbatim `records` field bytes for one `(topic, partition)` slot,
/// captured zero-copy from the request frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRecordSlice {
    /// Index of the topic within `ProduceRequest.topic_data` (wire order).
    pub topic_index: usize,
    /// Index of the partition within the topic's `partition_data`.
    pub partition_index: usize,
    /// The partition index (`PartitionProduceData.index`) on the wire.
    pub partition: i32,
    /// The records field as a zero-copy slice of the frame, or `None` when
    /// the field was wire-null.
    pub records: Option<Bytes>,
}

/// The request-level + per-topic framing of a `ProduceRequest`, captured by
/// [`produce_framing`] **without decoding (or decompressing) any record
/// batch**. This is the header-only view the broker's produce hot path uses
/// to decide verbatim-passthrough vs owned-decode per partition: every field
/// here comes straight off the wire framing; the actual record bytes are kept
/// as zero-copy [`Bytes`] slices in [`ProduceFramingTopic::partitions`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProduceFraming {
    /// `transactional_id` (v≥3, nullable). Drives the txn ACL preamble.
    pub transactional_id: Option<String>,
    /// `acks` (-1 / 0 / 1).
    pub acks: i16,
    /// `timeout_ms`.
    pub timeout_ms: i32,
    /// Per-topic framing in wire order.
    pub topics: Vec<ProduceFramingTopic>,
}

/// One topic's framing within a [`ProduceFraming`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProduceFramingTopic {
    /// Topic name (STRING for v≤12; empty for v≥13 which is id-only).
    pub name: String,
    /// Topic id (16-byte UUID for v≥13; [`Uuid::ZERO`] for v≤12).
    pub topic_id: Uuid,
    /// Per-partition records slices in wire order.
    pub partitions: Vec<PartitionRecordSlice>,
}

/// Walk a `ProduceRequest` body (already positioned at the start of the
/// request body, i.e. after the request header) and return every
/// partition's `records` field as a zero-copy `Bytes` slice.
///
/// `body` must be a `Bytes` cursor over exactly the produce request body;
/// `version` is the produce-request API version. The walk does not
/// allocate for the records payloads — each slice is a refcount view into
/// `body`'s backing buffer.
///
/// # Errors
///
/// Returns [`ProtocolError`] if the bytes do not parse as a `ProduceRequest`
/// of the given version (malformed/short frame).
pub fn produce_record_slices(
    mut body: Bytes,
    version: i16,
) -> Result<Vec<PartitionRecordSlice>, ProtocolError> {
    let flex = version >= FLEXIBLE_MIN;
    let buf = &mut body;

    // transactional_id (v>=3, present for every version we serve).
    if version >= 3 {
        if flex {
            get_compact_nullable_string_owned(buf)?;
        } else {
            get_nullable_string_owned(buf)?;
        }
    }
    // acks (i16), timeout_ms (i32).
    let _acks = get_i16(buf)?;
    let _timeout_ms = get_i32(buf)?;

    let mut out = Vec::new();

    let topic_count = get_array_len(buf, flex)?;
    for topic_index in 0..topic_count {
        // name: STRING/COMPACT_STRING for v<=12; topic_id (16 bytes) for v>=13.
        if version <= 12 {
            if flex {
                get_compact_nullable_string_owned(buf)?;
            } else {
                get_nullable_string_owned(buf)?;
            }
        }
        if version >= 13 {
            skip(buf, 16)?; // topic_id UUID
        }

        let partition_count = get_array_len(buf, flex)?;
        for partition_index in 0..partition_count {
            let partition = get_i32(buf)?;
            // records: NULLABLE_BYTES (v<9) / COMPACT_NULLABLE_BYTES (v>=9).
            let records = read_nullable_bytes_slice(buf, flex)?;
            out.push(PartitionRecordSlice {
                topic_index,
                partition_index,
                partition,
                records,
            });
            // Per-partition tagged fields (flexible only).
            if flex {
                read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
            }
        }
        // Per-topic tagged fields (flexible only).
        if flex {
            read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        }
    }
    // Request-level tagged fields (flexible only) — not needed, but consume
    // them for totality so a trailing-byte assertion in callers holds.
    if flex {
        read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
    }

    Ok(out)
}

/// Walk a `ProduceRequest` body (v≥3) and return the full request +
/// per-topic framing **without decoding or decompressing any record
/// batch**. Each partition's `records` field is captured as a zero-copy
/// [`Bytes`] slice of `body`.
///
/// This is the header-only entry point for the produce hot path: it gives
/// the handler everything it needs to run the ACL preamble, topic
/// resolution, and the verbatim-vs-owned dispatch, while leaving the
/// (potentially LZ4-compressed, 100×-expanding) record bodies untouched.
/// The owned fallback re-decodes a single partition's slice only when the
/// passthrough predicate fails.
///
/// The walk mirrors the generated `ProduceRequest::decode` field order
/// byte-for-byte; it is exercised against the generated encoder in the
/// tests below so the two cannot drift.
///
/// # Errors
///
/// Returns [`ProtocolError`] if the bytes do not parse as a `ProduceRequest`
/// of the given version (malformed/short frame).
pub fn produce_framing(mut body: Bytes, version: i16) -> Result<ProduceFraming, ProtocolError> {
    let flex = version >= FLEXIBLE_MIN;
    let buf = &mut body;

    // transactional_id (v>=3, present for every version we serve).
    let transactional_id = if version >= 3 {
        if flex {
            get_compact_nullable_string_owned(buf)?
        } else {
            get_nullable_string_owned(buf)?
        }
    } else {
        None
    };
    let acks = get_i16(buf)?;
    let timeout_ms = get_i32(buf)?;

    let topic_count = get_array_len(buf, flex)?;
    let mut topics = Vec::with_capacity(topic_count);
    for topic_index in 0..topic_count {
        // name: STRING/COMPACT_STRING for v<=12; topic_id (16 bytes) for v>=13.
        let mut name = String::new();
        if version <= 12 {
            name = if flex {
                get_compact_nullable_string_owned(buf)?.unwrap_or_default()
            } else {
                get_nullable_string_owned(buf)?.unwrap_or_default()
            };
        }
        let mut topic_id = Uuid::ZERO;
        if version >= 13 {
            topic_id = get_uuid(buf)?;
        }

        let partition_count = get_array_len(buf, flex)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for partition_index in 0..partition_count {
            let partition = get_i32(buf)?;
            let records = read_nullable_bytes_slice(buf, flex)?;
            partitions.push(PartitionRecordSlice {
                topic_index,
                partition_index,
                partition,
                records,
            });
            // Per-partition tagged fields (flexible only).
            if flex {
                read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
            }
        }
        // Per-topic tagged fields (flexible only).
        if flex {
            read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        }
        topics.push(ProduceFramingTopic {
            name,
            topic_id,
            partitions,
        });
    }
    // Request-level tagged fields (flexible only).
    if flex {
        read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
    }

    Ok(ProduceFraming {
        transactional_id,
        acks,
        timeout_ms,
        topics,
    })
}

/// Read a `NULLABLE_BYTES` / `COMPACT_NULLABLE_BYTES` length prefix and
/// return the payload as a **zero-copy** `Bytes` slice of `buf`.
fn read_nullable_bytes_slice(buf: &mut Bytes, flex: bool) -> Result<Option<Bytes>, ProtocolError> {
    let len = if flex {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            return Ok(None);
        }
        (raw - 1) as usize
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            return Ok(None);
        }
        usize::try_from(n).expect("non-negative i32 fits usize")
    };
    if buf.remaining() < len {
        return Err(ProtocolError::UnexpectedEof {
            needed: len - buf.remaining(),
        });
    }
    // `Bytes::copy_to_bytes` is a refcount split, not a memcpy.
    Ok(Some(buf.copy_to_bytes(len)))
}

fn skip(buf: &mut Bytes, n: usize) -> Result<(), ProtocolError> {
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof {
            needed: n - buf.remaining(),
        });
    }
    buf.advance(n);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Encode;
    use crate::owned::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData};
    use crate::records::{Record, RecordBatch, RecordsPayload};
    use assert2::{assert, check};
    use bytes::BytesMut;

    fn batch_with_value(v: &[u8], base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            records: vec![Record {
                value: Some(Bytes::copy_from_slice(v)),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Encode a produce request, then assert the captured slices match the
    /// records the full decoder produces — byte-for-byte — for every
    /// (topic, partition).
    fn check_roundtrip(req: &ProduceRequest, version: i16) {
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        let body = buf.freeze();

        let slices = produce_record_slices(body.clone(), version).unwrap();

        // Flatten the request's (topic, partition) records in wire order.
        let mut expected: Vec<Option<Bytes>> = Vec::new();
        for t in &req.topic_data {
            for p in &t.partition_data {
                let enc = p.records.as_ref().map(|rp| {
                    let mut b = BytesMut::new();
                    <RecordsPayload as Encode>::encode(rp, &mut b, version).unwrap();
                    b.freeze()
                });
                expected.push(enc);
            }
        }
        assert!(
            slices.len() == expected.len(),
            "slice count mismatch: got {}, want {}",
            slices.len(),
            expected.len()
        );
        for (got, want) in slices.iter().zip(expected.iter()) {
            match (&got.records, want) {
                (Some(g), Some(w)) => assert!(&g[..] == &w[..], "records bytes differ"),
                (None, None) => {}
                _ => panic!("nullability mismatch"),
            }
        }
    }

    fn multi_partition_request(version: i16) -> ProduceRequest {
        let topic = |name: &str, parts: usize| TopicProduceData {
            name: if version <= 12 {
                name.to_string()
            } else {
                String::new()
            },
            topic_id: if version >= 13 {
                crate::primitives::uuid::Uuid([7u8; 16])
            } else {
                crate::primitives::uuid::Uuid::ZERO
            },
            partition_data: (0..parts)
                .map(|i| PartitionProduceData {
                    index: i32::try_from(i).unwrap(),
                    records: Some(RecordsPayload::V2(vec![batch_with_value(
                        format!("topic-{name}-part-{i}").as_bytes(),
                        i64::try_from(i).unwrap(),
                    )])),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 1000,
            topic_data: vec![topic("alpha", 2), topic("beta", 1)],
            ..Default::default()
        }
    }

    #[test]
    fn captures_match_decoder_non_flexible() {
        // v3..=8 are non-flexible (i32 length prefixes, no tagged fields).
        for version in 3..=8 {
            check_roundtrip(&multi_partition_request(version), version);
        }
    }

    #[test]
    fn captures_match_decoder_flexible() {
        // v9..=13 are flexible (compact encodings + tagged fields).
        for version in 9..=13 {
            check_roundtrip(&multi_partition_request(version), version);
        }
    }

    #[test]
    fn handles_null_records_field() {
        let version = 9;
        let req = ProduceRequest {
            transactional_id: Some("txn".to_string()),
            acks: 1,
            timeout_ms: 0,
            topic_data: vec![TopicProduceData {
                name: "t".to_string(),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: None,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        let slices = produce_record_slices(buf.freeze(), version).unwrap();
        assert!(slices.len() == 1);
        assert!(slices[0].records.is_none());
    }

    #[test]
    fn captured_slice_is_zero_copy_view() {
        let version = 9;
        let req = multi_partition_request(version);
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        let body = buf.freeze();
        let body_start = body.as_ptr() as usize;
        let body_end = body_start + body.len();

        let slices = produce_record_slices(body.clone(), version).unwrap();
        let first = slices[0].records.as_ref().unwrap();
        let ptr = first.as_ptr() as usize;
        assert!(
            ptr >= body_start && ptr < body_end,
            "captured slice must point into the request frame (zero-copy)"
        );
    }

    /// `produce_framing` must reproduce the request-level + per-topic +
    /// per-partition framing that the full owned decoder produces, and
    /// capture the same zero-copy records bytes — for every served version.
    fn check_framing_roundtrip(req: &ProduceRequest, version: i16) {
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        let body = buf.freeze();

        let framing = produce_framing(body.clone(), version).unwrap();

        check!(framing.transactional_id == req.transactional_id);
        check!(framing.acks == req.acks);
        check!(framing.timeout_ms == req.timeout_ms);
        assert!(framing.topics.len() == req.topic_data.len());
        for (ft, rt) in framing.topics.iter().zip(req.topic_data.iter()) {
            check!(ft.name == rt.name);
            check!(ft.topic_id.0 == rt.topic_id.0);
            assert!(ft.partitions.len() == rt.partition_data.len());
            for (fp, rp) in ft.partitions.iter().zip(rt.partition_data.iter()) {
                check!(fp.partition == rp.index);
                let want = rp.records.as_ref().map(|rpld| {
                    let mut b = BytesMut::new();
                    <RecordsPayload as Encode>::encode(rpld, &mut b, version).unwrap();
                    b.freeze()
                });
                match (&fp.records, &want) {
                    (Some(g), Some(w)) => assert!(&g[..] == &w[..], "records bytes differ"),
                    (None, None) => {}
                    _ => panic!("nullability mismatch"),
                }
            }
        }
    }

    #[test]
    fn framing_matches_decoder_all_versions() {
        for version in 3..=13 {
            check_framing_roundtrip(&multi_partition_request(version), version);
        }
    }

    #[test]
    fn framing_captures_transactional_id_and_acks() {
        let version = 9;
        let req = ProduceRequest {
            transactional_id: Some("my-txn".to_string()),
            acks: -1,
            timeout_ms: 7777,
            topic_data: vec![TopicProduceData {
                name: "t".to_string(),
                partition_data: vec![PartitionProduceData {
                    index: 3,
                    records: None,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        let framing = produce_framing(buf.freeze(), version).unwrap();
        let expected = ProduceFraming {
            transactional_id: Some("my-txn".to_string()),
            acks: -1,
            timeout_ms: 7777,
            topics: vec![ProduceFramingTopic {
                name: "t".to_string(),
                topic_id: Uuid::ZERO,
                partitions: vec![PartitionRecordSlice {
                    topic_index: 0,
                    partition_index: 0,
                    partition: 3,
                    records: None,
                }],
            }],
        };
        assert!(framing == expected);
    }

    #[test]
    fn empty_topic_list_yields_no_slices() {
        let version = 9;
        let req = ProduceRequest {
            acks: 1,
            timeout_ms: 0,
            topic_data: vec![],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, version).unwrap();
        let slices = produce_record_slices(buf.freeze(), version).unwrap();
        assert!(slices.is_empty());
    }
}
