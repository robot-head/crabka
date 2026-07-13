//! Write-plan serialization for `FetchResponse` (the broker's zero-copy fetch
//! path, "Increment C").
//!
//! The generated [`FetchResponse::encode`] materializes the whole response —
//! including every partition's records region — into one contiguous
//! `BytesMut`, which forces the broker to copy the (potentially 100 KB+)
//! records bytes through a throwaway buffer (`__rb_buf`), then again into the
//! `Framed` codec's write buffer, then a third time to prepend the response
//! header. This module produces the **same bytes** as an ordered list of
//! [`FetchWriteOp`] segments so the broker can write the envelope inline and
//! hand each partition's records region to the socket directly (vectored
//! write, or — on Linux plaintext — `sendfile`), without copying the records
//! payload.
//!
//! ## Byte-exactness invariant
//!
//! Concatenating the bytes of every [`FetchWriteOp`] produced by
//! [`FetchResponse::write_plan`] yields **exactly** the bytes that
//! [`FetchResponse::encode`] writes, for every Fetch version v4..=v18 (the
//! generated `MIN_VERSION..=MAX_VERSION`), covering both the non-flexible
//! (`INT32` length prefix, v4–v11) and flexible (`COMPACT_BYTES`
//! unsigned-varint prefix + trailing tagged-field buffer, v12+) records-field
//! encodings, multi-topic / multi-partition / multi-batch responses, and the
//! truncated-trailing-batch case (the records `Bytes` is emitted verbatim, so
//! a clipped final batch rides through untouched).
//!
//! This is enforced by `fetch_response_plan_matches_encode` in the test module
//! below, which asserts `concat(write_plan) == encode` for `populated(version)`
//! and hand-built multi-partition fixtures across all versions.

use bytes::{Bytes, BytesMut};

use super::{FetchResponse, FetchableTopicResponse, PartitionData};
use crate::{
    ProtocolError,
    primitives::{
        array::{array_len_prefix_len, put_array_len, put_nullable_array_len},
        fixed::{put_i16, put_i32, put_i64},
        string_bytes::{put_compact_string, put_string},
        uuid::put_uuid,
    },
    records::RecordsPayload,
    tagged_fields::{WriteTaggedFields, encode_to_bytes},
};

/// One ordered segment of a `FetchResponse` write-plan.
///
/// The broker drains these in order: `Inline` bytes are written from
/// userspace; a `Records` segment carries the partition's records payload,
/// which the broker resolves to a vectored `write_all` (any payload kind) or
/// — on Linux plaintext for `RecordsPayload::FileRegions` — a `sendfile`.
///
/// The `Records` op holds the [`RecordsPayload`] itself (a cheap refcounted
/// clone for `Raw`/`FileRegions`) rather than pre-encoded bytes so the broker
/// can choose its drain strategy. The records **length prefix** (`INT32` for
/// non-flex, unsigned-varint for flex) is part of the **preceding** `Inline`
/// segment, exactly as the wire format requires; `Records` is the raw payload
/// region only.
#[derive(Debug, Clone)]
pub enum FetchWriteOp {
    /// Userspace bytes — response header fragment, partition metadata,
    /// records length prefixes, tagged-field trailers, array prefixes, etc.
    Inline(Bytes),
    /// A partition's records payload region (no length prefix). Drained by
    /// the broker via vectored write or sendfile.
    Records(RecordsPayload),
}

impl FetchWriteOp {
    /// Byte length this op contributes to the frame body.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline(b) => b.len(),
            Self::Records(p) => p.payload_len(),
        }
    }

    /// `true` when this op contributes zero bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl FetchResponse {
    /// Serialize this response as an ordered [`FetchWriteOp`] plan whose
    /// concatenated bytes are **byte-identical** to [`crate::Encode::encode`]
    /// at the same `version`.
    ///
    /// Only valid for the canonical (v4+) Fetch codec — the legacy
    /// `kafka_3_6_2` path (v0–v3) does not get a plan (it down-converts and
    /// stays on the copy path). The caller must gate on `version >= 4`.
    ///
    /// The records length prefix is written into the inline segment that
    /// precedes each `Records` op, so resolving `Records` to its bytes and
    /// concatenating reproduces the exact wire frame.
    ///
    /// # Errors
    /// Returns an error if the response cannot be encoded for `version`.
    pub fn write_plan(&self, version: i16) -> Result<Vec<FetchWriteOp>, ProtocolError> {
        debug_assert!(
            version >= 4,
            "write_plan is only valid for the canonical Fetch codec (v4+)"
        );
        let flex = version >= 12;
        let mut ops: Vec<FetchWriteOp> = Vec::new();
        // Running inline accumulator. Flushed (as an `Inline` op) whenever a
        // `Records` op must be emitted, and once more at the end.
        let mut buf = BytesMut::new();

        // --- top-level header ---
        if version >= 1 {
            put_i32(&mut buf, self.throttle_time_ms);
        }
        if version >= 7 {
            put_i16(&mut buf, self.error_code);
        }
        if version >= 7 {
            put_i32(&mut buf, self.session_id);
        }
        // responses[] (always present, v0+)
        put_array_len(&mut buf, self.responses.len(), flex);
        for topic in &self.responses {
            encode_topic(&mut buf, &mut ops, topic, version, flex)?;
        }

        // Top-level tagged fields (node_endpoints at tag 0, v16+). These come
        // *after* all partition records, so they always land in the trailing
        // inline segment. Mirror the generated encoder exactly.
        if flex {
            let mut tagged = WriteTaggedFields::new();
            if !crate::codegen_helpers::is_default(&self.node_endpoints) {
                let node_endpoints = &self.node_endpoints;
                let payload = encode_to_bytes(
                    {
                        let prefix = array_len_prefix_len(node_endpoints.len(), flex);
                        let body: usize = node_endpoints
                            .iter()
                            .map(|it| crate::Encode::encoded_len(it, version))
                            .sum();
                        prefix + body
                    },
                    |b| {
                        put_array_len(b, node_endpoints.len(), flex);
                        for it in node_endpoints {
                            crate::Encode::encode(it, b, version)?;
                        }
                        Ok(())
                    },
                );
                tagged.add(0, payload);
            }
            tagged.write(&mut buf, &self.unknown_tagged_fields);
        }

        if !buf.is_empty() {
            ops.push(FetchWriteOp::Inline(buf.freeze()));
        }
        Ok(ops)
    }
}

/// Encode one `FetchableTopicResponse` worth of bytes, splitting out each
/// partition's records region as a `Records` op. Mirrors the generated
/// `FetchableTopicResponse::encode` field-for-field.
fn encode_topic(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    topic: &FetchableTopicResponse,
    version: i16,
    flex: bool,
) -> Result<(), ProtocolError> {
    if (0..=12).contains(&version) {
        if flex {
            put_compact_string(buf, &topic.topic);
        } else {
            put_string(buf, &topic.topic);
        }
    }
    if version >= 13 {
        put_uuid(buf, topic.topic_id);
    }
    // partitions[]
    put_array_len(buf, topic.partitions.len(), flex);
    for partition in &topic.partitions {
        encode_partition(buf, ops, partition, version, flex)?;
    }
    if flex {
        // FetchableTopicResponse has no known tagged fields — only unknowns.
        let tagged = WriteTaggedFields::new();
        tagged.write(buf, &topic.unknown_tagged_fields);
    }
    Ok(())
}

/// Encode one `PartitionData`, emitting the records payload as a separate
/// `Records` op (the records *length prefix* stays in the inline `buf`).
/// Mirrors the generated `PartitionData::encode` field-for-field.
fn encode_partition(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    p: &PartitionData,
    version: i16,
    flex: bool,
) -> Result<(), ProtocolError> {
    put_i32(buf, p.partition_index);
    put_i16(buf, p.error_code);
    put_i64(buf, p.high_watermark);
    if version >= 4 {
        put_i64(buf, p.last_stable_offset);
    }
    if version >= 5 {
        put_i64(buf, p.log_start_offset);
    }
    if version >= 4 {
        let len = p.aborted_transactions.as_ref().map(Vec::len);
        put_nullable_array_len(buf, len, flex);
        if let Some(v) = &p.aborted_transactions {
            for it in v {
                crate::Encode::encode(it, buf, version)?;
            }
        }
    }
    if version >= 11 {
        put_i32(buf, p.preferred_read_replica);
    }
    // records: nullable. Write the length prefix into `buf`, then emit the
    // payload region as its own op (no copy of the records bytes).
    match &p.records {
        None => {
            if flex {
                // COMPACT_BYTES null == uvarint 0.
                crate::primitives::varint::put_uvarint(buf, 0);
            } else {
                put_i32(buf, -1);
            }
        }
        Some(payload) => {
            let payload_len = payload.payload_len();
            if flex {
                // COMPACT_BYTES: uvarint(len + 1) prefix.
                let prefixed = u32::try_from(payload_len + 1).map_err(|_| {
                    ProtocolError::InvalidValue("records too large for compact len")
                })?;
                crate::primitives::varint::put_uvarint(buf, prefixed);
            } else {
                let len = i32::try_from(payload_len)
                    .map_err(|_| ProtocolError::InvalidValue("records too large for i32 len"))?;
                put_i32(buf, len);
            }
            // Flush the accumulated inline bytes (which now end exactly at the
            // records length prefix), then emit the records region.
            flush_inline(buf, ops);
            ops.push(FetchWriteOp::Records(payload.clone()));
        }
    }
    if flex {
        // PartitionData tagged fields: diverging_epoch(0), current_leader(1),
        // snapshot_id(2). These serialize AFTER the records bytes — they land
        // in the next inline segment, which is correct.
        let mut tagged = WriteTaggedFields::new();
        if !crate::codegen_helpers::is_default(&p.diverging_epoch) {
            let payload = encode_to_bytes(
                crate::Encode::encoded_len(&p.diverging_epoch, version),
                |b| {
                    crate::Encode::encode(&p.diverging_epoch, b, version)?;
                    Ok(())
                },
            );
            tagged.add(0, payload);
        }
        if !crate::codegen_helpers::is_default(&p.current_leader) {
            let payload = encode_to_bytes(
                crate::Encode::encoded_len(&p.current_leader, version),
                |b| {
                    crate::Encode::encode(&p.current_leader, b, version)?;
                    Ok(())
                },
            );
            tagged.add(1, payload);
        }
        if !crate::codegen_helpers::is_default(&p.snapshot_id) {
            let payload =
                encode_to_bytes(crate::Encode::encoded_len(&p.snapshot_id, version), |b| {
                    crate::Encode::encode(&p.snapshot_id, b, version)?;
                    Ok(())
                });
            tagged.add(2, payload);
        }
        tagged.write(buf, &p.unknown_tagged_fields);
    }
    Ok(())
}

/// Flush the running inline accumulator as an `Inline` op (no-op if empty),
/// leaving `buf` empty and ready to accumulate the next segment. Uses
/// `split()` so the emitted `Bytes` shares the existing allocation and the
/// continued accumulation reuses the buffer's spare capacity.
#[inline]
fn flush_inline(buf: &mut BytesMut, ops: &mut Vec<FetchWriteOp>) {
    if !buf.is_empty() {
        ops.push(FetchWriteOp::Inline(buf.split().freeze()));
    }
}

#[cfg(test)]
mod tests {

    use bytes::BytesMut;

    use super::{
        super::{AbortedTransaction, FetchableTopicResponse, NodeEndpoint, PartitionData},
        *,
    };
    use crate::{Encode, records::RecordsPayload};

    /// Concatenate a write-plan's ops into one byte buffer (resolving
    /// `Records` ops via the payload's own `encode_to`, exactly as the broker
    /// fallback path would).
    fn concat_plan(ops: &[FetchWriteOp]) -> Vec<u8> {
        let mut out = BytesMut::new();
        for op in ops {
            match op {
                FetchWriteOp::Inline(b) => out.extend_from_slice(b),
                FetchWriteOp::Records(p) => p.encode_to(&mut out).unwrap(),
            }
        }
        out.to_vec()
    }

    fn raw_batch_bytes(base_offset: i64) -> Bytes {
        // Build a real v2 batch and take its verbatim bytes (this is what the
        // fetch pass-through stores in `RecordsPayload::Raw`).
        use crate::records::{Record, RecordBatch};
        let rb = RecordBatch {
            base_offset,
            records: vec![Record {
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"val")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        };
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        buf.freeze()
    }

    /// The load-bearing golden test: for a hand-built multi-topic,
    /// multi-partition, multi-batch response, the write-plan's concatenated
    /// bytes must equal the generated `encode` output, at every version.
    fn assert_plan_matches_encode(_case_name: &str, resp: &FetchResponse, version: i16) {
        let mut canonical = BytesMut::new();
        resp.encode(&mut canonical, version).unwrap();
        let plan = resp.write_plan(version).unwrap();
        let assembled = concat_plan(&plan);
        assert2::assert!(&assembled[..] == &canonical[..]);
        // The plan's total length must also equal encoded_len (the frame
        // length prefix the broker writes is derived from encoded_len).
        let plan_total: usize = plan.iter().map(FetchWriteOp::len).sum();
        assert2::assert!(plan_total == resp.encoded_len(version));
        assert2::assert!(plan_total == canonical.len());
    }

    fn multi_partition_response(version: i16) -> FetchResponse {
        // Two batches concatenated in p0's records (multi-batch), one batch in
        // p1, a null-records p2 (e.g. empty fetch), and a second topic with one
        // partition — exercises every records-field shape.
        let mut p0_records = BytesMut::new();
        p0_records.extend_from_slice(&raw_batch_bytes(0));
        p0_records.extend_from_slice(&raw_batch_bytes(1));
        let p0 = PartitionData {
            partition_index: 0,
            high_watermark: 2,
            last_stable_offset: 2,
            log_start_offset: 0,
            records: Some(RecordsPayload::Raw(p0_records.freeze())),
            ..PartitionData::default()
        };
        let p1 = PartitionData {
            partition_index: 1,
            high_watermark: 1,
            last_stable_offset: 1,
            log_start_offset: 0,
            // read_committed: Some(empty) aborted list to exercise the array
            // path on v4+.
            aborted_transactions: if version >= 4 {
                Some(vec![AbortedTransaction {
                    producer_id: 7,
                    first_offset: 0,
                    ..AbortedTransaction::default()
                }])
            } else {
                None
            },
            records: Some(RecordsPayload::Raw(raw_batch_bytes(0))),
            ..PartitionData::default()
        };
        let p2 = PartitionData {
            partition_index: 2,
            error_code: 0,
            high_watermark: 0,
            records: None,
            ..PartitionData::default()
        };
        let topic0 = FetchableTopicResponse {
            topic: if version <= 12 {
                "topic-a".to_string()
            } else {
                String::new()
            },
            topic_id: if version >= 13 {
                crate::primitives::uuid::Uuid([9u8; 16])
            } else {
                crate::primitives::uuid::Uuid([0u8; 16])
            },
            partitions: vec![p0, p1, p2],
            ..FetchableTopicResponse::default()
        };
        let topic1 = FetchableTopicResponse {
            topic: if version <= 12 {
                "topic-b".to_string()
            } else {
                String::new()
            },
            topic_id: if version >= 13 {
                crate::primitives::uuid::Uuid([3u8; 16])
            } else {
                crate::primitives::uuid::Uuid([0u8; 16])
            },
            partitions: vec![PartitionData {
                partition_index: 0,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                records: Some(RecordsPayload::Raw(raw_batch_bytes(5))),
                ..PartitionData::default()
            }],
            ..FetchableTopicResponse::default()
        };
        FetchResponse {
            throttle_time_ms: 3,
            error_code: 0,
            session_id: 42,
            responses: vec![topic0, topic1],
            node_endpoints: if version >= 16 {
                vec![NodeEndpoint {
                    node_id: 1,
                    host: "h".to_string(),
                    port: 9092,
                    rack: None,
                    ..NodeEndpoint::default()
                }]
            } else {
                Vec::new()
            },
            ..FetchResponse::default()
        }
    }

    #[test]
    fn fetch_response_plan_cases_match_encode_all_versions() {
        // Cover non-flexible (4..=11, i32 prefix) and flexible (12..=18,
        // uvarint prefix + tagged buffers) — the canonical codec range.
        type TestCase1<'a> = (&'a str, fn(i16) -> FetchResponse);
        let cases: [TestCase1<'_>; 4] = [
            ("multi_partition", multi_partition_response),
            ("populated", FetchResponse::populated),
            ("default", |_| FetchResponse::default()),
            ("empty_responses", |_| FetchResponse {
                throttle_time_ms: 0,
                session_id: 0,
                responses: Vec::new(),
                ..FetchResponse::default()
            }),
        ];

        for (case_name, make_response) in cases {
            for version in 4..=18 {
                let response = make_response(version);
                assert_plan_matches_encode(case_name, &response, version);
            }
        }
    }

    #[test]
    fn truncated_trailing_batch_rides_through_verbatim() {
        // Kafka returns a partition's records ending in a partially-truncated
        // final batch when the byte budget cuts mid-batch. `RecordsPayload::Raw`
        // is emitted verbatim by the plan, so the clipped bytes must survive
        // byte-for-byte and match `encode`.
        for version in [4i16, 11, 12, 18] {
            let mut records = BytesMut::new();
            records.extend_from_slice(&raw_batch_bytes(0));
            records.extend_from_slice(&raw_batch_bytes(1));
            // 9 bytes of a partial trailing batch header.
            records.extend_from_slice(&[0u8; 9]);
            let resp = FetchResponse {
                throttle_time_ms: 0,
                session_id: 1,
                responses: vec![FetchableTopicResponse {
                    topic: if version <= 12 {
                        "t".to_string()
                    } else {
                        String::new()
                    },
                    topic_id: if version >= 13 {
                        crate::primitives::uuid::Uuid([1u8; 16])
                    } else {
                        crate::primitives::uuid::Uuid([0u8; 16])
                    },
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        high_watermark: 2,
                        last_stable_offset: 2,
                        log_start_offset: 0,
                        records: Some(RecordsPayload::Raw(records.freeze())),
                        ..PartitionData::default()
                    }],
                    ..FetchableTopicResponse::default()
                }],
                ..FetchResponse::default()
            };
            assert_plan_matches_encode("truncated_trailing_batch", &resp, version);
        }
    }
}
