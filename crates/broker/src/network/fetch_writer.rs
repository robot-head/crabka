//! Zero-copy(-ish) fetch response writer (Increment C).
//!
//! The generic dispatch loop writes every response via
//! `Framed<S, LengthDelimitedCodec>::send`, which copies the whole body into
//! the codec's write buffer (and the body itself was already copied once by
//! `encode_response` to prepend the correlation header). For a 100 KB+ fetch
//! that is hundreds of KB of avoidable `memcpy` per request.
//!
//! This module replaces that path **for Fetch responses only** with an ordered
//! [`WriteOp`] plan: the response header + envelope metadata are written inline
//! from userspace, and each partition's records region is handed to the socket
//! as its own segment via a vectored `write_all` — without copying the records
//! bytes through the codec. (Increment D adds a `WriteOp::File` variant drained
//! by the kernel `sendfile(2)` zero-copy path on Linux plaintext connections.)
//!
//! ## Framing
//!
//! Kafka frames every response with a 4-byte big-endian length prefix. The
//! length is **not** part of any records bytes, so the writer computes it up
//! front from the exact body length (`correlation header + Σ op lengths`) and
//! writes it from userspace before draining the ops. The body length is known
//! exactly without materializing the body: the records ops carry their own
//! `payload_len`, and the inline ops are already-built `Bytes`.

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crabka_protocol::owned::fetch_response::{FetchResponse, FetchWriteOp};
use crabka_protocol::records::RecordsPayload;

use crate::error::BrokerError;
use crate::network::codec::MAX_FRAME_BYTES;

/// One ordered segment of the fetch response wire frame.
#[derive(Debug)]
pub enum WriteOp {
    /// Userspace bytes: the length prefix + correlation header, partition
    /// metadata, records length prefixes, tagged-field trailers, and — on the
    /// vectored (Increment C) path — the resolved records bytes.
    Inline(Bytes),
}

impl WriteOp {
    /// Byte length this op contributes to the frame body. Used by the
    /// frame-length accounting (Increment D's size threshold + tests).
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        match self {
            Self::Inline(b) => b.len(),
        }
    }
}

/// Build the ordered [`WriteOp`] plan for a v4+ fetch response, including the
/// leading frame-length prefix + correlation header as the first inline op.
///
/// Byte-exactness: the concatenation of every op's bytes equals exactly what
/// `encode_response(api_key=1, correlation_id, body_flexible,
/// &encode_fetch_response(resp))` would produce, because:
///   * the frame length == 4-byte big-endian `u32` of `(header_len + body_len)`,
///   * the header == `correlation_id` (+ an empty tagged byte iff `body_flexible`),
///   * the body ops come straight from [`FetchResponse::write_plan`], whose
///     concatenation is byte-identical to `FetchResponse::encode` (proven by
///     the protocol-crate golden tests).
///
/// `resolve_records` decides how each records segment is emitted — for the
/// portable C path see [`resolve_records_inline`]; Increment D supplies a
/// resolver that emits `WriteOp::File` on Linux plaintext.
pub fn build_fetch_plan<F>(
    resp: &FetchResponse,
    version: i16,
    correlation_id: i32,
    body_flexible: bool,
    mut resolve_records: F,
) -> Result<Vec<WriteOp>, BrokerError>
where
    F: FnMut(&RecordsPayload) -> Result<Vec<WriteOp>, BrokerError>,
{
    debug_assert!(
        version >= 4,
        "build_fetch_plan requires the canonical v4+ codec"
    );
    // The response header is v1 (a trailing empty tagged-fields byte) iff the
    // body is flexible. The `encode_response` exception for ApiVersions
    // (api_key 18) never applies here — this is always Fetch (api_key 1).
    let header_v1 = body_flexible;
    let header_len = if header_v1 { 5 } else { 4 };

    let proto_plan = resp.write_plan(version)?;
    let body_len: usize = proto_plan.iter().map(FetchWriteOp::len).sum();
    let frame_body_len = header_len + body_len;
    if frame_body_len >= MAX_FRAME_BYTES {
        return Err(BrokerError::Io(std::io::Error::other(
            "fetch response exceeds max frame size",
        )));
    }

    let mut ops: Vec<WriteOp> = Vec::with_capacity(proto_plan.len() + 1);

    // First inline op: 4-byte frame length + correlation header.
    let mut head = BytesMut::with_capacity(4 + header_len);
    head.put_u32(u32::try_from(frame_body_len).expect("checked < MAX_FRAME_BYTES"));
    head.put_i32(correlation_id);
    if header_v1 {
        head.put_u8(0); // empty response-header tagged fields
    }
    ops.push(WriteOp::Inline(head.freeze()));

    for op in proto_plan {
        match op {
            FetchWriteOp::Inline(b) => ops.push(WriteOp::Inline(b)),
            FetchWriteOp::Records(payload) => {
                ops.extend(resolve_records(&payload)?);
            }
        }
    }
    Ok(ops)
}

/// Portable (Increment C) records resolver: emit the records payload as a
/// single inline segment. For `RecordsPayload::Raw` this hands the verbatim
/// `.log` `Bytes` to the socket directly (a refcounted view — no copy of the
/// records bytes). For parsed/legacy payloads it encodes them into a fresh
/// buffer (the rare non-passthrough path).
pub fn resolve_records_inline(payload: &RecordsPayload) -> Result<Vec<WriteOp>, BrokerError> {
    let bytes = match payload {
        // `Raw`/`Legacy` are already verbatim wire bytes — share the `Bytes`.
        RecordsPayload::Raw(b) | RecordsPayload::Legacy(b) => b.clone(),
        // Parsed batches must be encoded; rare on the fetch path.
        RecordsPayload::V2(_) => {
            let mut buf = BytesMut::with_capacity(payload.payload_len());
            payload
                .encode_to(&mut buf)
                .map_err(|e| BrokerError::Io(std::io::Error::other(e.to_string())))?;
            buf.freeze()
        }
    };
    Ok(vec![WriteOp::Inline(bytes)])
}

/// Drain a fetch write-plan to `stream`, writing each op in order via
/// `write_all`, then flush.
///
/// The caller MUST have flushed any pending `Framed` codec output first so the
/// bytes do not interleave with the codec's write buffer.
pub async fn write_fetch_plan<S>(stream: &mut S, ops: Vec<WriteOp>) -> Result<(), BrokerError>
where
    S: AsyncWrite + Unpin,
{
    for op in ops {
        match op {
            WriteOp::Inline(b) => {
                stream.write_all(&b).await.map_err(BrokerError::Io)?;
            }
        }
    }
    stream.flush().await.map_err(BrokerError::Io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::Encode;
    use crabka_protocol::owned::fetch_response::{FetchableTopicResponse, PartitionData};
    use crabka_protocol::records::{Record, RecordBatch, RecordsPayload};

    fn raw_batch(base: i64) -> Bytes {
        let rb = RecordBatch {
            base_offset: base,
            records: vec![Record {
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"value-payload")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        };
        let mut buf = BytesMut::new();
        rb.encode(&mut buf).unwrap();
        buf.freeze()
    }

    fn sample_response(version: i16) -> FetchResponse {
        let p0 = PartitionData {
            partition_index: 0,
            high_watermark: 1,
            last_stable_offset: 1,
            log_start_offset: 0,
            records: Some(RecordsPayload::Raw(raw_batch(0))),
            ..PartitionData::default()
        };
        let p1 = PartitionData {
            partition_index: 1,
            high_watermark: 2,
            last_stable_offset: 2,
            log_start_offset: 0,
            records: Some(RecordsPayload::Raw(raw_batch(1))),
            ..PartitionData::default()
        };
        FetchResponse {
            throttle_time_ms: 0,
            session_id: 7,
            responses: vec![FetchableTopicResponse {
                topic: if version <= 12 {
                    "t".to_string()
                } else {
                    String::new()
                },
                topic_id: if version >= 13 {
                    crabka_protocol::primitives::uuid::Uuid([5u8; 16])
                } else {
                    crabka_protocol::primitives::uuid::Uuid([0u8; 16])
                },
                partitions: vec![p0, p1],
                ..FetchableTopicResponse::default()
            }],
            ..FetchResponse::default()
        }
    }

    /// The broker-level golden test: the full framed bytes produced by
    /// `build_fetch_plan` (length prefix + correlation header + body) must
    /// equal the bytes the old `encode_response(encode_fetch_response(..))`
    /// path produced, for both non-flexible and flexible versions.
    #[test]
    fn build_fetch_plan_matches_legacy_encode_path() {
        for version in [4i16, 7, 11, 12, 13, 16, 18] {
            let resp = sample_response(version);
            let correlation_id = 0x1234_5678;
            let body_flexible = version >= 12;

            // New path: assemble the plan bytes.
            let ops = build_fetch_plan(
                &resp,
                version,
                correlation_id,
                body_flexible,
                resolve_records_inline,
            )
            .unwrap();
            let mut new_bytes = BytesMut::new();
            for op in &ops {
                match op {
                    WriteOp::Inline(b) => new_bytes.extend_from_slice(b),
                }
            }

            // Old path: encode the body, then the response header, then frame.
            let mut body = BytesMut::new();
            resp.encode(&mut body, version).unwrap();
            let header_v1 = body_flexible; // Fetch is never ApiVersions
            let header_len = if header_v1 { 5 } else { 4 };
            let frame_body_len = header_len + body.len();
            let mut old_bytes = BytesMut::new();
            old_bytes.put_u32(u32::try_from(frame_body_len).unwrap());
            old_bytes.put_i32(correlation_id);
            if header_v1 {
                old_bytes.put_u8(0);
            }
            old_bytes.extend_from_slice(&body);

            assert_eq!(
                &new_bytes[..],
                &old_bytes[..],
                "plan != legacy encode at version {version}"
            );
        }
    }

    #[test]
    fn plan_total_len_matches_frame_prefix() {
        // The 4-byte frame prefix the writer emits must equal the actual bytes
        // following it (header + body). Off-by-one here corrupts every frame.
        for version in [4i16, 12, 18] {
            let resp = sample_response(version);
            let ops =
                build_fetch_plan(&resp, version, 1, version >= 12, resolve_records_inline).unwrap();
            // First op is [u32 len][header]; the declared length must equal the
            // sum of the remaining bytes of op0 (the header) + all later ops.
            let WriteOp::Inline(ref head) = ops[0];
            let declared = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
            let header_after_len = head.len() - 4;
            let tail_len: usize = ops[1..].iter().map(WriteOp::len).sum();
            assert_eq!(declared, header_after_len + tail_len);
        }
    }
}
