//! Minimal single-partition `Fetch` helper over a raw [`Connection`].
//!
//! `crabka-client-consumer`'s group `Consumer` owns subscription-style
//! consumption; this helper is the manual building block for callers
//! (e.g. the tiered-storage metadata-log consumer) that drive their own
//! per-partition fetch loops with externally-owned offsets.

use bytes::Bytes;

use crate::connection::Connection;
use crate::error::ClientError;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// One record decoded from a single-partition fetch.
#[derive(Debug, Clone)]
pub struct FetchedRecord {
    /// Absolute offset within the partition.
    pub offset: i64,
    /// Record key, if any.
    pub key: Option<Bytes>,
    /// Record value, if any.
    pub value: Option<Bytes>,
}

/// Fetch up to `partition_max_bytes` from `(topic, partition)` starting
/// at `fetch_offset`, decoding every v2 `RecordBatch` into
/// [`FetchedRecord`]s.
///
/// Records are returned in offset order. An empty result means the
/// partition had nothing at/after `fetch_offset` within `max_wait_ms`.
/// Legacy (non-v2) message sets are skipped.
///
/// # Errors
///
/// Returns [`ClientError`] on transport / version-negotiation failure,
/// or [`ClientError::Server`] when the broker reports a non-zero
/// partition-level `error_code` (e.g. `OFFSET_OUT_OF_RANGE`,
/// `NOT_LEADER_OR_FOLLOWER`, `UNKNOWN_TOPIC_ID`) so the caller can react
/// instead of silently re-fetching the same offset forever.
pub async fn fetch_partition(
    conn: &Connection,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    partition_max_bytes: i32,
) -> Result<Vec<FetchedRecord>, ClientError> {
    // Default to READ_UNCOMMITTED (isolation_level = 0): every record visible.
    fetch_partition_with_isolation(
        conn,
        topic,
        topic_id,
        partition,
        fetch_offset,
        max_wait_ms,
        partition_max_bytes,
        0,
    )
    .await
}

/// Like [`fetch_partition`], but lets the caller set the Kafka
/// `Fetch.isolation_level` (`0` = `READ_UNCOMMITTED`, `1` = `READ_COMMITTED`).
///
/// `READ_COMMITTED` restricts the result to records below the last stable
/// offset and excludes records from aborted transactions — required for
/// exactly-once changelog restore so that aborted writes are not replayed.
///
/// # Errors
///
/// Same as [`fetch_partition`].
#[allow(clippy::too_many_arguments)]
pub async fn fetch_partition_with_isolation(
    conn: &Connection,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    partition_max_bytes: i32,
    isolation_level: i8,
) -> Result<Vec<FetchedRecord>, ClientError> {
    let resp = conn
        .send(FetchRequest {
            max_wait_ms,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            isolation_level,
            topics: vec![FetchTopic {
                topic: topic.to_string(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition,
                    fetch_offset,
                    partition_max_bytes,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await?;
    decode_fetch_response(&resp, partition)
}

/// Decode every v2 `RecordBatch` for `partition` in `resp` into
/// offset-ordered [`FetchedRecord`]s. Control batches and legacy
/// (non-v2) payloads are skipped. Socket-free so it is unit-testable
/// against a hand-built response.
///
/// A non-zero partition-level `error_code` is surfaced as
/// [`ClientError::Server`] rather than swallowed as an empty result,
/// which would otherwise make a fetch loop re-request the same offset
/// indefinitely.
fn decode_fetch_response(
    resp: &crabka_protocol::owned::fetch_response::FetchResponse,
    partition: i32,
) -> Result<Vec<FetchedRecord>, ClientError> {
    let mut out = Vec::new();
    for t in &resp.responses {
        for p in &t.partitions {
            if p.partition_index != partition {
                continue;
            }
            if p.error_code != 0 {
                return Err(ClientError::Server {
                    error_code: p.error_code,
                });
            }
            let Some(payload) = &p.records else { continue };
            let Some(batches) = payload.as_v2() else {
                continue;
            };
            for batch in batches {
                if batch.attributes.is_control_batch() {
                    continue;
                }
                for r in &batch.records {
                    out.push(FetchedRecord {
                        offset: batch.base_offset + i64::from(r.offset_delta),
                        key: r.key.clone(),
                        value: r.value.clone(),
                    });
                }
            }
        }
    }
    out.sort_by_key(|r| r.offset);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::fetch_response::{
        FetchResponse, FetchableTopicResponse, PartitionData,
    };
    use crabka_protocol::records::{Record, RecordBatch, RecordsPayload};

    fn batch_with(base_offset: i64, values: &[&[u8]]) -> RecordBatch {
        let records = values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(Bytes::copy_from_slice(v)),
                ..Default::default()
            })
            .collect();
        RecordBatch {
            base_offset,
            last_offset_delta: i32::try_from(values.len().saturating_sub(1)).unwrap(),
            records,
            ..Default::default()
        }
    }

    #[test]
    fn decode_yields_absolute_offsets_for_the_requested_partition() {
        // One batch starting at offset 5 on partition 0; a record on
        // partition 1 that must be ignored when decoding partition 0.
        let resp = FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "t".into(),
                partitions: vec![
                    PartitionData {
                        partition_index: 0,
                        high_watermark: 7,
                        records: Some(RecordsPayload::from(vec![batch_with(5, &[b"a", b"b"])])),
                        ..Default::default()
                    },
                    PartitionData {
                        partition_index: 1,
                        high_watermark: 1,
                        records: Some(RecordsPayload::from(vec![batch_with(0, &[b"z"])])),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let got = decode_fetch_response(&resp, 0).unwrap();
        assert!(got.len() == 2);
        assert!(got[0].offset == 5);
        assert!(got[0].value.as_deref() == Some(b"a".as_ref()));
        assert!(got[1].offset == 6);
        assert!(got[1].value.as_deref() == Some(b"b".as_ref()));
    }

    #[test]
    fn partition_error_code_surfaces_as_server_error() {
        // OFFSET_OUT_OF_RANGE (1) on the requested partition must become
        // an Err rather than an empty Vec, otherwise the caller would
        // re-fetch the same offset forever.
        let resp = FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "t".into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    error_code: 1,
                    high_watermark: 0,
                    records: None,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = decode_fetch_response(&resp, 0).unwrap_err();
        assert!(matches!(err, ClientError::Server { error_code: 1 }));
    }
}
