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
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

/// The single live-IO dependency [`fetch_partition_with_isolation`] needs: send
/// a typed [`FetchRequest`] and get the decoded [`FetchResponse`] back.
///
/// Abstracting it behind a trait (mirroring `connect-postgres`'s `PgCatalog`
/// seam) keeps the request-construction (`build_fetch_request`) and
/// response-decode (`decode_fetch_response`) logic mockable without a socket:
/// the trait method returns the already-decoded response, so a `mockall` mock
/// drives every decode/offset/error-code decision under the crate's default
/// feature set. The only un-mockable part — the actual frame write / read on
/// the wire — stays in the [`Connection`] adapter.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait FetchTransport: Send + Sync {
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse, ClientError>;
}

#[async_trait::async_trait]
impl FetchTransport for Connection {
    // cargo-mutants: thin Connection adapter; logic lives in fetch_partition_on
    #[cfg_attr(test, mutants::skip)]
    async fn fetch(&self, req: FetchRequest) -> Result<FetchResponse, ClientError> {
        self.send(req).await
    }
}

/// One record decoded from a single-partition fetch.
#[derive(Debug, Clone)]
pub struct FetchedRecord {
    /// Absolute offset within the partition.
    pub offset: i64,
    /// Record key, if any.
    pub key: Option<Bytes>,
    /// Record value, if any.
    pub value: Option<Bytes>,
    /// Record timestamp (epoch millis): batch `base_timestamp` + per-record delta.
    pub timestamp: i64,
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
// cargo-mutants: thin wrapper over fetch_partition_on (tested core)
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(topic = %topic, partition, fetch_offset),
    err,
)]
pub async fn fetch_partition(
    conn: &Connection,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    partition_max_bytes: i32,
) -> Result<Vec<FetchedRecord>, ClientError> {
    fetch_partition_on(
        conn,
        topic,
        topic_id,
        partition,
        fetch_offset,
        max_wait_ms,
        partition_max_bytes,
    )
    .await
}

/// `FetchTransport`-generic body of [`fetch_partition`]; the public entry point
/// is a thin `Connection` adapter so the build/decode logic is unit-testable
/// against a `mockall` `FetchTransport`.
async fn fetch_partition_on<T: FetchTransport + ?Sized>(
    conn: &T,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    partition_max_bytes: i32,
) -> Result<Vec<FetchedRecord>, ClientError> {
    // Default to READ_UNCOMMITTED (isolation_level = 0): every record visible.
    fetch_partition_with_isolation_on(
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
// cargo-mutants: thin wrapper over fetch_partition_with_isolation_on (tested core)
#[allow(clippy::too_many_arguments)]
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(topic = %topic, partition, fetch_offset, isolation_level),
    err,
)]
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
    fetch_partition_with_isolation_on(
        conn,
        topic,
        topic_id,
        partition,
        fetch_offset,
        max_wait_ms,
        partition_max_bytes,
        isolation_level,
    )
    .await
}

/// `FetchTransport`-generic body of [`fetch_partition_with_isolation`]. Holds the
/// build-request → send → decode-response logic so it is killable against a
/// `mockall` `FetchTransport` without a live broker socket.
#[allow(clippy::too_many_arguments)]
async fn fetch_partition_with_isolation_on<T: FetchTransport + ?Sized>(
    conn: &T,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    partition_max_bytes: i32,
    isolation_level: i8,
) -> Result<Vec<FetchedRecord>, ClientError> {
    let resp = conn
        .fetch(build_fetch_request(
            topic,
            topic_id,
            partition,
            fetch_offset,
            max_wait_ms,
            partition_max_bytes,
            isolation_level,
        ))
        .await?;
    decode_fetch_response(&resp, partition)
}

#[allow(clippy::too_many_arguments)]
fn build_fetch_request(
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    fetch_offset: i64,
    max_wait_ms: i32,
    partition_max_bytes: i32,
    isolation_level: i8,
) -> FetchRequest {
    FetchRequest {
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
    }
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
                        timestamp: batch.base_timestamp + r.timestamp_delta,
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
                timestamp_delta: i64::try_from(i).unwrap() * 10,
                value: Some(Bytes::copy_from_slice(v)),
                ..Default::default()
            })
            .collect();
        RecordBatch {
            base_offset,
            base_timestamp: 1_000,
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
        assert!(got[0].timestamp == 1_000);
        assert!(got[0].value.as_deref() == Some(b"a".as_ref()));
        assert!(got[1].offset == 6);
        assert!(got[1].timestamp == 1_010);
        assert!(got[1].value.as_deref() == Some(b"b".as_ref()));
    }

    #[test]
    fn build_fetch_request_preserves_single_partition_settings() {
        let topic_id = WireUuid([7; 16]);
        let req = build_fetch_request("orders", topic_id, 3, 123, 250, 64 * 1024, 1);

        assert!(req.max_wait_ms == 250);
        assert!(req.min_bytes == 1);
        assert!(req.max_bytes == 50 * 1024 * 1024);
        assert!(req.isolation_level == 1);
        assert!(req.topics.len() == 1);
        let topic = &req.topics[0];
        assert!(topic.topic == "orders");
        assert!(topic.topic_id == topic_id);
        assert!(topic.partitions.len() == 1);
        let partition = &topic.partitions[0];
        assert!(partition.partition == 3);
        assert!(partition.fetch_offset == 123);
        assert!(partition.partition_max_bytes == 64 * 1024);
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

    // ── socket-free end-to-end drive via MockFetchTransport ──────────────────
    //
    // These exercise `fetch_partition_with_isolation_on` (and the public
    // `fetch_partition` adapter) end-to-end without a broker: the mock captures
    // the `FetchRequest` the wrapper builds and returns a hand-built
    // `FetchResponse`, so build-request + decode-response decisions are killable
    // under the crate's default feature set.

    /// The default `fetch_partition` path requests `isolation_level` 0 and decodes
    /// the returned batch into absolute, offset-ordered records.
    #[tokio::test]
    async fn fetch_partition_on_builds_request_and_decodes_response() {
        let topic_id = WireUuid([3; 16]);
        let mut transport = MockFetchTransport::new();
        transport
            .expect_fetch()
            .withf(move |req: &FetchRequest| {
                // build_fetch_request wiring must be preserved on the wire.
                req.isolation_level == 0
                    && req.min_bytes == 1
                    && req.max_bytes == 50 * 1024 * 1024
                    && req.topics.len() == 1
                    && req.topics[0].topic == "t"
                    && req.topics[0].topic_id == topic_id
                    && req.topics[0].partitions.len() == 1
                    && req.topics[0].partitions[0].partition == 2
                    && req.topics[0].partitions[0].fetch_offset == 5
                    && req.topics[0].partitions[0].partition_max_bytes == 4096
            })
            .returning(|_req| {
                Ok(FetchResponse {
                    responses: vec![FetchableTopicResponse {
                        topic: "t".into(),
                        partitions: vec![PartitionData {
                            partition_index: 2,
                            high_watermark: 7,
                            records: Some(RecordsPayload::from(vec![batch_with(5, &[b"a", b"b"])])),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
            });

        let got = super::fetch_partition_on(&transport, "t", topic_id, 2, 5, 250, 4096)
            .await
            .unwrap();
        assert!(got.len() == 2);
        assert!(got[0].offset == 5);
        assert!(got[0].value.as_deref() == Some(b"a".as_ref()));
        assert!(got[1].offset == 6);
    }

    /// A caller-set isolation level (`READ_COMMITTED` = 1) must reach the wire.
    #[tokio::test]
    async fn fetch_partition_with_isolation_on_forwards_isolation_level() {
        let topic_id = WireUuid([9; 16]);
        let mut transport = MockFetchTransport::new();
        transport
            .expect_fetch()
            .withf(|req: &FetchRequest| req.isolation_level == 1)
            .returning(|_req| Ok(FetchResponse::default()));

        let got =
            super::fetch_partition_with_isolation_on(&transport, "t", topic_id, 0, 0, 100, 1024, 1)
                .await
                .unwrap();
        assert!(got.is_empty());
    }

    /// A transport error from the seam propagates unchanged.
    #[tokio::test]
    async fn fetch_partition_on_propagates_transport_error() {
        let mut transport = MockFetchTransport::new();
        transport
            .expect_fetch()
            .returning(|_req| Err(ClientError::Disconnected));

        let err = super::fetch_partition_on(&transport, "t", WireUuid([0; 16]), 0, 0, 100, 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::Disconnected));
    }

    /// A partition-level error code surfaces as `ClientError::Server` through the
    /// full send→decode path (not just the isolated decode helper).
    #[tokio::test]
    async fn fetch_partition_on_surfaces_partition_error_code() {
        let mut transport = MockFetchTransport::new();
        transport.expect_fetch().returning(|_req| {
            Ok(FetchResponse {
                responses: vec![FetchableTopicResponse {
                    topic: "t".into(),
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        error_code: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
        });

        let err = super::fetch_partition_on(&transport, "t", WireUuid([0; 16]), 0, 0, 100, 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::Server { error_code: 1 }));
    }
}
