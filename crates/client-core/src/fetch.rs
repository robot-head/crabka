//! Minimal single-partition `Fetch` helper over a raw [`Connection`].
//!
//! `crabka-client-consumer`'s group `Consumer` owns subscription-style
//! consumption; this helper is the manual building block for callers
//! (e.g. the tiered-storage metadata-log consumer) that drive their own
//! per-partition fetch loops with externally-owned offsets.

use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use crabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordHeader,
};

use crate::{connection::Connection, error::ClientError};

/// Default whole-response byte limit for single-partition fetch helpers.
pub const DEFAULT_FETCH_RESPONSE_MAX_BYTES: i32 = 50 * 1024 * 1024;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
    /// Absolute offset within the partition.
    pub offset: i64,
    /// Record key, if any.
    pub key: Option<Bytes>,
    /// Record value, if any.
    pub value: Option<Bytes>,
    /// Record timestamp (epoch millis): batch `base_timestamp` + per-record delta.
    pub timestamp: i64,
    /// Record headers in Kafka wire order, preserving duplicate keys and null values.
    pub headers: Vec<FetchedHeader>,
}

/// Parameters for a single-partition fetch with an explicit isolation level.
#[derive(Debug, Clone, Copy)]
pub struct IsolatedFetch<'a> {
    pub topic: &'a str,
    pub topic_id: WireUuid,
    pub partition: i32,
    pub fetch_offset: i64,
    pub max_wait_ms: i32,
    pub max_bytes: i32,
    pub partition_max_bytes: i32,
    pub isolation_level: i8,
}

/// Records returned by a fetch together with the next safe partition offset.
///
/// `next_offset` advances over every decoded batch, including control batches
/// and batches filtered because their transaction aborted. Callers which own a
/// fetch cursor must use it rather than deriving progress from `records`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResult {
    /// Visible records in offset order.
    pub records: Vec<FetchedRecord>,
    /// One past the highest decoded batch offset, if the response had v2 batches.
    pub next_offset: Option<i64>,
}

/// One Kafka record header decoded from a fetched record.
pub type FetchedHeader = RecordHeader;

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
        IsolatedFetch {
            topic,
            topic_id,
            partition,
            fetch_offset,
            max_wait_ms,
            max_bytes: DEFAULT_FETCH_RESPONSE_MAX_BYTES,
            partition_max_bytes,
            isolation_level: 0,
        },
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
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(topic = %fetch.topic, partition = fetch.partition, fetch_offset = fetch.fetch_offset, isolation_level = fetch.isolation_level),
    err,
)]
pub async fn fetch_partition_with_isolation(
    conn: &Connection,
    fetch: IsolatedFetch<'_>,
) -> Result<Vec<FetchedRecord>, ClientError> {
    fetch_partition_with_isolation_on(conn, fetch).await
}

/// Fetch a partition with isolation and return both visible records and cursor
/// progress across filtered transactional and control batches.
///
/// In `READ_COMMITTED` mode this applies the response's
/// `aborted_transactions` metadata client-side because Crabka brokers return
/// the underlying record batches verbatim.
///
/// # Errors
///
/// Returns [`ClientError`] when the fetch request fails or the broker response
/// is malformed.
pub async fn fetch_partition_with_isolation_progress(
    conn: &Connection,
    fetch: IsolatedFetch<'_>,
) -> Result<FetchPartitionResult, ClientError> {
    fetch_partition_with_isolation_progress_on(conn, fetch).await
}

/// `FetchTransport`-generic body of [`fetch_partition_with_isolation`]. Holds the
/// build-request → send → decode-response logic so it is killable against a
/// `mockall` `FetchTransport` without a live broker socket.
async fn fetch_partition_with_isolation_on<T: FetchTransport + ?Sized>(
    conn: &T,
    fetch: IsolatedFetch<'_>,
) -> Result<Vec<FetchedRecord>, ClientError> {
    let resp = conn.fetch(build_fetch_request(fetch)).await?;
    Ok(fetch_partition_with_isolation_progress_response(
        &resp,
        fetch.partition,
        fetch.fetch_offset,
        fetch.isolation_level,
    )?
    .records)
}

async fn fetch_partition_with_isolation_progress_on<T: FetchTransport + ?Sized>(
    conn: &T,
    fetch: IsolatedFetch<'_>,
) -> Result<FetchPartitionResult, ClientError> {
    let resp = conn.fetch(build_fetch_request(fetch)).await?;
    fetch_partition_with_isolation_progress_response(
        &resp,
        fetch.partition,
        fetch.fetch_offset,
        fetch.isolation_level,
    )
}

fn build_fetch_request(fetch: IsolatedFetch<'_>) -> FetchRequest {
    FetchRequest {
        max_wait_ms: fetch.max_wait_ms,
        min_bytes: 1,
        max_bytes: fetch.max_bytes,
        isolation_level: fetch.isolation_level,
        topics: vec![FetchTopic {
            topic: fetch.topic.to_string(),
            topic_id: fetch.topic_id,
            partitions: vec![FetchPartition {
                partition: fetch.partition,
                fetch_offset: fetch.fetch_offset,
                partition_max_bytes: fetch.partition_max_bytes,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Decode one partition response with client-side transaction filtering.
fn fetch_partition_with_isolation_progress_response(
    resp: &crabka_protocol::owned::fetch_response::FetchResponse,
    partition: i32,
    fetch_floor: i64,
    isolation_level: i8,
) -> Result<FetchPartitionResult, ClientError> {
    let mut out = Vec::new();
    let mut next_offset = None;
    let read_committed = isolation_level == 1;
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
            let mut aborted: Vec<(i64, i64)> = if read_committed {
                p.aborted_transactions
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|transaction| (transaction.first_offset, transaction.producer_id))
                    .collect()
            } else {
                Vec::new()
            };
            aborted.sort_unstable();
            let mut aborted: VecDeque<_> = aborted.into();
            let mut aborted_producers = HashSet::new();
            for batch in batches {
                next_offset = Some(
                    next_offset
                        .unwrap_or(fetch_floor)
                        .max(batch.base_offset + i64::from(batch.last_offset_delta) + 1),
                );
                while read_committed
                    && aborted
                        .front()
                        .is_some_and(|(first_offset, _)| *first_offset <= batch.base_offset)
                {
                    let Some((_, producer_id)) = aborted.pop_front() else {
                        break;
                    };
                    aborted_producers.insert(producer_id);
                }
                if batch.attributes.is_control_batch() {
                    if read_committed {
                        aborted_producers.remove(&batch.producer_id);
                    }
                    continue;
                }
                if read_committed
                    && batch.attributes.is_transactional()
                    && aborted_producers.contains(&batch.producer_id)
                {
                    continue;
                }
                for r in &batch.records {
                    let offset = batch.base_offset + i64::from(r.offset_delta);
                    if offset < fetch_floor {
                        continue;
                    }
                    out.push(FetchedRecord {
                        offset,
                        key: r.key.clone(),
                        value: r.value.clone(),
                        timestamp: batch.base_timestamp + r.timestamp_delta,
                        headers: r.headers.clone(),
                    });
                }
            }
        }
    }
    out.sort_by_key(|r| r.offset);
    Ok(FetchPartitionResult {
        records: out,
        next_offset,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BufMut as _;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            fetch_request::ReplicaState,
            fetch_response::{
                AbortedTransaction, FetchResponse, FetchableTopicResponse, PartitionData,
            },
        },
        records::{Attributes, Record, RecordBatch, RecordsPayload},
    };

    use super::*;

    fn put_signed_varint(buf: &mut Vec<u8>, value: i64) {
        let mut encoded = ((value << 1) ^ (value >> 63)).cast_unsigned();
        loop {
            let byte = (encoded & 0x7f) as u8;
            encoded >>= 7;
            buf.push(if encoded == 0 { byte } else { byte | 0x80 });
            if encoded == 0 {
                break;
            }
        }
    }

    fn put_hand_encoded_record(buf: &mut Vec<u8>, body: &[u8]) {
        put_signed_varint(buf, i64::try_from(body.len()).expect("record body fits"));
        buf.extend_from_slice(body);
    }

    /// Build bytes directly from the Kafka record-batch v2 grammar. This
    /// deliberately does not call either production encoder.
    fn hand_encoded_v2_header_batch() -> RecordBatch {
        let mut records = Vec::new();
        let mut empty = Vec::new();
        empty.push(0);
        put_signed_varint(&mut empty, 0);
        put_signed_varint(&mut empty, 0);
        put_signed_varint(&mut empty, -1);
        put_signed_varint(&mut empty, 5);
        empty.extend_from_slice(b"empty");
        put_signed_varint(&mut empty, 0);
        put_hand_encoded_record(&mut records, &empty);

        let long_key = "k".repeat(130);
        let long_value = vec![0xa5; 130];
        let mut headered = Vec::new();
        headered.push(0);
        put_signed_varint(&mut headered, 10);
        put_signed_varint(&mut headered, 1);
        put_signed_varint(&mut headered, -1);
        put_signed_varint(&mut headered, 4);
        headered.extend_from_slice(b"data");
        put_signed_varint(&mut headered, 3);
        put_signed_varint(&mut headered, 130);
        headered.extend_from_slice(long_key.as_bytes());
        put_signed_varint(&mut headered, 130);
        headered.extend_from_slice(&long_value);
        put_signed_varint(&mut headered, 3);
        headered.extend_from_slice(b"dup");
        put_signed_varint(&mut headered, -1);
        put_signed_varint(&mut headered, 3);
        headered.extend_from_slice(b"dup");
        put_signed_varint(&mut headered, 0);
        put_hand_encoded_record(&mut records, &headered);

        let mut batch = bytes::BytesMut::new();
        batch.put_i64(5);
        batch.put_i32(0);
        batch.put_i32(0);
        batch.put_i8(2);
        batch.put_u32(0);
        batch.put_i16(0);
        batch.put_i32(1);
        batch.put_i64(1_000);
        batch.put_i64(1_010);
        batch.put_i64(-1);
        batch.put_i16(-1);
        batch.put_i32(-1);
        batch.put_i32(2);
        batch.extend_from_slice(&records);
        let batch_len = i32::try_from(batch.len() - 12).expect("batch length fits");
        batch[8..12].copy_from_slice(&batch_len.to_be_bytes());
        let crc = crc32c::crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        RecordBatch::decode(&mut &batch[..]).expect("hand-encoded v2 batch decodes")
    }

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
        let got = fetch_partition_with_isolation_progress_response(&resp, 0, 0, 0)
            .unwrap()
            .records;
        assert!(
            got == vec![
                FetchedRecord {
                    offset: 5,
                    key: None,
                    value: Some(Bytes::from_static(b"a")),
                    timestamp: 1_000,
                    headers: Vec::new(),
                },
                FetchedRecord {
                    offset: 6,
                    key: None,
                    value: Some(Bytes::from_static(b"b")),
                    timestamp: 1_010,
                    headers: Vec::new(),
                },
            ]
        );
    }

    #[test]
    fn decode_preserves_hand_encoded_v2_header_wire_order_and_varints() {
        let response = FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "headers".into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    records: Some(RecordsPayload::from(vec![hand_encoded_v2_header_batch()])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let records = fetch_partition_with_isolation_progress_response(&response, 0, 0, 0)
            .expect("fetch response")
            .records;
        assert_eq!(
            records,
            vec![
                FetchedRecord {
                    offset: 5,
                    key: None,
                    value: Some(Bytes::from_static(b"empty")),
                    timestamp: 1_000,
                    headers: Vec::new(),
                },
                FetchedRecord {
                    offset: 6,
                    key: None,
                    value: Some(Bytes::from_static(b"data")),
                    timestamp: 1_010,
                    headers: vec![
                        FetchedHeader {
                            key: "k".repeat(130),
                            value: Some(Bytes::from(vec![0xa5; 130])),
                        },
                        FetchedHeader {
                            key: "dup".to_string(),
                            value: None,
                        },
                        FetchedHeader {
                            key: "dup".to_string(),
                            value: Some(Bytes::new()),
                        },
                    ],
                },
            ]
        );
    }

    #[test]
    fn build_fetch_request_preserves_single_partition_settings() {
        let topic_id = WireUuid([7; 16]);
        let req = build_fetch_request(IsolatedFetch {
            topic: "orders",
            topic_id,
            partition: 3,
            fetch_offset: 123,
            max_wait_ms: 250,
            max_bytes: 96 * 1024,
            partition_max_bytes: 64 * 1024,
            isolation_level: 1,
        });

        assert!(
            req == FetchRequest {
                replica_id: -1,
                max_wait_ms: 250,
                min_bytes: 1,
                max_bytes: 96 * 1024,
                isolation_level: 1,
                session_id: 0,
                session_epoch: -1,
                topics: vec![FetchTopic {
                    topic: "orders".to_string(),
                    topic_id: WireUuid([7; 16]),
                    partitions: vec![FetchPartition {
                        partition: 3,
                        current_leader_epoch: -1,
                        fetch_offset: 123,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: 64 * 1024,
                        replica_directory_id: WireUuid::ZERO,
                        high_watermark: i64::MAX,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                forgotten_topics_data: vec![],
                rack_id: String::new(),
                cluster_id: None,
                replica_state: ReplicaState {
                    replica_id: -1,
                    replica_epoch: -1,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                },
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }
        );
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
        let err = fetch_partition_with_isolation_progress_response(&resp, 0, 0, 0).unwrap_err();
        assert!(matches!(err, ClientError::Server { error_code: 1 }));
    }

    #[test]
    fn read_committed_filters_aborted_batches_and_advances_past_them() {
        let mut aborted_batch = batch_with(5, &[b"discard"]);
        aborted_batch.producer_id = 9;
        aborted_batch.attributes = Attributes(Attributes::TRANSACTIONAL_BIT);
        let visible_batch = batch_with(6, &[b"keep"]);
        let response = FetchResponse {
            responses: vec![FetchableTopicResponse {
                topic: "t".into(),
                partitions: vec![PartitionData {
                    partition_index: 0,
                    aborted_transactions: Some(vec![AbortedTransaction {
                        producer_id: 9,
                        first_offset: 5,
                        ..Default::default()
                    }]),
                    records: Some(RecordsPayload::from(vec![aborted_batch, visible_batch])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let result = fetch_partition_with_isolation_progress_response(&response, 0, 5, 1)
            .expect("successful fetch response");

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].offset, 6);
        assert_eq!(result.records[0].value, Some(Bytes::from_static(b"keep")));
        assert_eq!(result.next_offset, Some(7));
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
        assert!(
            got == vec![
                FetchedRecord {
                    offset: 5,
                    key: None,
                    value: Some(Bytes::from_static(b"a")),
                    timestamp: 1_000,
                    headers: Vec::new(),
                },
                FetchedRecord {
                    offset: 6,
                    key: None,
                    value: Some(Bytes::from_static(b"b")),
                    timestamp: 1_010,
                    headers: Vec::new(),
                },
            ]
        );
    }

    /// A caller-set isolation level (`READ_COMMITTED` = 1) must reach the wire.
    #[tokio::test]
    async fn fetch_partition_with_isolation_on_forwards_isolation_level() {
        let topic_id = WireUuid([9; 16]);
        let mut transport = MockFetchTransport::new();
        transport
            .expect_fetch()
            .withf(|req: &FetchRequest| req.isolation_level == 1 && req.max_bytes == 2048)
            .returning(|_req| Ok(FetchResponse::default()));

        let got = super::fetch_partition_with_isolation_on(
            &transport,
            IsolatedFetch {
                topic: "t",
                topic_id,
                partition: 0,
                fetch_offset: 0,
                max_wait_ms: 100,
                max_bytes: 2048,
                partition_max_bytes: 1024,
                isolation_level: 1,
            },
        )
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
