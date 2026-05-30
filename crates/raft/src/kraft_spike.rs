//! THROWAWAY KIP-595 slice-0 de-risking spike.
//!
//! This module serves REAL KRaft `ApiVersions` (api_key 18) and `Fetch`
//! (api_key 1) on the Crabka controller listener, gated behind the
//! `kraft-spike` cargo feature. It runs PARALLEL to (does not replace) the
//! existing openraft RPC flow. Its only purpose is to let a JVM Kafka 4.0
//! broker observer perform its `ApiVersions` handshake, `Fetch` a hand-built
//! bootstrap metadata log from offset 0, and decode it without error.
//!
//! Decode-only: no registration, no election, no writes. None of this code
//! is intended to survive past the spike — once slices 1–3 land a real KRaft
//! quorum, delete this module and its feature gate.
//!
//! Wire facts captured against `apache/kafka:4.0.0` are recorded in
//! `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md`.

#![allow(dead_code)]

use bytes::{BufMut, Bytes, BytesMut};

use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::owned::fetch_request::FetchRequest;
use crabka_protocol::owned::fetch_response::{
    FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData,
};
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordsPayload};
use crabka_protocol::{Decode, Encode};

/// Observer's `Fetch` version (flexible; `ResponseHeader` v1).
const FETCH_REQ_VERSION: i16 = 17;
/// Observer's `ApiVersions` version (`ResponseHeader` v0 special-case — NO
/// tagged-fields byte).
const APIVERSIONS_REQ_VERSION: i16 = 4;
/// Well-known `__cluster_metadata` topic id `00000000-0000-0000-0000-000000000001`.
const CLUSTER_METADATA_TOPIC_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
/// `kafka:4.0.0` default `metadata.version`.
const METADATA_VERSION_LEVEL: i16 = 25;
/// Fresh single-voter cluster leader id.
const SPIKE_LEADER_ID: i32 = 1;
/// Fresh single-voter cluster leader epoch.
const SPIKE_LEADER_EPOCH: i32 = 1;

// --- Step B: bootstrap log batch ---------------------------------------

/// The records the spike serves at offset 0. A single non-control data
/// record carrying a `FeatureLevelRecord`-style value (`metadata.version`
/// level, as the captured JVM bootstrap log does). The exact byte-for-byte
/// JVM record framing is refined later during live iteration — for the spike
/// this only needs to be a structurally valid, non-empty record.
fn bootstrap_records() -> Vec<Record> {
    // Value: a placeholder feature-level payload. Non-empty so the batch has
    // body bytes and a meaningful CRC; the live-iteration step replaces this
    // with byte-exact JVM `FEATURE_LEVEL_RECORD` bytes.
    let mut value = BytesMut::with_capacity(2);
    value.put_i16(METADATA_VERSION_LEVEL);
    vec![Record {
        attributes: 0,
        timestamp_delta: 0,
        offset_delta: 0,
        key: None,
        value: Some(value.freeze()),
        headers: Vec::new(),
    }]
}

/// Build the bootstrap metadata log: a single structurally valid v2
/// `RecordBatch` at `base_offset` 0 with `partition_leader_epoch` =
/// [`SPIKE_LEADER_EPOCH`], magic 2, and a correct CRC-32C (computed by
/// `RecordBatch::encode`).
pub(crate) fn bootstrap_log_batch() -> Bytes {
    let batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: SPIKE_LEADER_EPOCH,
        attributes: Attributes::default(),
        last_offset_delta: 0,
        base_timestamp: 0,
        max_timestamp: 0,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: bootstrap_records(),
    };
    let mut buf = BytesMut::with_capacity(batch.encoded_len());
    batch
        .encode(&mut buf)
        .expect("bootstrap batch encodes (no compression, in-range lengths)");
    buf.freeze()
}

/// Number of records the bootstrap log contains (used as the Fetch
/// high-watermark / log-end-offset for offset 0).
fn bootstrap_record_count() -> i64 {
    bootstrap_records().len() as i64
}

// --- Step C: ApiVersions responder -------------------------------------

/// Fully-framed `ApiVersions` response: 4-byte length prefix +
/// `ResponseHeader` **v0** (correlation id only — NO tagged-fields byte, the
/// documented Kafka special-case) + `ApiVersionsResponse` body encoded at
/// `req_version`.
///
/// Advertises at least `(api_key=18, min=0, max=4)` (ApiVersions) and
/// `(api_key=1, min=4, max=17)` (Fetch) so the observer negotiates the
/// versions this spike serves.
pub(crate) fn api_versions_response_frame(correlation_id: i32, req_version: i16) -> Bytes {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                unknown_tagged_fields: Default::default(),
            },
            ApiVersion {
                api_key: 1,
                min_version: 4,
                max_version: 17,
                unknown_tagged_fields: Default::default(),
            },
        ],
        ..Default::default()
    };

    let mut body = BytesMut::with_capacity(resp.encoded_len(req_version));
    resp.encode(&mut body, req_version)
        .expect("api versions body encodes");

    // ResponseHeader v0: correlation id only, no tagged-fields byte.
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(correlation_id);
    frame.put_slice(&body);

    prepend_len(frame)
}

// --- Step D: Fetch responder -------------------------------------------

/// Build the single metadata partition for the Fetch response. At
/// `fetch_offset == 0` the bootstrap batch is returned with the high
/// watermark = record count. At any higher offset the records are omitted
/// (the observer has already consumed them) but the same high watermark is
/// reported.
fn metadata_partition(fetch_offset: i64) -> PartitionData {
    let hwm = bootstrap_record_count();
    let records = if fetch_offset == 0 {
        Some(RecordsPayload::Raw(bootstrap_log_batch()))
    } else {
        None
    };
    PartitionData {
        partition_index: 0,
        error_code: 0,
        high_watermark: hwm,
        last_stable_offset: hwm,
        log_start_offset: 0,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records,
        diverging_epoch: Default::default(),
        current_leader: LeaderIdAndEpoch {
            leader_id: SPIKE_LEADER_ID,
            leader_epoch: SPIKE_LEADER_EPOCH,
            unknown_tagged_fields: Default::default(),
        },
        snapshot_id: Default::default(),
        unknown_tagged_fields: Default::default(),
    }
}

/// Fully-framed `Fetch` response: 4-byte length prefix + `ResponseHeader`
/// **v1** for flexible Fetch (correlation id + empty tagged-fields byte
/// `0x00` when `req_version >= 12`) + `FetchResponse` body at `req_version`.
/// One topic (`topic_id` = [`CLUSTER_METADATA_TOPIC_ID`]) with one partition.
pub(crate) fn fetch_response_frame(
    correlation_id: i32,
    req_version: i16,
    fetch_offset: i64,
) -> Bytes {
    let resp = FetchResponse {
        throttle_time_ms: 0,
        error_code: 0,
        session_id: 0,
        responses: vec![FetchableTopicResponse {
            topic: String::new(),
            topic_id: crabka_protocol::primitives::uuid::Uuid(CLUSTER_METADATA_TOPIC_ID),
            partitions: vec![metadata_partition(fetch_offset)],
            unknown_tagged_fields: Default::default(),
        }],
        node_endpoints: Vec::new(),
        unknown_tagged_fields: Default::default(),
    };

    let mut body = BytesMut::with_capacity(resp.encoded_len(req_version));
    resp.encode(&mut body, req_version)
        .expect("fetch body encodes");

    // ResponseHeader v1 for flexible Fetch: correlation id + empty
    // tagged-fields byte (0x00). Fetch FLEXIBLE_MIN = v12.
    let flexible = req_version >= 12;
    let mut frame = BytesMut::with_capacity(4 + usize::from(flexible) + body.len());
    frame.put_i32(correlation_id);
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(&body);

    prepend_len(frame)
}

/// Decode a `FetchRequest` body at `version` and return the first
/// partition's `fetch_offset`, if present.
pub(crate) fn fetch_offset_from_request(body: &[u8], version: i16) -> Option<i64> {
    let mut cur = body;
    let req = FetchRequest::decode(&mut cur, version).ok()?;
    let topic = req.topics.first()?;
    let partition = topic.partitions.first()?;
    Some(partition.fetch_offset)
}

/// Prepend the 4-byte big-endian length prefix to a complete response frame.
fn prepend_len(frame: BytesMut) -> Bytes {
    let mut out = BytesMut::with_capacity(4 + frame.len());
    out.put_i32(i32::try_from(frame.len()).unwrap_or(i32::MAX));
    out.put_slice(&frame);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Buf;

    #[test]
    fn bootstrap_batch_is_structurally_valid_v2() {
        let bytes = bootstrap_log_batch();
        // magic byte sits at offset 16 (base_offset 8 + batch_length 4 +
        // partition_leader_epoch 4).
        assert!(bytes[16] == 2);
        // Re-decode the batch to prove the CRC and framing are valid.
        let mut cur: &[u8] = &bytes;
        let batch = RecordBatch::decode(&mut cur).expect("bootstrap batch re-decodes");
        assert!(batch.base_offset == 0);
        assert!(batch.partition_leader_epoch == SPIKE_LEADER_EPOCH);
        assert!(!batch.records.is_empty());
        assert!(batch.records[0].value.is_some());
        assert!(batch.records[0].key.is_none());
    }

    #[test]
    fn api_versions_frame_decodes_and_advertises_keys() {
        let frame = api_versions_response_frame(7, APIVERSIONS_REQ_VERSION);
        let mut cur: &[u8] = &frame;
        // Strip length prefix.
        let len = cur.get_i32();
        assert!(len as usize == cur.remaining());
        // ResponseHeader v0: correlation id only.
        let corr = cur.get_i32();
        assert!(corr == 7);
        let resp = ApiVersionsResponse::decode(&mut cur, APIVERSIONS_REQ_VERSION)
            .expect("api versions body decodes at v4");
        assert!(resp.error_code == 0);
        let keys: Vec<i16> = resp.api_keys.iter().map(|k| k.api_key).collect();
        assert!(keys.contains(&1));
        assert!(keys.contains(&18));
    }

    #[test]
    fn fetch_frame_at_offset_zero_carries_records_and_leader() {
        let frame = fetch_response_frame(9, FETCH_REQ_VERSION, 0);
        let mut cur: &[u8] = &frame;
        // Strip length prefix.
        let len = cur.get_i32();
        assert!(len as usize == cur.remaining());
        // ResponseHeader v1 (flexible): correlation id + empty tagged byte.
        let corr = cur.get_i32();
        assert!(corr == 9);
        let tagged = cur.get_u8();
        assert!(tagged == 0);
        let resp =
            FetchResponse::decode(&mut cur, FETCH_REQ_VERSION).expect("fetch body decodes at v17");
        assert!(resp.error_code == 0);
        let topic = &resp.responses[0];
        assert!(topic.topic_id.0 == CLUSTER_METADATA_TOPIC_ID);
        let partition = &topic.partitions[0];
        assert!(partition.error_code == 0);
        assert!(partition.records.is_some());
        assert!(partition.current_leader.leader_id == SPIKE_LEADER_ID);
        assert!(partition.current_leader.leader_epoch == SPIKE_LEADER_EPOCH);
    }

    #[test]
    fn fetch_frame_above_offset_zero_omits_records() {
        let frame = fetch_response_frame(10, FETCH_REQ_VERSION, 5);
        let mut cur: &[u8] = &frame;
        let _len = cur.get_i32();
        let _corr = cur.get_i32();
        let _tagged = cur.get_u8();
        let resp = FetchResponse::decode(&mut cur, FETCH_REQ_VERSION).expect("fetch body decodes");
        let partition = &resp.responses[0].partitions[0];
        assert!(partition.records.is_none());
        assert!(partition.high_watermark == bootstrap_record_count());
    }

    #[test]
    fn fetch_offset_round_trips_through_request_decode() {
        // Build a minimal FetchRequest with one topic + partition at a known
        // fetch_offset, encode it, then decode the offset back out.
        use crabka_protocol::owned::fetch_request::{FetchPartition, FetchTopic};
        let mut req = FetchRequest::default();
        let mut partition = FetchPartition::default();
        partition.fetch_offset = 42;
        let mut topic = FetchTopic::default();
        topic.partitions = vec![partition];
        req.topics = vec![topic];

        let mut buf = BytesMut::new();
        req.encode(&mut buf, FETCH_REQ_VERSION).unwrap();
        let got = fetch_offset_from_request(&buf, FETCH_REQ_VERSION);
        assert!(got == Some(42));
    }
}
