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

// Throwaway spike: explicit `Default::default()` initializers on the
// generated protocol structs read more clearly here than naming each type,
// and the constants are exercised only from tests.
#![allow(dead_code, clippy::default_trait_access, clippy::doc_markdown)]

use bytes::{BufMut, Bytes, BytesMut};

use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::owned::fetch_request::FetchRequest;
use crabka_protocol::owned::fetch_response::{
    FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData,
};
use crabka_protocol::records::RecordsPayload;
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

// --- Step B: bootstrap log bytes ---------------------------------------

/// Real `apache/kafka:4.0.0` metadata-log bytes — offsets 0..=5: the
/// `LEADER_CHANGE` control batch + the bootstrap feature-level transaction
/// (`metadata.version`=25, `group.version`=1, `transaction.version`=2) —
/// captured verbatim from a freshly-formatted JVM node (cut at a batch
/// boundary, 284 bytes).
///
/// Replaying the JVM's own bytes back to a JVM observer is the truest
/// decode-only test: it exercises the `Fetch`/`ApiVersions` WIRE end to end
/// without yet hand-encoding KRaft metadata records (that is slice 1's job).
/// See `docs/superpowers/specs/2026-05-30-kraft-wire-findings.md`.
const SPIKE_METADATA_LOG: &[u8] = include_bytes!("kraft_spike_metadata_log.bin");

/// Log-end offset of [`SPIKE_METADATA_LOG`] — one past the last record offset
/// (5). Reported as the Fetch high watermark so the observer treats the served
/// records as committed and stops fetching once it has caught up to offset 6.
const SPIKE_LOG_END_OFFSET: i64 = 6;

/// The metadata-log bytes the spike serves (the embedded JVM capture).
pub(crate) fn bootstrap_log_batch() -> Bytes {
    Bytes::from_static(SPIKE_METADATA_LOG)
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

    prepend_len(&frame)
}

// --- Step D: Fetch responder -------------------------------------------

/// Build the single metadata partition for the Fetch response. At
/// `fetch_offset == 0` the bootstrap batch is returned with the high
/// watermark = record count. At any higher offset the records are omitted
/// (the observer has already consumed them) but the same high watermark is
/// reported.
fn metadata_partition(fetch_offset: i64) -> PartitionData {
    let records = if fetch_offset == 0 {
        Some(RecordsPayload::Raw(bootstrap_log_batch()))
    } else {
        None
    };
    PartitionData {
        partition_index: 0,
        error_code: 0,
        high_watermark: SPIKE_LOG_END_OFFSET,
        last_stable_offset: SPIKE_LOG_END_OFFSET,
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

    prepend_len(&frame)
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
fn prepend_len(frame: &BytesMut) -> Bytes {
    let mut out = BytesMut::with_capacity(4 + frame.len());
    out.put_i32(i32::try_from(frame.len()).unwrap_or(i32::MAX));
    out.put_slice(frame);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Buf;

    #[test]
    fn embedded_metadata_log_is_a_valid_v2_batch() {
        use crabka_protocol::records::RecordBatch;
        let bytes = bootstrap_log_batch();
        // The embedded JVM capture is 284 bytes: a LEADER_CHANGE control batch
        // (offset 0) followed by the bootstrap feature-level transaction
        // (offsets 1..=5).
        assert!(bytes.len() == 284);
        // magic byte sits at offset 16 (base_offset 8 + batch_length 4 +
        // partition_leader_epoch 4).
        assert!(bytes[16] == 2);
        // Re-decode the first batch (the control LEADER_CHANGE batch) to prove
        // the CRC and framing are valid.
        let mut cur: &[u8] = &bytes;
        let batch = RecordBatch::decode(&mut cur).expect("first batch re-decodes");
        assert!(batch.base_offset == 0);
        assert!(batch.partition_leader_epoch == SPIKE_LEADER_EPOCH);
    }

    #[test]
    fn api_versions_frame_decodes_and_advertises_keys() {
        let frame = api_versions_response_frame(7, APIVERSIONS_REQ_VERSION);
        let mut cur: &[u8] = &frame;
        // Strip length prefix.
        let len = cur.get_i32();
        assert!(usize::try_from(len).unwrap() == cur.remaining());
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
        assert!(usize::try_from(len).unwrap() == cur.remaining());
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
        assert!(partition.high_watermark == SPIKE_LOG_END_OFFSET);
    }

    #[test]
    fn fetch_offset_round_trips_through_request_decode() {
        // Build a minimal FetchRequest with one topic + partition at a known
        // fetch_offset, encode it, then decode the offset back out.
        use crabka_protocol::owned::fetch_request::{FetchPartition, FetchTopic};
        let partition = FetchPartition {
            fetch_offset: 42,
            ..Default::default()
        };
        let topic = FetchTopic {
            partitions: vec![partition],
            ..Default::default()
        };
        let req = FetchRequest {
            topics: vec![topic],
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        req.encode(&mut buf, FETCH_REQ_VERSION).unwrap();
        let got = fetch_offset_from_request(&buf, FETCH_REQ_VERSION);
        assert!(got == Some(42));
    }
}
