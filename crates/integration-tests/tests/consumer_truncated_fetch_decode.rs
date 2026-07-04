//! Regression: a Fetch response with a truncated trailing record batch (as
//! Apache Kafka returns when a partition byte budget is hit) must decode the
//! complete batches and drop the fragment, rather than failing the whole
//! response decode and stalling the consumer.

use assert2::assert;
use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    records::{Record, RecordBatch, RecordsPayload},
};

fn batch(base_offset: i64, value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn fetch_response_with_truncated_trailing_batch_decodes_complete_batches() {
    // Build the records-field bytes by hand: one complete batch + a truncated
    // second batch (only part of its header).
    let mut field = BytesMut::new();
    batch(0, b"hello").encode(&mut field).unwrap();
    field.extend_from_slice(&[0u8; 9]); // partial trailing batch

    // Use version 12 (first flexible version) so the topic string field is
    // used rather than topic_id UUID (which is only used at version 13+).
    let version = 12;
    let resp = FetchResponse {
        responses: vec![FetchableTopicResponse {
            topic: "t".into(),
            partitions: vec![PartitionData {
                partition_index: 0,
                high_watermark: 2,
                records: Some(RecordsPayload::Raw(field.freeze())),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Encode at a modern Fetch version, then decode — the decode path is the
    // one the consumer uses, and must tolerate the truncated tail.
    let mut wire = BytesMut::new();
    resp.encode(&mut wire, version).unwrap();
    let mut cur: &[u8] = &wire;
    let decoded = FetchResponse::decode(&mut cur, version).expect("lenient decode");

    let part = &decoded.responses[0].partitions[0];
    let batches = part.records.as_ref().unwrap().as_v2().expect("v2");
    assert!(batches.len() == 1, "complete batch kept, fragment dropped");
    assert!(batches[0].base_offset == 0);
}
