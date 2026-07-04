// Bespoke tests for the borrowed GetReplicaLogInfo wrappers. GetReplicaLogInfo
// is a crabka-internal RPC (api_key 93) excluded from the JVM differential
// sweep, so its generated borrowed codecs need direct populated round-trip
// coverage here — the generated min/max wrapper tests only exercise default
// (empty) messages and skip the nested-element loops and nullable-string path.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::{
    DecodeBorrow, Encode,
    borrowed::{
        get_replica_log_info_request::{
            GetReplicaLogInfoRequest, MAX_VERSION, MIN_VERSION, TopicPartitions,
        },
        get_replica_log_info_response::{
            GetReplicaLogInfoResponse, PartitionLogInfo, TopicPartitionLogInfo,
        },
    },
    primitives::uuid::Uuid,
};

#[test]
fn borrowed_request_populated_roundtrip() {
    for v in [MIN_VERSION, MAX_VERSION] {
        let req = GetReplicaLogInfoRequest {
            broker_id: 7,
            topic_partitions: vec![TopicPartitions {
                topic_id: Uuid([1u8; 16]),
                partitions: vec![0, 3, 5],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, v).unwrap();
        assert!(req.encoded_len(v) == buf.len());
        let frozen = buf.freeze();
        let mut cur = &frozen[..];
        let decoded = GetReplicaLogInfoRequest::decode_borrow(&mut cur, v).unwrap();
        assert!(cur.is_empty(), "decoder left trailing bytes");
        assert!(decoded == req);
    }
}

#[test]
fn borrowed_response_populated_roundtrip() {
    let resp = GetReplicaLogInfoResponse {
        broker_epoch: 42,
        topic_partition_log_info_list: vec![TopicPartitionLogInfo {
            topic_id: Uuid([3u8; 16]),
            partition_log_info: vec![
                PartitionLogInfo {
                    partition: 0,
                    last_written_leader_epoch: 4,
                    current_leader_epoch: 5,
                    log_end_offset: 1000,
                    error_code: 0,
                    error_message: None,
                    ..Default::default()
                },
                PartitionLogInfo {
                    partition: 1,
                    last_written_leader_epoch: 2,
                    current_leader_epoch: 3,
                    log_end_offset: 0,
                    error_code: 9,
                    error_message: Some("not leader"),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let v = 0;
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, v).unwrap();
    assert!(resp.encoded_len(v) == buf.len());
    let frozen = buf.freeze();
    let mut cur = &frozen[..];
    let decoded = GetReplicaLogInfoResponse::decode_borrow(&mut cur, v).unwrap();
    assert!(cur.is_empty(), "decoder left trailing bytes");
    assert!(decoded == resp);
}
