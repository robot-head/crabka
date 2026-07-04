// Bespoke tests for the owned GetReplicaLogInfo wrappers. GetReplicaLogInfo is
// a crabka-internal RPC (api_key 93) excluded from the JVM differential sweep,
// so its generated codecs need direct populated round-trip coverage here. The
// generated min/max wrapper tests only exercise default (empty) messages, which
// skip the nested-element encode/decode loops and the nullable-string path.

use assert2::assert;
use bytes::BytesMut;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        get_replica_log_info_request::{
            GetReplicaLogInfoRequest, MAX_VERSION, MIN_VERSION, TopicPartitions,
        },
        get_replica_log_info_response::{
            GetReplicaLogInfoResponse, PartitionLogInfo, TopicPartitionLogInfo,
        },
    },
    primitives::uuid::Uuid,
};

fn populated_request() -> GetReplicaLogInfoRequest {
    GetReplicaLogInfoRequest {
        broker_id: 7,
        topic_partitions: vec![
            TopicPartitions {
                topic_id: Uuid([1u8; 16]),
                partitions: vec![0, 3, 5],
                ..Default::default()
            },
            TopicPartitions {
                topic_id: Uuid([2u8; 16]),
                partitions: vec![],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn populated_response() -> GetReplicaLogInfoResponse {
    GetReplicaLogInfoResponse {
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
                    error_message: Some("not leader".to_string()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn owned_request_populated_roundtrip() {
    for v in [MIN_VERSION, MAX_VERSION] {
        let req = populated_request();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, v).unwrap();
        assert!(req.encoded_len(v) == buf.len());
        let mut cur = &buf[..];
        assert!(GetReplicaLogInfoRequest::decode(&mut cur, v).unwrap() == req);
        assert!(cur.is_empty(), "decoder left trailing bytes");
    }
}

#[test]
fn owned_response_populated_roundtrip() {
    let resp = populated_response();
    let v = 0;
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, v).unwrap();
    assert!(resp.encoded_len(v) == buf.len());
    let mut cur = &buf[..];
    let decoded = GetReplicaLogInfoResponse::decode(&mut cur, v).unwrap();
    assert!(decoded == resp);
    assert!(cur.is_empty(), "decoder left trailing bytes");
}
