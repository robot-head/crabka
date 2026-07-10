use assert2::{assert, check};
mod support;

use crabka_protocol::{
    owned::{
        api_versions_request::ApiVersionsRequest,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        find_coordinator_request::FindCoordinatorRequest,
        init_producer_id_request::InitProducerIdRequest,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        metadata_request::MetadataRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};

/// Build a single `RecordBatch` carrying `n` empty records with sequential
/// offset deltas.
fn one_record_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            ..Default::default()
        });
    }
    b
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "CreateTopics for {name}");
}

/// Resolve a topic's UUID via a Metadata round trip. Produce v ≥ 13 sends
/// only `topic_id` on the wire, so tests need this to drive the broker
/// with a non-zero UUID.
async fn topic_id_for(
    p: &support::InProcess,
    name: &str,
) -> crabka_protocol::primitives::uuid::Uuid {
    use crabka_protocol::owned::metadata_request::MetadataRequestTopic;
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

#[tokio::test]
async fn api_versions_round_trip() {
    let p = support::start().await;
    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert!(
        (
            resp.error_code,
            resp.api_keys.iter().any(|k| k.api_key == 18)
        ) == (0, true)
    );
    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_then_delete_topic_round_trip() {
    let p = support::start().await;

    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "alpha".into(),
            num_partitions: 2,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert!(
        (
            resp.topics.len(),
            resp.topics[0].error_code,
            resp.topics[0].num_partitions
        ) == (1, 0, 2)
    );

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("alpha".into()),
            ..Default::default()
        }],
        topic_names: vec!["alpha".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let dresp = p.client.send(delete).await.expect("DeleteTopics");
    assert!((dresp.responses.len(), dresp.responses[0].error_code) == (1, 0));

    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_topic_with_zero_partitions_errors() {
    let p = support::start().await;
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "zero".into(),
            num_partitions: 0,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert!(resp.topics[0].error_code == 37); // INVALID_PARTITIONS
    p.broker.shutdown().await;
}

#[tokio::test]
async fn duplicate_create_returns_topic_already_exists() {
    let p = support::start().await;
    let req = || CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "dup".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let r1 = p.client.send(req()).await.expect("CreateTopics 1");
    assert!(r1.topics[0].error_code == 0);
    let r2 = p.client.send(req()).await.expect("CreateTopics 2");
    assert!(r2.topics[0].error_code == 36); // TOPIC_ALREADY_EXISTS
    p.broker.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_this_broker_and_listed_topics() {
    let p = support::start().await;
    // Create a topic first.
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "beta".into(),
            num_partitions: 3,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let _ = p.client.send(create).await.unwrap();

    let resp = p
        .client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata");
    assert!(resp.brokers.len() == 1);
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("beta"))
        .unwrap();
    assert!(topic.partitions.len() == 3);
    for (i, part) in topic.partitions.iter().enumerate() {
        check!(
            (part.error_code, part.partition_index, part.leader_id)
                == (0, i32::try_from(i).unwrap(), 1)
        );
    }
    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_assigns_base_offsets() {
    let p = support::start().await;
    create_topic(&p, "prod", 1).await;
    let topic_id = topic_id_for(&p, "prod").await;

    // First produce: 3 records → base 0.
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "prod".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(3).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce 1");
    assert!(resp.responses.len() == 1);
    let first = &resp.responses[0].partition_responses;
    assert!((first.len(), first[0].error_code, first[0].base_offset) == (1, 0, 0));

    // Second produce: 2 records → base 3.
    let req2 = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "prod".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(2).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp2 = p.client.send(req2).await.expect("Produce 2");
    let second = &resp2.responses[0].partition_responses[0];
    assert!((second.error_code, second.base_offset) == (0, 3));

    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_to_unknown_topic_returns_3() {
    let p = support::start().await;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "nope".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(1).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce unknown");
    assert!(resp.responses[0].partition_responses[0].error_code == 3);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_then_fetch_round_trip() {
    let p = support::start().await;
    create_topic(&p, "round", 1).await;
    let topic_id = topic_id_for(&p, "round").await;

    let prod = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "round".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(3).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let presp = p.client.send(prod).await.expect("Produce");
    assert!(presp.responses[0].partition_responses[0].error_code == 0);

    let fetch = FetchRequest {
        max_wait_ms: 100,
        min_bytes: 1,
        topics: vec![FetchTopic {
            topic: "round".into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let fresp = p.client.send(fetch).await.expect("Fetch");
    assert!(fresp.responses.len() == 1);
    let part = &fresp.responses[0].partitions[0];
    assert!(part.error_code == 0);
    let batches = part
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records must be present after produce");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert!(total == 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn list_offsets_earliest_and_latest() {
    let p = support::start().await;
    create_topic(&p, "empty", 1).await;

    let mk = |ts: i64| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![ListOffsetsTopic {
            name: "empty".into(),
            partitions: vec![ListOffsetsPartition {
                partition_index: 0,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let earliest = p.client.send(mk(-2)).await.expect("ListOffsets earliest");
    let latest = p.client.send(mk(-1)).await.expect("ListOffsets latest");
    for (label, resp) in [("earliest", &earliest), ("latest", &latest)] {
        check!(
            (
                resp.topics[0].partitions[0].error_code,
                resp.topics[0].partitions[0].offset
            ) == (0, 0),
            "{label}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn find_coordinator_returns_self() {
    let p = support::start().await;
    let req = FindCoordinatorRequest {
        coordinator_keys: vec!["any-group".into()],
        ..Default::default()
    };
    let r = p.client.send(req).await.expect("FindCoordinator");
    for c in &r.coordinators {
        check!((c.error_code, c.node_id, c.host.is_empty(), c.port > 0) == (0, 1, false, true));
    }
    p.broker.shutdown().await;
}

#[tokio::test]
async fn join_group_with_empty_member_returns_member_id_required() {
    use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
    let p = support::start().await;
    let req = JoinGroupRequest {
        group_id: "g".into(),
        protocol_type: "consumer".into(),
        member_id: String::new(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 2_000,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: bytes::Bytes::from_static(b""),
            ..Default::default()
        }],
        ..Default::default()
    };
    let r = p.client.send(req).await.expect("JoinGroup");
    assert!((r.error_code, r.member_id.is_empty()) == (79, false)); // MEMBER_ID_REQUIRED
    p.broker.shutdown().await;
}

#[tokio::test]
async fn join_group_single_member_completes_after_deadline() {
    use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
    let p = support::start().await;
    // First call to obtain a server-assigned member_id.
    let r1 = p
        .client
        .send(JoinGroupRequest {
            group_id: "g".into(),
            protocol_type: "consumer".into(),
            member_id: String::new(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup1");
    // Retry with the assigned member_id. The handler will block ~1.5s
    // waiting for the rebalance deadline.
    let r2 = p
        .client
        .send(JoinGroupRequest {
            group_id: "g".into(),
            protocol_type: "consumer".into(),
            member_id: r1.member_id.clone(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup2");
    check!(
        (
            r2.error_code,
            &r2.leader,
            &r2.member_id,
            r2.members.is_empty(),
        ) == (0, &r1.member_id, &r1.member_id, false),
        "leader response must echo the member id and include the member list"
    );
    p.broker.shutdown().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_group_flow_join_sync_heartbeat_commit_fetch_leave() {
    use crabka_protocol::owned::{
        heartbeat_request::HeartbeatRequest,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        leave_group_request::LeaveGroupRequest,
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
    };

    let p = support::start().await;

    // KIP-516: OffsetCommit/OffsetFetch negotiate to v10/v8+, which key by
    // topic_id on the wire — so the topic must exist to carry a real UUID.
    create_topic(&p, "t", 1).await;
    let tid = topic_id_for(&p, "t").await;

    // Step 1: empty member_id → broker returns one.
    let r1 = p
        .client
        .send(JoinGroupRequest {
            group_id: "g".into(),
            protocol_type: "consumer".into(),
            member_id: String::new(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r1.error_code == 79);
    let mid = r1.member_id.clone();
    assert!(!mid.is_empty());

    // Step 2: re-join with assigned member_id → wait for rebalance, become leader.
    let r2 = p
        .client
        .send(JoinGroupRequest {
            group_id: "g".into(),
            protocol_type: "consumer".into(),
            member_id: mid.clone(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: bytes::Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!((r2.error_code, &r2.leader) == (0, &mid));
    let generation = r2.generation_id;

    // Step 3: leader SyncGroup with a single-member assignment.
    let r3 = p
        .client
        .send(SyncGroupRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: mid.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: mid.clone(),
                assignment: bytes::Bytes::from_static(b"asgn"),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!((r3.error_code, r3.assignment.as_ref()) == (0, b"asgn".as_slice()));

    // Step 4: Heartbeat → 0.
    let r4 = p
        .client
        .send(HeartbeatRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: mid.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r4.error_code == 0);

    // Step 5: OffsetCommit → 0.
    let r5 = p
        .client
        .send(OffsetCommitRequest {
            group_id: "g".into(),
            generation_id_or_member_epoch: generation,
            member_id: mid.clone(),
            topics: vec![OffsetCommitRequestTopic {
                name: "t".into(),
                topic_id: tid,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    committed_leader_epoch: 0,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r5.topics[0].partitions[0].error_code == 0);

    // Step 6: OffsetFetch → returns 42. v8+ uses the multi-group `groups[]`
    // shape, keyed by topic_id at v10.
    let r6 = p
        .client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "t".into(),
                    topic_id: tid,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r6.groups[0].topics[0].partitions[0].committed_offset == 42);

    // Step 7: LeaveGroup.
    let r7 = p
        .client
        .send(LeaveGroupRequest {
            group_id: "g".into(),
            member_id: mid.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r7.error_code == 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn init_producer_id_returns_fresh_pid() {
    let p = support::start().await;
    let r = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    check!((r.error_code, r.producer_id >= 1000, r.producer_epoch) == (0, true, 0));
    p.broker.shutdown().await;
}

#[tokio::test]
async fn init_producer_id_without_coordinator_bootstrap_returns_not_coordinator() {
    // Without a prior FindCoordinator(TRANSACTION) call, the broker has not
    // yet refreshed its leader_partitions set for __transaction_state, so it
    // cannot confirm it is the coordinator and returns NOT_COORDINATOR (16).
    let p = support::start().await;
    let r = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("tx-1".into()),
            ..Default::default()
        })
        .await
        .expect("InitProducerId");
    assert!(r.error_code == 16); // NOT_COORDINATOR
    p.broker.shutdown().await;
}

fn one_batch_with_producer(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    let n = i32::try_from(values.len()).expect("values.len fits i32");
    let mut records = Vec::with_capacity(values.len());
    for (i, v) in values.iter().enumerate() {
        records.push(Record {
            offset_delta: i32::try_from(i).expect("index fits i32"),
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    RecordBatch {
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: base_seq,
        last_offset_delta: n - 1,
        max_timestamp: i64::from(n),
        records,
        ..Default::default()
    }
}

#[tokio::test]
async fn idempotent_produce_dedups_duplicate_batch() {
    let p = support::start().await;

    create_topic(&p, "idem", 1).await;
    let idem_id = topic_id_for(&p, "idem").await;

    let init = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    let pid = init.producer_id;

    let req = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "idem".into(),
            topic_id: idem_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_batch_with_producer(pid, 0, 0, &["a", "b", "c"]).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let r1 = p.client.send(req.clone()).await.expect("Produce 1");
    let first = &r1.responses[0].partition_responses[0];
    assert!((first.error_code, first.base_offset) == (0, 0));

    // Send the same batch again — must be deduped (error 0, base_offset 0).
    let r2 = p.client.send(req).await.expect("Produce 2 (dup)");
    let duplicate = &r2.responses[0].partition_responses[0];
    assert!((duplicate.error_code, duplicate.base_offset) == (0, 0));

    p.broker.shutdown().await;
}

#[tokio::test]
async fn out_of_order_returns_45() {
    let p = support::start().await;

    create_topic(&p, "ooo", 1).await;
    let ooo_id = topic_id_for(&p, "ooo").await;

    let init = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    let pid = init.producer_id;

    let mk = |base_seq: i32| ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "ooo".into(),
            topic_id: ooo_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_batch_with_producer(pid, 0, base_seq, &["x", "y"]).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // First batch (base_seq=0, 2 records → last_seq=1). Must succeed.
    let r1 = p.client.send(mk(0)).await.expect("Produce seq=0");
    assert!(r1.responses[0].partition_responses[0].error_code == 0);

    // Skip to base_seq=10 — gap → OUT_OF_ORDER_SEQUENCE_NUMBER (45).
    let r2 = p.client.send(mk(10)).await.expect("Produce seq=10");
    assert!(r2.responses[0].partition_responses[0].error_code == 45);

    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_rf_too_high_returns_invalid_replication_factor() {
    let p = support::start().await; // single-voter broker
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "boom".into(),
                num_partitions: 1,
                replication_factor: 5, // single broker → RF=5 is invalid
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        resp.topics[0].error_code == 38 /* INVALID_REPLICATION_FACTOR */
    );
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_txn_creates_topic_and_returns_local_broker() {
    let p = support::start().await; // single-voter broker
    // Use coordinator_keys (v4+ style) so the transaction-id reaches the
    // broker on the wire. key_type=1 selects the TRANSACTION branch.
    let r = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["my-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(TRANSACTION)");
    // The broker bootstraps __transaction_state on demand, resolves the
    // partition leader, and returns itself (the only broker in the cluster).
    assert!((r.error_code, r.coordinators.len()) == (0, 1));
    let c = &r.coordinators[0];
    check!(
        (c.error_code, c.node_id, c.host.is_empty(), c.port > 0) == (0, 1, false, true),
        "coordinator must be this broker with a non-empty endpoint"
    );
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_transactional_id_returns_real_pid() {
    let p = support::start().await;
    // Bootstrap __transaction_state via FindCoordinator (key_type=1).
    // Use coordinator_keys (v4+ wire format) so the transaction-id reaches
    // the broker and triggers topic creation + leader registration.
    let _ = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["my-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");

    let r = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("my-tid".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        })
        .await
        .expect("InitProducerId");
    check!(
        (r.error_code, r.producer_id >= 1_000, r.producer_epoch) == (0, true, 0),
        "successful first allocation must return a real producer id at epoch 0"
    );
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_same_tid_bumps_epoch() {
    let p = support::start().await;
    // Bootstrap __transaction_state for stable-tid.
    let _ = p
        .client
        .send(FindCoordinatorRequest {
            coordinator_keys: vec!["stable-tid".into()],
            key_type: 1, // TRANSACTION
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");

    let r1 = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("stable-tid".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        })
        .await
        .expect("InitProducerId 1");
    assert!((r1.error_code, r1.producer_id >= 1_000, r1.producer_epoch) == (0, true, 0));

    let r2 = p
        .client
        .send(InitProducerIdRequest {
            transactional_id: Some("stable-tid".into()),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        })
        .await
        .expect("InitProducerId 2");
    check!(
        (r2.error_code, r2.producer_id, r2.producer_epoch,)
            == (0, r1.producer_id, r1.producer_epoch + 1),
        "second call must preserve producer id and bump epoch by 1"
    );
    p.broker.shutdown().await;
}
