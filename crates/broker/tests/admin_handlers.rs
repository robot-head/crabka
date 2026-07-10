// rustc 1.95 clippy::pedantic ICEs on this file (an upstream bug in
// clippy's body-analysis pass). Disable pedantic locally; the rest of
// the workspace still enforces the full pedantic gate.
#![allow(clippy::pedantic)]

//! Broker-side integration tests for the admin handlers.
//!
//! Each test spins up a 1-broker cluster via [`support::start_n_node`],
//! dispatches the relevant request through `crabka-client-core`, and
//! asserts on either the response or observable broker state exposed by
//! the `BrokerHandle` test-helper methods.

#![allow(clippy::default_trait_access, clippy::manual_assert)]

use assert2::{assert, check};
mod support;

use std::time::Duration;

use bytes::Bytes;
use crabka_protocol::{
    owned::{
        alter_configs_request::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig},
        create_partitions_request::{CreatePartitionsRequest, CreatePartitionsTopic},
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        describe_cluster_request::DescribeClusterRequest,
        describe_quorum_request::{
            DescribeQuorumRequest, PartitionData as DescribeQuorumReqPartition,
            TopicData as DescribeQuorumReqTopic,
        },
        list_config_resources_request::ListConfigResourcesRequest,
        list_groups_request::ListGroupsRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use support::start_n_node;

/// Kafka resource type id for a topic.
const RESOURCE_TYPE_TOPIC: i8 = 2;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn build_client(addr: std::net::SocketAddr) -> crabka_client_core::Client {
    crabka_client_core::Client::builder()
        .bootstrap(format!("127.0.0.1:{}", addr.port()))
        .client_id("admin-handlers-test")
        .build()
        .await
        .expect("client build")
}

async fn create_topic_helper(client: &crabka_client_core::Client, name: &str, partitions: i32) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_topics");
    let result = &resp.topics[0];
    assert!(
        result.error_code == 0,
        "create_topics failed: {:?}",
        result.error_message
    );
}

// ── AlterConfigs (api_key 33) ────────────────────────────────────────────────

/// AlterConfigs round-trip: setting `retention.ms` on a known topic returns
/// `error_code == 0`, and the supervisor eventually pushes the new config
/// into the partition's log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_round_trip() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-alter", 1).await;

    let req = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t-alter".into(),
            configs: vec![AlterableConfig {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("alter_configs");
    assert!(
        resp.responses[0].error_code == 0,
        "alter_configs response: {:?}",
        resp.responses[0].error_message
    );

    // Wait for the supervisor reconcile loop to push the new config into the
    // partition's log. The supervisor runs on every metadata-image update
    // (typically within a few hundred ms). The partition is queryable
    // immediately after `create_topic_helper` returns, carrying the broker's
    // default retention; we poll until the supervisor swaps in the override
    // (or until the deadline).
    //
    // intentional poll (not an awaiter): the override lands in the local log
    // config *after* the image commits, so no image/metric signal reflects it
    // — same convergence gate the recompression / tiered-storage tests use.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let want = Duration::from_millis(60_000);
    let last = loop {
        let cur = broker
            .partition_retention_ms_for_test("t-alter", 0)
            .and_then(|inner| inner);
        if cur == Some(want) {
            break cur;
        }
        if std::time::Instant::now() > deadline {
            break cur;
        }
        tokio::task::yield_now().await;
    };
    assert!(
        last == Some(want),
        "retention_ms did not converge within 10 s after AlterConfigs"
    );
}

/// AlterConfigs rejects an unknown key with `error_code == 40` (INVALID_CONFIG)
/// and includes the offending key name in the error message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_configs_rejects_unknown_key() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-bad-cfg", 1).await;

    let req = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t-bad-cfg".into(),
            configs: vec![AlterableConfig {
                name: "flush.ms".into(),
                value: Some("1000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("alter_configs");
    // 40 = INVALID_CONFIG
    assert!(
        (
            resp.responses[0].error_code,
            resp.responses[0]
                .error_message
                .as_deref()
                .unwrap_or("")
                .contains("flush.ms"),
        ) == (40, true),
        "expected INVALID_CONFIG mentioning flush.ms, got {:?}",
        resp.responses[0]
    );
}

/// `min.insync.replicas` pre-flight: after the operator sets
/// `min.insync.replicas=2` via `AlterConfigs`, an `acks=-1` produce
/// against a 1-broker cluster (ISR={1}, isr.len()=1) must fail fast
/// with `NOT_ENOUGH_REPLICAS` (19) — before the writer queues the
/// batch. An `acks=1` produce against the same topic still succeeds
/// because leader-only acks bypass the ISR threshold entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn min_insync_replicas_blocks_acks_all_when_isr_too_small() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-min-isr", 1).await;

    // Wait for partition 0 to materialize; otherwise the produce path returns
    // UNKNOWN_TOPIC_OR_PARTITION before the min.insync.replicas pre-flight runs.
    broker.wait_until_partition_present("t-min-isr", 0).await;

    // Produce v13+ drops `name` from the wire and demands `topic_id`.
    // Fetch it via Metadata so the produce calls below resolve.
    let md = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("t-min-isr".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    let topic_id: WireUuid = md
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("t-min-isr"))
        .expect("topic in Metadata response")
        .topic_id;

    // Set min.insync.replicas=2 on the topic. The 1-broker cluster only
    // has ISR={1}, so this is impossible to satisfy.
    let alter = AlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "t-min-isr".into(),
            configs: vec![AlterableConfig {
                name: "min.insync.replicas".into(),
                value: Some("2".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    let alter_resp = client.send(alter).await.expect("alter_configs");
    assert!(
        alter_resp.responses[0].error_code == 0,
        "AlterConfigs must accept min.insync.replicas=2: {:?}",
        alter_resp.responses[0].error_message
    );

    // Build a one-record batch for the produce calls below.
    let batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::from_static(b"x")),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };

    // acks=-1 ("all"): must be rejected pre-flight with NOT_ENOUGH_REPLICAS (19).
    let bad = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "t-min-isr".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.clone().into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce (acks=-1)");
    assert!(
        bad.responses[0].partition_responses[0].error_code == 19,
        "acks=-1 with isr.len()=1 < min.insync.replicas=2 must return NOT_ENOUGH_REPLICAS (19); \
         got code = {}",
        bad.responses[0].partition_responses[0].error_code
    );

    // acks=1: leader-only — min.insync.replicas does NOT gate, so this
    // must still succeed even though the threshold is unsatisfiable for
    // acks=all.
    let ok = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "t-min-isr".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce (acks=1)");
    assert!(
        ok.responses[0].partition_responses[0].error_code == 0,
        "acks=1 must succeed regardless of min.insync.replicas; got code = {}",
        ok.responses[0].partition_responses[0].error_code
    );
}

// ── CreatePartitions (api_key 37) ────────────────────────────────────────────

/// CreatePartitions: extending a 1-partition topic to 3 returns
/// `error_code == 0`, and all three partitions materialise in the broker's
/// local registry within a few seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_extends_topic() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-cp", 1).await;

    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cp".into(),
            count: 3,
            assignments: None,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_partitions");
    assert!(
        resp.results[0].error_code == 0,
        "create_partitions result: {:?}",
        resp.results[0].error_message
    );

    // Wait for the supervisor reconcile to materialise all three partitions.
    for p in 0..3 {
        broker.wait_until_partition_present("t-cp", p).await;
    }
}

/// CreatePartitions: explicit `assignments` list. The topic's rf is 1 on a
/// single-broker cluster, and the operator pins the new partition to
/// broker 0. The handler must accept it (error_code == 0) and materialise
/// the partition. A second call with a wrong-length assignment list must
/// return `INVALID_REPLICA_ASSIGNMENT` (39).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_honors_explicit_assignments() {
    use crabka_protocol::owned::create_partitions_request::CreatePartitionsAssignment;

    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-cpa", 1).await;

    // Happy path: 1 existing partition → 2 partitions, explicit assignment
    // pins broker 0 (the only one available).
    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cpa".into(),
            count: 2,
            assignments: Some(vec![CreatePartitionsAssignment {
                broker_ids: vec![1],
                ..Default::default()
            }]),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client
        .send(req)
        .await
        .expect("create_partitions (explicit)");
    assert!(
        resp.results[0].error_code == 0,
        "explicit assignment must succeed: {:?}",
        resp.results[0].error_message
    );

    // Wait for the new partition to materialise.
    broker.wait_until_partition_present("t-cpa", 1).await;

    // Invalid path: ask for 1 more partition (total 3) but supply 2
    // assignments. Must surface INVALID_REPLICA_ASSIGNMENT and NOT add a
    // partition.
    let bad = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cpa".into(),
            count: 3,
            assignments: Some(vec![
                CreatePartitionsAssignment {
                    broker_ids: vec![1],
                    ..Default::default()
                },
                CreatePartitionsAssignment {
                    broker_ids: vec![1],
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let bad_resp = client
        .send(bad)
        .await
        .expect("create_partitions (length-mismatch)");
    assert!(
        bad_resp.results[0].error_code == 39,
        "length-mismatch must return INVALID_REPLICA_ASSIGNMENT (39): {:?}",
        bad_resp.results[0].error_message
    );
    assert!(
        !broker.partition_exists_for_test("t-cpa", 2),
        "partition 2 must NOT have been created on an INVALID_REPLICA_ASSIGNMENT path",
    );
}

// ── DeleteRecords (api_key 21) ───────────────────────────────────────────────

/// DeleteRecords: producing 100 records and then trimming from offset 50
/// returns a valid `low_watermark`, and the broker's `log_start_offset`
/// moves forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_trims_log_start() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-dr", 1).await;

    // Produce 100 single-record batches through the broker's test helper.
    broker
        .produce_records_for_test("t-dr", 0, 100)
        .await
        .expect("produce_records_for_test");

    let req = DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: "t-dr".into(),
            partitions: vec![DeleteRecordsPartition {
                partition_index: 0,
                offset: 50,
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("delete_records");
    let part_result = &resp.topics[0].partitions[0];
    check!(
        (
            part_result.error_code,
            part_result.low_watermark >= 0,
            part_result.low_watermark <= 50,
        ) == (0, true, true),
        "low_watermark {} should be <= requested offset 50",
        part_result.low_watermark
    );

    let log_start = broker
        .partition_log_start_for_test("t-dr", 0)
        .expect("partition exists");
    assert!(
        log_start == part_result.low_watermark,
        "partition log_start_offset should equal low_watermark"
    );
}

// ── DescribeCluster (api_key 60) ─────────────────────────────────────────────

/// DescribeCluster on a 1-broker cluster returns `error_code == 0`, exactly
/// one broker entry, and `controller_id == 1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_lists_brokers() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let resp = client
        .send(DescribeClusterRequest::default())
        .await
        .expect("describe_cluster");
    check!((resp.error_code, resp.brokers.len(), resp.controller_id) == (0, 1, 1));
}

/// KIP-919: `DescribeCluster` with `endpoint_type = 2` (CONTROLLERS) projects
/// the KRaft voter set instead of the broker set, so an AdminClient can
/// discover the controller quorum. On a 1-node cluster that is the single
/// bootstrap voter (id=1) advertised on its CONTROLLER listener endpoint, and
/// the response echoes `endpoint_type = 2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_cluster_endpoint_type_controllers_lists_voters() {
    const ENDPOINT_TYPE_CONTROLLERS: i8 = 2;
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let resp = client
        .send(DescribeClusterRequest {
            endpoint_type: ENDPOINT_TYPE_CONTROLLERS,
            ..Default::default()
        })
        .await
        .expect("describe_cluster controllers");
    check!(
        (
            resp.error_code,
            resp.endpoint_type,
            resp.brokers.len(),
            resp.brokers[0].broker_id,
            resp.brokers[0].host.is_empty(),
            resp.brokers[0].port > 0,
        ) == (0, ENDPOINT_TYPE_CONTROLLERS, 1, 1, false, true),
        "controller endpoint response mismatch: {resp:?}"
    );
}

// ── DescribeQuorum (api_key 55, KIP-595) ───────────────────────────────────

/// DescribeQuorum against the cluster-metadata topic on a 1-broker
/// cluster returns one partition row carrying the broker's voter id with
/// leader_id == 1. Verifies the dispatch glue, the ACL allow path, and
/// the response encoding — the pure `build_topic_responses` helper has
/// its own unit tests in
/// `crates/broker/src/handlers/describe_quorum.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_quorum_reports_cluster_metadata_voter_set() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let req = DescribeQuorumRequest {
        topics: vec![DescribeQuorumReqTopic {
            topic_name: "__cluster_metadata".into(),
            partitions: vec![DescribeQuorumReqPartition {
                partition_index: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = client.send(req).await.expect("describe_quorum");
    check!(
        (
            resp.error_code,
            resp.topics.len(),
            resp.topics[0].topic_name.as_str()
        ) == (0, 1, "__cluster_metadata")
    );
    let pd = &resp.topics[0].partitions[0];
    check!(
        (
            pd.partition_index,
            pd.error_code,
            pd.leader_id,
            pd.leader_epoch >= 1,
            pd.high_watermark >= 0,
            pd.current_voters.len(),
            pd.current_voters[0].replica_id,
            pd.current_voters[0].log_end_offset >= 0,
            pd.observers.is_empty(),
        ) == (0, 0, 1, true, true, 1, 1, true, true),
        "DescribeQuorum partition projection mismatch: {pd:?}"
    );
}

// ── ListConfigResources (api_key 74, KIP-1142) ─────────────────────────────

/// ListConfigResources v1 with empty `resource_types` returns the default
/// set: every topic + every broker (+ empty client-metrics). Verifies the
/// dispatch glue, the ACL gate's allow path, and the response encoding —
/// the pure `collect_resources` helper has its own unit tests in
/// `crates/broker/src/handlers/list_config_resources.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_config_resources_default_set_includes_topics_and_brokers() {
    const RESOURCE_TYPE_BROKER: i8 = 4;

    let cluster = start_n_node(1).await.expect("start_n_node");
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-lcr-a", 1).await;
    create_topic_helper(&client, "t-lcr-b", 1).await;

    let resp = client
        .send(ListConfigResourcesRequest::default())
        .await
        .expect("list_config_resources");
    assert!(resp.error_code == 0, "list_config_resources error_code");

    // Default set on a 1-broker cluster with two topics: 2 topic entries
    // (type 2) + 1 broker entry (type 4) + 0 client-metrics entries.
    let topics: Vec<&str> = resp
        .config_resources
        .iter()
        .filter(|r| r.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|r| r.resource_name.as_str())
        .collect();
    assert!(
        topics.contains(&"t-lcr-a") && topics.contains(&"t-lcr-b"),
        "expected both seeded topics in response, got {topics:?}"
    );
    let brokers: Vec<&str> = resp
        .config_resources
        .iter()
        .filter(|r| r.resource_type == RESOURCE_TYPE_BROKER)
        .map(|r| r.resource_name.as_str())
        .collect();
    assert!(
        brokers == vec!["1"],
        "expected exactly broker '1', got {brokers:?}"
    );
}

// ── ListGroups (api_key 16) ──────────────────────────────────────────────────

/// ListGroups includes a group that was injected directly into the
/// `GroupManager` via the test helper, without running a full JoinGroup /
/// SyncGroup exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_freshly_created_group() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    // Seed the group manager directly with a new group.
    broker.group_create_for_test("test-group-listed");

    let resp = client
        .send(ListGroupsRequest::default())
        .await
        .expect("list_groups");
    assert!(resp.error_code == 0, "list_groups error_code");

    let ids: Vec<&str> = resp.groups.iter().map(|g| g.group_id.as_str()).collect();
    assert!(
        ids.contains(&"test-group-listed"),
        "expected `test-group-listed` in group list, got {ids:?}"
    );
}
