//! Broker-side integration tests for the slice-11 admin handlers.
//!
//! Each test spins up a 1-broker cluster via [`support::start_n_node`],
//! dispatches the relevant request through `crabka-client-core`, and
//! asserts on either the response or observable broker state exposed by
//! the `BrokerHandle` test-helper methods.
//!
//! Gated `#[cfg(not(target_os = "windows"))]` to mirror the other
//! multi-node test files: openraft's `debug_assert!` races on the hosted
//! Windows task scheduler, and `start_n_node` boots even a 1-node cluster
//! through the same raft bootstrap path.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::default_trait_access, clippy::manual_assert)]

mod support;

use std::time::Duration;

use crabka_protocol::owned::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use crabka_protocol::owned::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use crabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;
use crabka_protocol::owned::list_groups_request::ListGroupsRequest;

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
    assert_eq!(
        result.error_code, 0,
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
    assert_eq!(
        resp.responses[0].error_code, 0,
        "alter_configs response: {:?}",
        resp.responses[0].error_message
    );

    // Wait for the supervisor reconcile loop to push the new config into the
    // partition's log. The supervisor runs on every metadata-image update
    // (typically within a few hundred ms).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(Some(retention)) = broker.partition_retention_ms_for_test("t-alter", 0) {
            assert_eq!(
                retention,
                Duration::from_millis(60_000),
                "unexpected retention_ms after AlterConfigs"
            );
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("retention.ms did not converge within 10 s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
    assert_eq!(
        resp.responses[0].error_code, 40,
        "expected INVALID_CONFIG(40), got {}",
        resp.responses[0].error_code
    );
    assert!(
        resp.responses[0]
            .error_message
            .as_deref()
            .unwrap_or("")
            .contains("flush.ms"),
        "expected error_message to mention `flush.ms`, got {:?}",
        resp.responses[0].error_message
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
    assert_eq!(
        resp.results[0].error_code, 0,
        "create_partitions result: {:?}",
        resp.results[0].error_message
    );

    // Wait for the supervisor reconcile to materialise the new partition dirs.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let all_present = (0..3).all(|p| broker.partition_exists_for_test("t-cp", p));
        if all_present {
            break;
        }
        if std::time::Instant::now() > deadline {
            let present: Vec<i32> = (0..3)
                .filter(|&p| broker.partition_exists_for_test("t-cp", p))
                .collect();
            panic!("only partitions {present:?} present after 10 s; expected [0, 1, 2]");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
    assert_eq!(
        part_result.error_code, 0,
        "delete_records error: {:?}",
        part_result.error_code
    );
    // low_watermark must be the resulting log_start_offset after trim.
    assert!(
        part_result.low_watermark >= 0,
        "low_watermark should be non-negative, got {}",
        part_result.low_watermark
    );
    assert!(
        part_result.low_watermark <= 50,
        "low_watermark {} should be <= requested offset 50",
        part_result.low_watermark
    );

    let log_start = broker
        .partition_log_start_for_test("t-dr", 0)
        .expect("partition exists");
    assert_eq!(
        log_start, part_result.low_watermark,
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
    assert_eq!(resp.error_code, 0, "describe_cluster error_code");
    assert_eq!(resp.brokers.len(), 1, "expected exactly 1 broker");
    assert_eq!(resp.controller_id, 1, "expected controller_id == 1");
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
    assert_eq!(resp.error_code, 0, "list_groups error_code");

    let ids: Vec<&str> = resp.groups.iter().map(|g| g.group_id.as_str()).collect();
    assert!(
        ids.contains(&"test-group-listed"),
        "expected `test-group-listed` in group list, got {ids:?}"
    );
}
