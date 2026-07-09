#![allow(clippy::pedantic)]

use std::sync::Arc;

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        find_coordinator_request::FindCoordinatorRequest,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("sharegroup-backlog-test")
            .build()
            .await
            .expect("client connects"),
    )
}

async fn create_topic(broker: &crabka_broker::BrokerHandle, client: &Client, topic: &str) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "topic create: {resp:?}");
    broker.wait_until_partition_present(topic, 0).await;
}

fn topic_id(broker: &crabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    broker
        .controller_image_for_test()
        .topic(topic)
        .map(|topic| uuid::Uuid::from_bytes(*topic.topic_id.as_bytes()))
        .expect("topic present")
}

fn wire(topic_id: uuid::Uuid) -> WireUuid {
    WireUuid(*topic_id.as_bytes())
}

async fn bootstrap_share_state(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    group_id: &str,
) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2,
            coordinator_keys: vec![group_id.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert!(
        resp.coordinators[0].error_code == 0,
        "FindCoordinator(SHARE): {resp:?}"
    );
    for partition in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, partition)
            .await;
    }
}

async fn join_share_group(client: &Client, group_id: &str, topic: &str) -> (String, i32) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![topic.into()]),
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat");
    assert!(resp.error_code == 0, "join failed: {resp:?}");
    (resp.member_id.expect("member id"), resp.member_epoch)
}

async fn wait_for_share_init(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    group_id: &str,
    topic: &str,
    member_id: &str,
    member_epoch: i32,
    topic_id: uuid::Uuid,
) {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let _ = client
                .send(ShareGroupHeartbeatRequest {
                    group_id: group_id.into(),
                    member_id: member_id.into(),
                    member_epoch,
                    subscribed_topic_names: Some(vec![topic.into()]),
                    ..Default::default()
                })
                .await;
            if broker
                .share_state_summary_for_test(group_id, topic_id, 0)
                .await
                .is_some()
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "share state did not initialize");
}

async fn produce_n(client: &Client, topic: &str, topic_id: uuid::Uuid, n: i64) {
    let records: Vec<Record> = (0..n)
        .map(|index| Record {
            offset_delta: i32::try_from(index).expect("test batch fits i32"),
            value: Some(bytes::Bytes::copy_from_slice(
                format!("v{index}").as_bytes(),
            )),
            ..Default::default()
        })
        .collect();
    let resp = client
        .send(ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.to_string(),
                topic_id: wire(topic_id),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(
                        RecordBatch {
                            last_offset_delta: i32::try_from(n - 1).expect("test batch fits i32"),
                            records,
                            ..Default::default()
                        }
                        .into(),
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    let partition = &resp.responses[0].partition_responses[0];
    assert!(partition.error_code == 0, "produce failed: {partition:?}");
}

async fn list_offset(client: &Client, topic: &str, partition: i32, timestamp: i64) -> i64 {
    let resp = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_string(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    let partition = &resp.topics[0].partitions[0];
    assert!(
        partition.error_code == 0,
        "ListOffsets failed: {partition:?}"
    );
    partition.offset
}

async fn scrape(addr: std::net::SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect metrics");
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.expect("write HTTP");
    stream.flush().await.expect("flush HTTP");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read HTTP");
    let response = String::from_utf8(buf).expect("metrics are UTF-8");
    let body_start = response.find("\r\n\r\n").map_or(0, |index| index + 4);
    response[body_start..].to_string()
}

async fn wait_for_metric(addr: std::net::SocketAddr, needle: &str) {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let body = scrape(addr).await;
            if body.contains(needle) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "metric {needle:?} did not appear");
}

fn require_replicated_external_backlog_gate() {
    assert!(
        std::env::var_os("CRABKA_RUN_SHAREGROUP_BACKLOG_EXTERNAL").is_some(),
        "set CRABKA_RUN_SHAREGROUP_BACKLOG_EXTERNAL=1 after starting a replicated multi-broker \
         fixture with a non-coordinator data leader, Prometheus scraping every broker, a forced \
         coordinator handoff, and a share-group delete/tombstone path available"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_gauge_reports_local_share_group_backlog() {
    let log_dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("metrics addr"));
    config.share_group_backlog_poll_interval_secs = 1;

    let broker = Broker::start(config).await.expect("broker starts");
    let metrics_addr = broker.metrics_addr().expect("metrics bound");
    let bootstrap = broker.listen_addr().to_string();
    let client = connect(&bootstrap).await;

    let topic = "bk-itest";
    let group_id = "bk-g";
    create_topic(&broker, &client, topic).await;
    let topic_id = topic_id(&broker, topic);
    bootstrap_share_state(&broker, &client, &format!("{group_id}:{topic_id}:0")).await;
    let (member_id, member_epoch) = join_share_group(&client, group_id, topic).await;
    wait_for_share_init(
        &broker,
        &client,
        group_id,
        topic,
        &member_id,
        member_epoch,
        topic_id,
    )
    .await;
    produce_n(&client, topic, topic_id, 5).await;
    assert!(list_offset(&client, topic, 0, -1).await == 5);
    assert!(list_offset(&client, topic, 0, -2).await == 0);

    wait_for_metric(
        metrics_addr,
        "crabka_broker_share_group_backlog{group_id=\"bk-g\",topic=\"bk-itest\",partition=\"0\"} 5",
    )
    .await;

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rf1_list_offsets_reports_hwm_and_log_start_used_by_backlog_reader() {
    let log_dir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("metrics addr"));
    config.share_group_backlog_poll_interval_secs = 1;

    let broker = Broker::start(config).await.expect("broker starts");
    let bootstrap = broker.listen_addr().to_string();
    let client = connect(&bootstrap).await;

    let topic = "bk-logstart-itest";
    create_topic(&broker, &client, topic).await;
    let topic_id = topic_id(&broker, topic);
    produce_n(&client, topic, topic_id, 5).await;
    broker
        .test_advance_log_start(topic, 0, 2)
        .await
        .expect("advance log start");
    assert!(list_offset(&client, topic, 0, -1).await == 5);
    assert!(list_offset(&client, topic, 0, -2).await == 2);

    broker.shutdown().await;
}

#[test]
#[ignore = "requires replicated multi-broker fixture for remote-HWM parity, coordinator handoff, and delete/tombstone smoke validation"]
fn replicated_multi_broker_external_gate_is_declared() {
    require_replicated_external_backlog_gate();
}
