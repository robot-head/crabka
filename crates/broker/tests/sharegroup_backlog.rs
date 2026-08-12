//! End-to-end coverage for the coordinator-emitted share-group backlog gauge.

use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec, metrics::ShareGroupLabel};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        alter_share_group_offsets_request::{
            AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
            AlterShareGroupOffsetsRequestTopic,
        },
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        find_coordinator_request::FindCoordinatorRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use crabka_security::ListenerProtocol;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    sync::Mutex,
};

const TOPIC: &str = "backlog-itest";
const GROUP: &str = "backlog-workers";
const OFFSETS_TOPIC: &str = "__consumer_offsets";
const SHARE_STATE_TOPIC: &str = "__share_group_state";

mod support;

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await.unwrap();
    let response = String::from_utf8(bytes).unwrap();
    let body = response.find("\r\n\r\n").map_or(0, |at| at + 4);
    response[body..].to_owned()
}

async fn create_topic(client: &Client, partitions: i32, replication_factor: i16) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: partitions,
                replication_factor,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(response.topics[0].error_code == 0, "{response:?}");
}

fn java_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn group_for_partition(partition: i32, partition_count: i32) -> String {
    (0..100)
        .map(|i| format!("backlog-rf3-{i}"))
        .find(|group| {
            let hash = java_hash(group);
            let positive = if hash == i32::MIN { 0 } else { hash.abs() };
            positive % partition_count == partition
        })
        .expect("each offsets partition receives a candidate group")
}

async fn produce_five(client: &Client, topic_id: uuid::Uuid) {
    let records = (0..5)
        .map(|offset| Record {
            offset_delta: offset,
            value: Some(bytes::Bytes::from_static(b"work")),
            ..Default::default()
        })
        .collect();
    let response = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(
                        RecordBatch {
                            last_offset_delta: 4,
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
        .unwrap();
    assert!(
        response.responses[0].partition_responses[0].error_code == 0,
        "{response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_is_scraped_and_survives_scale_to_zero() {
    let _guard = test_lock().lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listeners = vec![ListenerSpec {
        name: "PLAINTEXT".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::Plaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    config.inter_broker_listener_name = "PLAINTEXT".into();
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    config.share_coordinator.state_topic_num_partitions = 1;
    config.share_group.backlog_poll_interval = Duration::from_millis(50);

    let broker = Broker::start(config).await.unwrap();
    let client = Arc::new(
        Client::builder()
            .bootstrap(broker.listen_addr().to_string())
            .client_id("backlog-itest")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, 1, 1).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    let topic_id = broker
        .controller_image_for_test()
        .topic(TOPIC)
        .map(|topic| topic.topic_id)
        .expect("topic metadata");
    produce_five(&client, topic_id).await;

    let joined = client
        .send(ShareGroupHeartbeatRequest {
            group_id: GROUP.into(),
            member_id: "member-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![TOPIC.into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(joined.error_code == 0, "{joined:?}");
    broker
        .wait_for_share_state_summary(GROUP, topic_id, 0)
        .await;

    let label = ShareGroupLabel {
        group_id: GROUP.into(),
        topic: TOPIC.into(),
        partition: 0,
    };
    broker
        .wait_for_metrics("share-group backlog = 5", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 5
        })
        .await;

    let metrics_addr = broker.metrics_addr().unwrap();
    let expected = format!(
        "crabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\",partition=\"0\"}} 5"
    );
    assert!(scrape(metrics_addr).await.contains(&expected));

    let left = client
        .send(ShareGroupHeartbeatRequest {
            group_id: GROUP.into(),
            member_id: joined.member_id.unwrap(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(left.error_code == 0, "{left:?}");
    assert!(
        broker
            .share_state_summary_for_test(GROUP, topic_id, 0)
            .await
            .is_some(),
        "the durable cursor must survive when the final consumer leaves"
    );
    assert!(scrape(metrics_addr).await.contains(&expected));

    let altered = client
        .send(AlterShareGroupOffsetsRequest {
            group_id: GROUP.into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: TOPIC.into(),
                partitions: vec![AlterShareGroupOffsetsRequestPartition {
                    partition_index: 0,
                    start_offset: 5,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(altered.error_code == 0, "{altered:?}");
    assert!(
        altered.responses[0].partitions[0].error_code == 0,
        "{altered:?}"
    );
    broker
        .wait_for_metrics("share-group backlog drains to zero", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;
    let drained = format!(
        "crabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\",partition=\"0\"}} 0"
    );
    assert!(scrape(metrics_addr).await.contains(&drained));

    let deleted = client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(TOPIC.into()),
                ..Default::default()
            }],
            topic_names: vec![TOPIC.into()],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(deleted.responses[0].error_code == 0, "{deleted:?}");
    broker
        .wait_for_image(|image| image.topic(TOPIC).is_none())
        .await;
    broker
        .wait_for_metrics("deleted topic backlog series is removed", |metrics| {
            metrics.share_group_backlog.get(&label).is_none()
        })
        .await;
    assert!(!scrape(metrics_addr).await.contains(&format!(
        "crabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\""
    )));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn rf_three_remote_leader_uses_committed_high_watermark() {
    let _guard = test_lock().lock().await;
    let mut attempt = 0;
    let mut cluster = loop {
        attempt += 1;
        match support::start_n_node_with(3, |_, config| {
            config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
            config.offsets_topic_num_partitions = 3;
            config.share_coordinator.state_topic_num_partitions = 1;
            config.share_group.backlog_poll_interval = Duration::from_millis(50);
            config.replica_lag_time_max = crabka_units::secs(30);
        })
        .await
        {
            Ok(cluster) => break cluster,
            Err(error) if attempt < 3 => {
                eprintln!("backlog RF=3 cluster start attempt {attempt} failed: {error}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => panic!("backlog RF=3 cluster failed to start: {error}"),
        }
    };
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let admin = Arc::new(
        Client::builder()
            .bootstrap(cluster[0].0.listen_addr().to_string())
            .client_id("backlog-rf3-admin")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&admin, 3, 3).await;
    for (broker, _, _) in &cluster {
        broker.wait_until_partition_present(TOPIC, 2).await;
    }
    let topic_id = cluster[0]
        .0
        .controller_image_for_test()
        .topic(TOPIC)
        .expect("topic metadata")
        .topic_id;
    let mut share_ready = false;
    for _ in 0..40 {
        let response = admin
            .send(FindCoordinatorRequest {
                key_type: 2,
                coordinator_keys: vec![format!("backlog-rf3-bootstrap:{topic_id}:0")],
                ..Default::default()
            })
            .await
            .unwrap();
        if response.coordinators[0].error_code == 0 {
            share_ready = true;
            break;
        }
        assert!(response.coordinators[0].error_code == 15, "{response:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(share_ready, "share coordinator did not become ready");
    for (broker, _, _) in &cluster {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, 0)
            .await;
    }

    let image = cluster[0].0.controller_image_for_test();
    let offsets_partitions = image.topic_partition_count(OFFSETS_TOPIC);
    let state_leader = image
        .partition(SHARE_STATE_TOPIC, 0)
        .expect("share-state partition 0")
        .leader;
    let controller_leader = cluster[0].0.controller_leader_id();
    let nodes = [
        crabka_broker::NodeId(1),
        crabka_broker::NodeId(2),
        crabka_broker::NodeId(3),
    ];
    let mut candidates = Vec::new();
    for offsets_partition in 0..offsets_partitions {
        let coordinator_id = image
            .partition(OFFSETS_TOPIC, offsets_partition)
            .expect("offsets partition")
            .leader;
        for data_partition in 0..3 {
            let data_leader_id = image
                .partition(TOPIC, data_partition)
                .expect("data partition")
                .leader;
            if coordinator_id == data_leader_id {
                continue;
            }
            let stopped_id = nodes
                .into_iter()
                .find(|id| *id != coordinator_id && *id != data_leader_id)
                .expect("third broker");
            if stopped_id != state_leader {
                candidates.push((
                    offsets_partition,
                    coordinator_id,
                    data_partition,
                    data_leader_id,
                    stopped_id,
                ));
            }
        }
    }
    let candidate = candidates
        .iter()
        .find(|candidate| Some(candidate.4) != controller_leader)
        .or_else(|| candidates.first())
        .copied()
        .expect("remote data leader with a live share-state leader");
    let (offsets_partition, coordinator_id, data_partition, data_leader_id, stopped_id) = candidate;
    let leader_epoch = image
        .partition(TOPIC, data_partition)
        .expect("data partition metadata")
        .leader_epoch;
    drop(image);

    let coordinator_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == coordinator_id)
        .unwrap();
    let group_id = group_for_partition(offsets_partition, offsets_partitions);
    let coordinator_client = Arc::new(
        Client::builder()
            .bootstrap(cluster[coordinator_index].0.listen_addr().to_string())
            .client_id("backlog-rf3-coordinator")
            .build()
            .await
            .unwrap(),
    );
    let share_coordinator = coordinator_client
        .send(FindCoordinatorRequest {
            key_type: 2,
            coordinator_keys: vec![format!("{group_id}:{topic_id}:{data_partition}")],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        share_coordinator.coordinators[0].error_code == 0,
        "{share_coordinator:?}"
    );
    let joined = coordinator_client
        .send(ShareGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![TOPIC.into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(joined.error_code == 0, "{joined:?}");
    let member_id = joined.member_id.expect("broker mints a share member id");
    let mut member_epoch = joined.member_epoch;
    let initialized = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let heartbeat = coordinator_client
                .send(ShareGroupHeartbeatRequest {
                    group_id: group_id.clone(),
                    member_id: member_id.clone(),
                    member_epoch,
                    subscribed_topic_names: Some(vec![TOPIC.into()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(heartbeat.error_code == 0, "{heartbeat:?}");
            member_epoch = heartbeat.member_epoch;
            let mut present = false;
            for (broker, _, _) in &cluster {
                present |= broker
                    .share_state_summary_for_test(&group_id, topic_id, data_partition)
                    .await
                    .is_some();
            }
            if present {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(initialized.is_ok(), "RF=3 share state did not initialize");

    let label = ShareGroupLabel {
        group_id: group_id.clone(),
        topic: TOPIC.into(),
        partition: data_partition,
    };
    cluster[coordinator_index]
        .0
        .wait_for_metrics("initial RF=3 backlog sample", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;

    let stopped_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == stopped_id)
        .unwrap();
    let (stopped, _, _) = cluster.remove(stopped_index);
    stopped.shutdown().await;

    let data_leader_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == data_leader_id)
        .unwrap();
    let coordinator_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == coordinator_id)
        .unwrap();
    assert!(
        cluster[data_leader_index]
            .0
            .partition_isr_for_test(TOPIC, data_partition)
            .is_some_and(|isr| isr.len() == 3),
        "stopped follower must remain in the ISR during the probe"
    );
    let last = cluster[data_leader_index]
        .0
        .produce_records_for_test(TOPIC, data_partition, 5)
        .await
        .unwrap();
    assert!(last == 4);
    assert!(
        cluster[data_leader_index]
            .0
            .local_log_end_offset(TOPIC, data_partition)
            == Some(5)
    );

    let data_client = Client::builder()
        .bootstrap(cluster[data_leader_index].0.listen_addr().to_string())
        .client_id("backlog-rf3-data")
        .build()
        .await
        .unwrap();
    let fetched: FetchResponse = data_client
        .send(FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            topics: vec![FetchTopic {
                topic: TOPIC.into(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                partitions: vec![FetchPartition {
                    partition: data_partition,
                    current_leader_epoch: leader_epoch.0,
                    fetch_offset: i64::MAX,
                    partition_max_bytes: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let partition = &fetched.responses[0].partitions[0];
    assert!(partition.error_code == 0, "{fetched:?}");
    assert!(partition.high_watermark == 0, "{fetched:?}");

    // A sentinel makes the poll completion observable: the next real sample
    // must replace it with HWM(0) - SPSO/log-start(0), not the leader LEO(5).
    cluster[coordinator_index]
        .0
        .metrics()
        .share_group_backlog
        .get_or_create(&label)
        .set(99);
    cluster[coordinator_index]
        .0
        .wait_for_metrics("remote RF=3 committed-HWM sample", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;
    assert!(
        cluster[data_leader_index]
            .0
            .partition_isr_for_test(TOPIC, data_partition)
            .is_some_and(|isr| isr.len() == 3),
        "the committed-HWM sample must land before ISR shrink can expose LEO"
    );

    let expected = format!(
        "crabka_broker_share_group_backlog{{group_id=\"{group_id}\",topic=\"{TOPIC}\",partition=\"{data_partition}\"}} 0"
    );
    assert!(
        scrape(cluster[coordinator_index].0.metrics_addr().unwrap())
            .await
            .contains(&expected)
    );
    for (index, (broker, _, _)) in cluster.iter().enumerate() {
        if index != coordinator_index {
            assert!(
                !scrape(broker.metrics_addr().unwrap())
                    .await
                    .contains(&format!("group_id=\"{group_id}\"")),
                "non-coordinator emitted a backlog series"
            );
        }
    }

    for (broker, _, _) in cluster {
        broker.shutdown().await;
    }
}
