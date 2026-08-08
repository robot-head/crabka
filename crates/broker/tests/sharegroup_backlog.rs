//! End-to-end coverage for the coordinator-emitted share-group backlog gauge.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec, metrics::ShareGroupLabel};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
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
};

const TOPIC: &str = "backlog-itest";
const GROUP: &str = "backlog-workers";

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

async fn create_topic(client: &Client) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(response.topics[0].error_code == 0, "{response:?}");
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
    create_topic(&client).await;
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

    broker.shutdown().await;
}
