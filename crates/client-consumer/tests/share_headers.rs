use std::time::Duration;

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{Header as ConsumerHeader, ShareAckMode, ShareConsumer};
use crabka_client_core::Client;
use crabka_client_producer::{Header as ProducerHeader, Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    find_coordinator_request::FindCoordinatorRequest,
};

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

async fn create_topic(client: &Client, topic: &str) {
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
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
}

fn topic_id(broker: &crabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|topic| *topic.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

async fn bootstrap_share_state(broker: &crabka_broker::BrokerHandle, client: &Client, key: &str) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2,
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert!(
        resp.coordinators[0].error_code == 0,
        "FindCoordinator(SHARE)"
    );
    for partition in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, partition)
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn share_consumer_record_carries_headers() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("share-header-admin")
        .build()
        .await
        .unwrap();
    create_topic(&admin, "share-h").await;
    broker.wait_until_partition_present("share-h", 0).await;
    let topic_id = topic_id(&broker, "share-h");
    bootstrap_share_state(&broker, &admin, &format!("share-h-group:{topic_id}:0")).await;

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-header-producer")
        .build()
        .await
        .unwrap();
    producer
        .send(ProducerRecord {
            topic: "share-h".into(),
            partition: Some(0),
            key: None,
            value: Some("v".into()),
            headers: vec![
                ProducerHeader {
                    key: "trace".into(),
                    value: Some("abc".into()),
                },
                ProducerHeader {
                    key: "empty".into(),
                    value: Some(bytes::Bytes::new()),
                },
                ProducerHeader {
                    key: "null".into(),
                    value: None,
                },
            ],
            timestamp_ms: None,
        })
        .await
        .await
        .unwrap()
        .unwrap();
    producer.flush().await.unwrap();

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-header-consumer")
        .group_id("share-h-group")
        .subscribe(["share-h".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(Duration::from_millis(300))
        .build()
        .await
        .unwrap();
    broker
        .wait_for_share_state_summary("share-h-group", topic_id, 0)
        .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let record = loop {
        let records = consumer.poll(Duration::from_millis(300)).await.unwrap();
        if let Some(record) = records.into_iter().next() {
            break record;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "share poll returned no records"
        );
    };

    assert!(record.topic == "share-h");
    assert!(record.partition == 0);
    assert!(
        record.headers
            == vec![
                ConsumerHeader {
                    key: "trace".into(),
                    value: Some("abc".into()),
                },
                ConsumerHeader {
                    key: "empty".into(),
                    value: Some(bytes::Bytes::new()),
                },
                ConsumerHeader {
                    key: "null".into(),
                    value: None,
                },
            ]
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
