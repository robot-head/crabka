use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::AdminClient;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::{Producer, ProducerRecord};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lists_groups_and_committed_offsets() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[crabka_client_admin::CreateTopicSpec {
                name: "t1".into(),
                partitions: 1,
                replicas: 1,
                configs: std::collections::BTreeMap::default(),
            }],
            crabka_units::secs(5),
        )
        .await
        .unwrap();

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    producer
        .send(ProducerRecord {
            topic: "t1".into(),
            partition: None,
            key: None,
            value: Some("v".into()),
            headers: vec![],
            timestamp_ms: None,
        })
        .await
        .await
        .unwrap()
        .unwrap();
    producer.flush().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("g1")
        .subscribe(vec!["t1".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let _ = consumer.poll(crabka_units::secs(2)).await.unwrap();
    consumer.commit_sync().await.unwrap();

    let groups = admin.list_groups().await.unwrap();
    assert2::assert!(groups.iter().any(|g| g == "g1"));

    let offsets = admin.list_consumer_group_offsets("g1").await.unwrap();
    let committed = offsets.get(&("t1".to_string(), 0)).copied();
    assert2::assert!(committed == Some(1));
}
