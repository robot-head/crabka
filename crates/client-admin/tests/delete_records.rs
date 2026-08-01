use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec, DeleteRecordsOp};
use crabka_client_core::{ClientError, Connection, ConnectionOptions, fetch_partition};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_truncates_wal_and_maps_outcome() {
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
            &[CreateTopicSpec {
                name: "wal".to_string(),
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
    for offset in 0..100 {
        producer
            .send(ProducerRecord {
                topic: "wal".to_string(),
                partition: Some(0),
                key: None,
                value: Some(format!("frame-{offset}").into_bytes().into()),
                headers: Vec::new(),
                timestamp_ms: None,
            })
            .await
            .await
            .unwrap()
            .unwrap();
    }
    producer.flush().await.unwrap();

    let outcomes = admin
        .delete_records(
            &[DeleteRecordsOp {
                topic: "wal".to_string(),
                partition: 0,
                offset: 50,
            }],
            crabka_units::secs(5),
        )
        .await
        .unwrap();

    assert!(
        outcomes
            == vec![crabka_client_admin::DeleteRecordsOutcome {
                topic: "wal".to_string(),
                partition: 0,
                error_code: 0,
                low_watermark: 50,
            }]
    );

    let topic_id = admin
        .metadata(&["wal"])
        .await
        .unwrap()
        .topics
        .into_iter()
        .find(|topic| topic.name == "wal")
        .and_then(|topic| topic.topic_id)
        .map_or(WireUuid::ZERO, |id| WireUuid(*id.as_bytes()));

    let reader = connect_reader(&bootstrap).await;
    let fetch_error = fetch_partition(
        &reader,
        "wal",
        topic_id,
        0,
        0,
        crabka_units::millis(500),
        crabka_units::mebibytes(1),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(fetch_error, ClientError::Server { error_code: 1, .. }),
        "fetch below log start should return OFFSET_OUT_OF_RANGE, got {fetch_error:?}"
    );
}

async fn connect_reader(bootstrap: &str) -> Connection {
    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .expect("resolve bootstrap")
        .next()
        .expect("bootstrap address");
    Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: "delete-records-test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect reader")
}
