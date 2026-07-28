//! End-to-end integration test: a real `KafkaStreams` runtime running against an
//! in-process broker, processing records through a typed upper-case topology.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, FetchedRecord, fetch_partition};
use crabka_client_streams::{
    KafkaStreams, NodeHandle, Processor, ProcessorContext, Record, Topology,
};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

// ─── helpers (identical to tests/integration.rs) ─────────────────────────────

async fn boot() -> (BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert_eq!(
        resp.error_code, 0,
        "streams.version finalize failed: {resp:?}"
    );
}

async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(
        resp.topics[0].error_code, 0,
        "topic create failed: {resp:?}"
    );
}

// ─── Upper processor ──────────────────────────────────────────────────────────

struct Upper;

#[async_trait::async_trait]
impl Processor<String, String, String, String> for Upper {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, String>,
        r: Record<String, String>,
    ) {
        ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
    }
}

// ─── fetch helpers ────────────────────────────────────────────────────────────

/// Resolve the `topic_id` for `topic_name` via a metadata refresh, then open a
/// dedicated `Connection` and poll `fetch_partition` in a loop until `want`
/// values have been collected from partition 0.  Returns the decoded UTF-8
/// values in arrival order.
async fn collect_output(
    admin: &Client,
    bootstrap: &str,
    topic_name: &str,
    want: usize,
) -> Vec<String> {
    let meta = admin.refresh_metadata().await.expect("metadata");
    let topic_id = meta
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic_name))
        .map_or_else(
            || panic!("{topic_name} not found in metadata"),
            |t| t.topic_id,
        );

    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .expect("resolve")
        .next()
        .expect("no addr");
    let conn = Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: "test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    let mut collected: Vec<String> = Vec::new();
    let mut next_offset: i64 = 0;

    loop {
        let records: Vec<FetchedRecord> = fetch_partition(
            &conn,
            topic_name,
            topic_id,
            0,
            next_offset,
            crabka_units::millis(500),
            crabka_units::mebibytes(1),
        )
        .await
        .unwrap_or_default();

        for r in &records {
            if let Some(val) = &r.value
                && let Ok(s) = std::str::from_utf8(val)
            {
                collected.push(s.to_string());
            }
            next_offset = r.offset + 1;
        }

        if collected.len() >= want {
            break;
        }

        tokio::task::yield_now().await;
    }

    collected
}

// ─── test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kafka_streams_processes_records_end_to_end() {
    // 1. Boot broker.
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "stream-in", 1).await;
    create_topic(&admin, "stream-out", 1).await;

    // 2. Produce 3 input records to stream-in.
    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    for (k, v) in [("k1", "hello"), ("k2", "world"), ("k3", "streams")] {
        drop(
            producer
                .send(crabka_client_producer::ProducerRecord {
                    topic: "stream-in".into(),
                    partition: Some(0),
                    key: Some(bytes::Bytes::copy_from_slice(k.as_bytes())),
                    value: Some(bytes::Bytes::copy_from_slice(v.as_bytes())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    // 3. Build and start the upper-case streams app.
    let mut topo = Topology::new();
    let src: NodeHandle<String, String> = topo.add_source("src", ["stream-in"]);
    let up = topo.add_processor("up", || Upper, [&src]);
    topo.add_sink("out", "stream-out", [&up]);
    let built = topo.build("stream-app").unwrap();

    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id("stream-app")
        .topology(built)
        .build()
        .await
        .unwrap();

    // 4. Poll stream-out until HELLO / WORLD / STREAMS appear (25 s timeout).
    let got = tokio::time::timeout(
        Duration::from_secs(25),
        collect_output(&admin, &bootstrap, "stream-out", 3),
    )
    .await
    .expect("streams produced output within 25 s");

    let mut got = got;
    got.sort();
    assert_eq!(
        got,
        vec![
            "HELLO".to_string(),
            "STREAMS".to_string(),
            "WORLD".to_string()
        ]
    );

    streams.close().await.unwrap();
    broker.shutdown().await;
}
