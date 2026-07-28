//! Broker integration test: stateful counting topology + restart-restore.
//!
//! Proves that a fresh `KafkaStreams` instance restores its `counts` store from
//! the changelog so that counts continue from where the previous instance left
//! off rather than resetting to zero.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, FetchedRecord, fetch_partition};
use crabka_client_streams::{
    I64Serde, KafkaStreams, NodeHandle, Processor, ProcessorContext, Record, StringSerde, Topology,
};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

// ─── broker helpers ───────────────────────────────────────────────────────────

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

// ─── Counter processor ────────────────────────────────────────────────────────

/// Counts per-value occurrences and forwards `(value_as_key, count)`.
struct Counter;

#[async_trait::async_trait]
impl Processor<String, String, String, i64> for Counter {
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, i64>,
        r: Record<String, String>,
    ) {
        let n = {
            let store = ctx.get_state_store::<String, i64>("counts").unwrap();
            let n = store.get(&r.value).await.unwrap_or(0) + 1;
            store.put(r.value.clone(), n).await;
            n
        };
        ctx.forward(Record::new(Some(r.value), n, r.timestamp));
    }
}

fn counting_topology(app_id: &str) -> crabka_client_streams::BuiltTopology {
    let mut topo = Topology::new();
    let src: NodeHandle<String, String> = topo.add_source("src", ["stream-in"]);
    let c = topo.add_processor("c", || Counter, [&src]);
    topo.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
    topo.add_sink("out", "stream-out", [&c]);
    topo.build(app_id).unwrap()
}

// ─── output collector ────────────────────────────────────────────────────────

/// Poll `stream-out` partition 0 until `want` records arrive.
/// Returns `(key, i64_value)` pairs in arrival order.
async fn collect_output_keyed(
    admin: &Client,
    bootstrap: &str,
    topic_name: &str,
    want: usize,
    start_offset: i64,
) -> Vec<(String, i64)> {
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

    let mut collected: Vec<(String, i64)> = Vec::new();
    let mut next_offset = start_offset;

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
            next_offset = r.offset + 1;
            let key = r
                .key
                .as_ref()
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(ToString::to_string)
                .unwrap_or_default();
            let value = r
                .value
                .as_ref()
                .filter(|b| b.len() == 8)
                .map_or(0, |b| i64::from_be_bytes(b.as_ref().try_into().unwrap()));
            collected.push((key, value));
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
async fn stateful_count_and_restart_restore() {
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

    // ── 1. Produce ["a","a","b"] to stream-in ─────────────────────────────────
    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();

    for val in ["a", "a", "b"] {
        drop(
            producer
                .send(crabka_client_producer::ProducerRecord {
                    topic: "stream-in".into(),
                    partition: Some(0),
                    key: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    // ── 2. Start counting KafkaStreams app ────────────────────────────────────
    let app_id = "count-restart-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(counting_topology(app_id))
        .build()
        .await
        .unwrap();

    // ── 3. Collect 3 output records from stream-out ───────────────────────────
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output_keyed(&admin, &bootstrap, "stream-out", 3, 0),
    )
    .await
    .expect("counting streams produced 3 output records within 30s");

    // a→1, a→2, b→1 (in key-order within each key)
    let a_counts: Vec<i64> = got
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| *v)
        .collect();
    let b_counts: Vec<i64> = got
        .iter()
        .filter(|(k, _)| k == "b")
        .map(|(_, v)| *v)
        .collect();
    assert_eq!(a_counts, vec![1, 2], "a counts must be [1, 2]; got {got:?}");
    assert_eq!(b_counts, vec![1], "b count must be [1]; got {got:?}");

    // ── 4. Close the first instance ───────────────────────────────────────────
    streams.close().await.unwrap();

    // ── 5. Start a FRESH instance with the SAME application_id ───────────────
    // Produce one more "a" to stream-in BEFORE starting so it's queued.
    drop(
        producer
            .send(crabka_client_producer::ProducerRecord {
                topic: "stream-in".into(),
                partition: Some(0),
                key: Some(bytes::Bytes::copy_from_slice(b"a")),
                value: Some(bytes::Bytes::copy_from_slice(b"a")),
                ..Default::default()
            })
            .await,
    );
    producer.flush().await.unwrap();

    let streams2 = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(counting_topology(app_id))
        .build()
        .await
        .unwrap();

    // ── 6. Collect the 4th output record (must be a→3, NOT a→1) ──────────────
    // We already collected 3 records; start reading from offset 3.
    let got2 = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output_keyed(&admin, &bootstrap, "stream-out", 1, 3),
    )
    .await
    .expect("restarted streams produced output within 30s");

    let a_restart = got2
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| *v)
        .next();

    assert_eq!(
        a_restart,
        Some(3),
        "after restart-restore, 'a' count must be 3 (restore from changelog), \
         not 1 (cold start); got {got2:?}",
    );

    streams2.close().await.unwrap();
    broker.shutdown().await;
}
