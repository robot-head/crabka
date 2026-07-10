//! Broker integration test: DSL `GlobalKTable` stream-globaltable join.
//!
//! Proves the full `GlobalKTable` runtime path end-to-end against a real broker:
//! the global consumer bootstraps a fully-replicated store from **every**
//! partition of the source topic (exercising the metadata-backed `partitions`
//! override) *before* tasks process, and a `KStream::join_global` then looks up
//! global values keyed off the stream record's value — including a key that lives
//! on a non-zero partition of the global topic.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, FetchedRecord, fetch_partition};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_client_streams::{GlobalKTable, KafkaStreams, StreamsBuilder};
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
    assert2::assert!(resp.error_code == 0);
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
    assert2::assert!(resp.topics[0].error_code == 0);
}

// ─── DSL global-table join topology ────────────────────────────────────────────

/// Build the stream-globaltable join topology:
/// `global` (2 partitions) → fully-replicated `global-store`;
/// `in` → `join_global` (lookup key = stream value) → `out`.
fn global_join_topology(app_id: &str) -> crabka_client_streams::BuiltTopology {
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> =
        b.global_table::<String, String>("global", "global-store");
    b.stream::<String, String>(["in"])
        .join_global(
            &g,
            // key-mapper: the stream VALUE is the global lookup key.
            |_k: &String, v: &String| v.clone(),
            // joiner: combine stream value and global value.
            |sv: &String, gv: &String| format!("{sv}-{gv}"),
        )
        .to("out");
    // Drop the GlobalKTable handle (it holds an Rc clone of the internal builder)
    // so `build` can `Rc::try_unwrap` the shared graph.
    drop(g);
    b.build(app_id).unwrap()
}

// ─── output collector ──────────────────────────────────────────────────────────

/// Poll `out` partition 0 until `want` string records arrive (or the outer
/// timeout fires). Returns `(key, value)` pairs in arrival order.
async fn collect_output(
    admin: &Client,
    bootstrap: &str,
    topic_name: &str,
    want: usize,
) -> Vec<(String, String)> {
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
            client_id: "global-test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    let mut collected: Vec<(String, String)> = Vec::new();
    let mut next_offset = 0i64;

    loop {
        let records: Vec<FetchedRecord> =
            fetch_partition(&conn, topic_name, topic_id, 0, next_offset, 500, 1 << 20)
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
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(ToString::to_string)
                .unwrap_or_default();
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
async fn global_table_join_reads_all_partitions() {
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("global-admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;

    // TWO partitions on the global topic exercises bootstrap-all-partitions + the
    // metadata-backed `partitions` override (default vec![0] would miss part. 1).
    create_topic(&admin, "global", 2).await;
    create_topic(&admin, "in", 1).await;
    create_topic(&admin, "out", 1).await;

    // ── 1. Populate the global table across BOTH partitions ───────────────────
    // Pin records to explicit partitions so we GUARANTEE coverage of partition 1.
    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    for (partition, key, val) in [(0i32, "a", "A"), (1i32, "b", "B")] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "global".into(),
                    partition: Some(partition),
                    key: Some(bytes::Bytes::copy_from_slice(key.as_bytes())),
                    value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    // ── 2. Start the global-table-join KafkaStreams app ───────────────────────
    let app_id = "global-join-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(global_join_topology(app_id))
        .build()
        .await
        .unwrap();

    // ── 3. Produce stream records whose VALUE is a global key ─────────────────
    // "k1"→"a" looks up partition-0 global value "A"; "k2"→"b" looks up the
    // partition-1 global value "B" — so a correct join must have bootstrapped
    // BOTH partitions before processing.
    for (key, val) in [("k1", "a"), ("k2", "b")] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "in".into(),
                    partition: Some(0),
                    key: Some(bytes::Bytes::copy_from_slice(key.as_bytes())),
                    value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    // ── 4. Collect 2 joined output records ────────────────────────────────────
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output(&admin, &bootstrap, "out", 2),
    )
    .await
    .expect("global-table-join streams produced 2 output records within 30s");

    // k1's value "a" joins partition-0 global "A" → "a-A".
    // k2's value "b" joins partition-1 global "B" → "b-B" (proves bootstrap read
    // partition 1, not just partition 0).
    assert2::assert!(got.contains(&("k1".to_string(), "a-A".to_string())));
    assert2::assert!(got.contains(&("k2".to_string(), "b-B".to_string())));

    streams.close().await.unwrap();
    broker.shutdown().await;
}
