//! Broker regression test for the `REUSE_KTABLE_SOURCE_TOPICS` changelog
//! write-back loop.
//!
//! Under `build_optimized()`, the `REUSE_KTABLE_SOURCE_TOPICS` optimizer points a
//! materialized `builder.table(topic, …)` store's changelog at its own source
//! `topic` (instead of a derived `<app>-<store>-changelog`). The runtime must NOT
//! re-produce that store's changelog entries — the source topic already IS the
//! log, so re-producing onto it feeds the source node an unbounded re-emit loop
//! (each stored row is written back, re-consumed, re-stored, re-written…).
//!
//! This test boots a single in-process broker (rf=1, no Docker), runs the
//! canonical reuse topology (`table → mapValues → toStream → to`) built with
//! `build_optimized()`, produces ONE record to the reused source topic, and
//! asserts that — after several commit/poll cycles — the source topic still holds
//! exactly that one record (no write-back loop) and the output topic has not
//! exploded. Before the fix, the source topic grew without bound from the single
//! input record.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, FetchedRecord, fetch_partition};
use crabka_client_streams::{KafkaStreams, StreamsBuilder};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

// ─── broker helpers (mirror fk_join_broker.rs / dsl_integration.rs) ─────────────

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
    assert_eq!(resp.error_code, 0, "streams.version finalize: {resp:?}");
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
    assert_eq!(resp.topics[0].error_code, 0, "topic create: {resp:?}");
}

async fn produce(producer: &crabka_client_producer::Producer, topic: &str, key: &str, val: &str) {
    drop(
        producer
            .send(crabka_client_producer::ProducerRecord {
                topic: topic.into(),
                partition: Some(0),
                key: Some(bytes::Bytes::copy_from_slice(key.as_bytes())),
                value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                ..Default::default()
            })
            .await,
    );
    producer.flush().await.unwrap();
}

async fn reader(bootstrap: &str, client_id: &str) -> Connection {
    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .expect("resolve")
        .next()
        .expect("no addr");
    Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: client_id.to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect")
}

async fn topic_id(admin: &Client, topic: &str) -> crabka_protocol::primitives::uuid::Uuid {
    let meta = admin.refresh_metadata().await.expect("metadata");
    meta.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map_or_else(|| panic!("{topic} not found"), |t| t.topic_id)
}

/// Count records on `topic` partition 0, capped at `CAP` so a write-back-loop
/// regression (which grows the topic without bound) fails the caller's assertion
/// cleanly instead of fetching forever.
async fn count_records(admin: &Client, bootstrap: &str, topic: &str) -> usize {
    const CAP: usize = 64; // legitimate counts here are tiny (1); >CAP ⇒ looping
    let tid = topic_id(admin, topic).await;
    let conn = reader(bootstrap, "reuse-count").await;
    let mut n = 0usize;
    let mut next = 0_i64;
    while n <= CAP {
        let records = fetch_partition(
            &conn,
            topic,
            tid,
            0,
            next,
            crabka_units::millis(500),
            crabka_units::mebibytes(1),
        )
        .await
        .unwrap_or_default();
        if records.is_empty() {
            break;
        }
        for r in &records {
            next = r.offset + 1;
            n += 1;
        }
    }
    n
}

/// Poll `out` partition 0 until the latest value for `key` equals `want`.
async fn poll_until_latest(admin: &Client, bootstrap: &str, topic: &str, key: &str, want: &str) {
    let tid = topic_id(admin, topic).await;
    let conn = reader(bootstrap, "reuse-poll").await;
    let mut latest: Option<String> = None;
    let mut next = 0_i64;
    loop {
        let records: Vec<FetchedRecord> = fetch_partition(
            &conn,
            topic,
            tid,
            0,
            next,
            crabka_units::millis(500),
            crabka_units::mebibytes(1),
        )
        .await
        .unwrap_or_default();
        for r in &records {
            next = r.offset + 1;
            if r.key.as_deref().and_then(|b| std::str::from_utf8(b).ok()) == Some(key) {
                latest = r
                    .value
                    .as_ref()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .map(ToString::to_string);
            }
        }
        if latest.as_deref() == Some(want) {
            return;
        }
        tokio::task::yield_now().await;
    }
}

// ─── topology: the canonical REUSE_KTABLE_SOURCE_TOPICS shape ───────────────────

/// `builder.table("rt-in", "rt-store").mapValues(id).toStream().to("rt-out")`,
/// built with `build_optimized` so the `rt-store` changelog reuses the `rt-in`
/// source topic (matches the `table_reuse` wire golden).
fn reuse_topology(app_id: &str) -> crabka_client_streams::BuiltTopology {
    let b = StreamsBuilder::new();
    b.table::<String, String>("rt-in", "rt-store")
        .map_values(|v: &String| v.clone())
        .to_stream()
        .to("rt-out");
    b.build_optimized(app_id).unwrap()
}

// ─── test ───────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reuse_source_topic_store_does_not_loop_changelog() {
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "rt-in", 1).await;
    create_topic(&admin, "rt-out", 1).await;

    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();

    // Exactly ONE record onto the reused source topic.
    produce(&producer, "rt-in", "k1", "V1").await;

    let app_id = "reuse-source-no-loop-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(reuse_topology(app_id))
        .build()
        .await
        .unwrap();

    // The table → toStream output converges to k1 -> V1.
    tokio::time::timeout(
        Duration::from_secs(45),
        poll_until_latest(&admin, &bootstrap, "rt-out", "k1", "V1"),
    )
    .await
    .expect("optimized table app must emit k1 -> V1 within 45s");

    // Give the app several commit/poll cycles for any write-back loop to manifest.
    // real-time wait (not a progress poll): settle-then-assert-ABSENCE — this window
    // lets any changelog write-back loop manifest before we assert it did NOT (in_count
    // == 1); there is no positive progress signal to poll for.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // CRITICAL: nothing must re-produce to the reused source topic. Pre-fix, the
    // changelog write-back loop re-emitted the stored row back onto `rt-in`,
    // growing it without bound from the single input record.
    let in_count = count_records(&admin, &bootstrap, "rt-in").await;
    assert_eq!(
        in_count, 1,
        "reused source topic 'rt-in' must hold exactly the 1 produced record; \
         found {in_count} (changelog write-back loop regression)",
    );

    // And the output must not have exploded (a couple of legitimate re-emissions
    // are tolerated; unbounded growth is the regression).
    let out_count = count_records(&admin, &bootstrap, "rt-out").await;
    assert!(
        out_count <= 4,
        "output topic 'rt-out' grew unexpectedly to {out_count} records \
         (changelog write-back loop regression)",
    );

    streams.close().await.unwrap();
    broker.shutdown().await;
}
