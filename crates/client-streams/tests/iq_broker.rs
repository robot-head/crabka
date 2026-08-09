//! Broker integration test for Interactive Queries against a live
//! `KafkaStreams`.
//!
//! The test boots an in-process broker, runs the stateful counting topology, and
//! produces `["a","a","b"]`. It then reads the materialized `counts` KV store
//! back through the public `KafkaStreams::key_value_store` interactive-query
//! interface, which is the same path an out-of-topology caller uses. It also
//! covers the error surfaces `StoreNotFound` and `WrongStoreKind`.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_client_streams::{
    I64Serde, IqError, KafkaStreams, NodeHandle, Processor, ProcessorContext, Record,
    StreamsClientError, StringSerde, Topology,
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

// ─── Counter processor (identical to state_store_integration) ──────────────────

/// Counts the occurrences of each value and forwards `(value_as_key, count)`.
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

// ─── test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interactive_query_kv_store_over_broker() {
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

    // ── 2. Start the counting KafkaStreams app ────────────────────────────────
    let app_id = "iq-broker-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(counting_topology(app_id))
        .build()
        .await
        .unwrap();

    // ── 3. Poll the IQ interface until the store has materialized a→2 ─────────
    // The store builds asynchronously as records are processed, so retry the
    // actual store state (per project guidance: wait on store state, not a sleep).
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let counts = loop {
        if let Ok(view) = streams
            .key_value_store("counts", StringSerde, I64Serde)
            .await
            && view.get(&"a".to_string()).await.unwrap() == Some(2)
        {
            break view;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "counts store did not reach a→2 within 15s",
        );
        tokio::task::yield_now().await;
    };

    // ── 4. Assert the materialized read semantics ─────────────────────────────
    assert_eq!(counts.get(&"a".to_string()).await.unwrap(), Some(2));
    assert_eq!(counts.get(&"b".to_string()).await.unwrap(), Some(1));
    assert_eq!(counts.get(&"missing".to_string()).await.unwrap(), None);
    assert!(
        counts.approximate_num_entries().await.unwrap() >= 2,
        "approximate_num_entries should count at least the two keys",
    );

    // ── 5. Error surfaces ─────────────────────────────────────────────────────
    // Unknown store name → StoreNotFound (the views don't impl Debug, so map to
    // the error before asserting).
    let not_found = streams
        .key_value_store::<String, i64>("does-not-exist", StringSerde, I64Serde)
        .await
        .err();
    assert!(
        matches!(
            not_found,
            Some(StreamsClientError::InteractiveQuery(
                IqError::StoreNotFound(_)
            ))
        ),
        "querying an absent store must be StoreNotFound, got {not_found:?}",
    );

    // Wrong kind: `counts` is a KV store, queried as a window store.
    let wrong_kind = streams
        .window_store::<String, i64>("counts", StringSerde, I64Serde)
        .await
        .err();
    assert!(
        matches!(
            wrong_kind,
            Some(StreamsClientError::InteractiveQuery(
                IqError::WrongStoreKind { .. }
            ))
        ),
        "querying a KV store as a window store must be WrongStoreKind, got {wrong_kind:?}",
    );

    // ── 6. Clean shutdown ─────────────────────────────────────────────────────
    streams.close().await.unwrap();
    broker.shutdown().await;
}
