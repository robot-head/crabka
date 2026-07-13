//! Live-broker integration test for the columnar topology runtime bridge.
//!
//! Boots an in-process broker, seeds two IPC-`DataFrame` records to `in`, runs
//! [`run_partition_once`] through test-local `RecordFetcher`/`RecordProducer`
//! adapters built over `crabka_client_core` (fetch) and `crabka_client_producer`
//! (produce), then reads `out` and asserts the filtered row count plus the
//! advanced next-offset.
#![cfg(feature = "polars")]

use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, fetch_partition};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_client_streams::{
    StreamsClientError,
    columnar::{
        serde::polars::PolarsIpcSerde,
        topology::{ColumnarTopology, codec::BlobCodec, operator::BuiltinOp, run_partition_once},
    },
    processor::serde::Serde,
    runtime::io::{FetchBatch, FetchedRec, IsolationLevel, RecordFetcher, RecordProducer},
};
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    },
    primitives::uuid::Uuid as WireUuid,
};
use polars::prelude::*;

// ─── broker boot helpers (mirror tests/runtime_integration.rs) ────────────────

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

async fn topic_id(admin: &Client, name: &str) -> WireUuid {
    let meta = admin.refresh_metadata().await.expect("metadata");
    meta.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map_or_else(|| panic!("{name} not found in metadata"), |t| t.topic_id)
}

async fn open_conn(bootstrap: &str, client_id: &str) -> Connection {
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

// ─── test-local I/O adapters over client_core / client_producer ───────────────

/// Fetches one partition over a raw `Connection`, polling until at least one
/// record is available (so the seeded records are visible before processing).
struct BrokerFetchAdapter {
    conn: Connection,
    topic_id: WireUuid,
}

#[async_trait::async_trait]
impl RecordFetcher for BrokerFetchAdapter {
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        _isolation: IsolationLevel,
    ) -> Result<FetchBatch, StreamsClientError> {
        // Poll a few times so freshly-produced records become visible.
        for _ in 0..50 {
            let recs = fetch_partition(
                &self.conn,
                topic,
                self.topic_id,
                partition,
                offset,
                500,
                1 << 20,
            )
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
            if !recs.is_empty() {
                return Ok(FetchBatch {
                    records: recs
                        .into_iter()
                        .map(|r| FetchedRec {
                            offset: r.offset,
                            key: r.key,
                            value: r.value,
                            timestamp: r.timestamp,
                        })
                        .collect(),
                });
            }
            // real-time wait (not a progress poll): the enclosing `for _ in 0..50` is
            // the loop bound, so this sleep IS the retry time budget (≈5s), not a
            // cadence inside a deadline-guarded poll loop.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(FetchBatch::default())
    }
}

/// Produces over a real `crabka_client_producer::Producer`.
struct BrokerProduceAdapter {
    producer: Producer,
}

#[async_trait::async_trait]
impl RecordProducer for BrokerProduceAdapter {
    async fn send(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError> {
        // `send` returns a oneshot ack receiver; durability is the `flush`
        // barrier (mirrors tests/runtime_integration.rs), so drop the receiver.
        drop(
            self.producer
                .send(ProducerRecord {
                    topic: topic.into(),
                    partition: partition.or(Some(0)),
                    key,
                    value,
                    ..Default::default()
                })
                .await,
        );
        Ok(())
    }

    async fn flush(&self) -> Result<(), StreamsClientError> {
        self.producer
            .flush()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }
}

// ─── topology ─────────────────────────────────────────────────────────────────

fn topo() -> ColumnarTopology {
    let mut t = ColumnarTopology::new();
    let src = t.add_source("src", ["in"], BlobCodec::default());
    let op = t.add_operator("flt", BuiltinOp::Filter(col("amount").gt(lit(4))), src);
    t.add_sink("out", "out", BlobCodec::default(), op);
    t
}

// ─── test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn columnar_runtime_bridge_against_live_broker() {
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "in", 1).await;
    create_topic(&admin, "out", 1).await;

    // Seed two IPC-DataFrame records to `in` (each a one-row frame).
    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    for amount in [1_i64, 9] {
        let df = df!("amount" => [amount]).unwrap();
        let value = PolarsIpcSerde.serialize("", &df);
        drop(
            producer
                .send(ProducerRecord {
                    topic: "in".into(),
                    partition: Some(0),
                    key: None,
                    value: Some(value),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    // Build the bridge I/O adapters.
    let in_id = topic_id(&admin, "in").await;
    let fetcher = BrokerFetchAdapter {
        conn: open_conn(&bootstrap, "bridge-fetch").await,
        topic_id: in_id,
    };
    let bridge_producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    let bridge_out = BrokerProduceAdapter {
        producer: bridge_producer,
    };

    // Run one fetch→process→produce cycle.
    let t = topo();
    let next = run_partition_once(&t, &fetcher, &bridge_out, "in", 0, 0)
        .await
        .expect("run_partition_once");
    assert_eq!(next, 2, "offset advances past both seeded records");

    // Read `out` and assert exactly one filtered row survived (amount 9 > 4).
    let out_id = topic_id(&admin, "out").await;
    let reader = open_conn(&bootstrap, "out-reader").await;
    let mut total_rows = 0usize;
    let mut next_offset = 0i64;
    'poll: for _ in 0..50 {
        let recs = fetch_partition(&reader, "out", out_id, 0, next_offset, 500, 1 << 20)
            .await
            .unwrap_or_default();
        for r in &recs {
            if let Some(v) = &r.value {
                let df = PolarsIpcSerde.deserialize("", v).unwrap();
                total_rows += df.height();
            }
            next_offset = r.offset + 1;
        }
        if total_rows >= 1 {
            break 'poll;
        }
        // real-time wait (not a progress poll): the enclosing `for _ in 0..50` is the
        // loop bound, so this sleep IS the retry time budget (≈5s), not a cadence
        // inside a deadline-guarded poll loop.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(total_rows, 1, "only amount=9 passes the amount>4 filter");

    broker.shutdown().await;
}
