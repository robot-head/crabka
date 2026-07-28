//! End-to-end metrics ingest: `remote_write` v1 -> distributor -> broker WAL ->
//! compactor -> object-store block.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_blockstore::read_block;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::Producer;
use crabka_ids::{Offset, PartitionIndex};
use crabka_metrics::{
    CompactionPartitionOffset, MetricBlockKind, MetricsCompactorConfig, SamplePayload, WAL_TOPIC,
    WalRecord, compaction_partition_object_key,
    distributor::{DistributorState, KafkaSink, router},
    run_compactor_consumer_loop,
    wire::pb,
};
use crabka_units::prelude::*;
use object_store::{ObjectStore, memory::InMemory};
use prost::Message;
use tower::ServiceExt as _;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_write_v1_lands_as_block() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(tempdir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    create_metrics_wal_topic(&bootstrap).await;

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .expect("producer build");
    let state = Arc::new(DistributorState::new(Arc::new(KafkaSink::new(Arc::new(
        producer,
    )))));
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/push")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::from(remote_write_v1_body()))
                .expect("request"),
        )
        .await
        .expect("push response");

    assert!(response.status() == StatusCode::NO_CONTENT);

    let wal_record = inspect_wal_record(&bootstrap).await;
    let fingerprint = wal_record.series_fingerprint();
    check!(wal_record.tenant == "tenant-a");
    check!(
        wal_record
            .labels
            .iter()
            .any(|(name, value)| name == "__name__" && value == "up")
    );
    assert!(matches!(
        wal_record.payload,
        SamplePayload::Float {
            timestamp_ms: 100,
            value,
            start_timestamp_ms: None,
        } if (value - 1.0).abs() < f64::EPSILON
    ));

    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut config = MetricsCompactorConfig::new(bootstrap);
    config.group_id = "metrics-roundtrip-compactor".into();
    config.client_id = "metrics-roundtrip-compactor".into();
    config.poll_timeout = millis(100);
    let runtime = config
        .build_runtime(object_store.clone())
        .expect("compactor runtime");
    let mut consumer = config.build_consumer().await.expect("compactor consumer");
    let result = run_compactor_consumer_loop(
        &mut consumer,
        &runtime.block_writer,
        &runtime.index_sink,
        runtime.loop_config,
        |poll| poll.compacted_records > 0,
    )
    .await
    .expect("run compactor");

    check!(result.compacted_records == 1);
    check!(result.writes == 1);
    check!(
        result.committed_offsets
            == vec![CompactionPartitionOffset {
                partition: PartitionIndex(0),
                offset: Offset(1),
            }]
    );

    let block_key = compaction_partition_object_key(
        "tenant-a",
        MetricBlockKind::Float,
        PartitionIndex(0),
        0,
        0,
    );
    let batches = read_block(object_store, &block_key)
        .await
        .expect("read compacted block");
    let rows: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert!(rows == 1);

    let expected_manifest_key = block_key.replace(".parquet", ".index");
    let manifest = runtime
        .index_sink
        .read_manifest(&expected_manifest_key)
        .await
        .expect("read compaction manifest");
    check!(manifest.tenant == "tenant-a");
    check!(manifest.block_key == block_key);
    check!(manifest.row_count == 1);
    check!(fingerprint != 0);
}

async fn create_metrics_wal_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: WAL_TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            crabka_units::secs(5),
        )
        .await
        .expect("create metrics wal topic");
}

async fn inspect_wal_record(bootstrap: &str) -> WalRecord {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("metrics-roundtrip-inspect")
        .client_id("metrics-roundtrip-inspect")
        .subscribe([WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("inspect consumer");
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let records = consumer
            .poll(crabka_units::millis(250))
            .await
            .expect("poll inspect consumer");
        if let Some(record) = records.into_iter().find(|record| record.topic == WAL_TOPIC) {
            assert!(record.partition == 0);
            assert!(record.offset == 0);
            let value = record.value.expect("wal record value");
            return WalRecord::decode(&value).expect("decode wal record");
        }
    }
    panic!("timed out waiting for metrics WAL record");
}

fn remote_write_v1_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: vec![pb::v1::TimeSeries {
            labels: vec![pb::v1::Label {
                name: "__name__".into(),
                value: "up".into(),
            }],
            samples: vec![pb::v1::Sample {
                value: 1.0,
                timestamp: 100,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress")
}
