//! End-to-end traces ingest: distributor -> WAL -> block-builder.

mod support;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::check;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_blockstore::{BlockWriter, TraceIndex};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::Producer;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_traces::{
    SpanRecord, TRACES_WAL_TOPIC, blockbuilder,
    distributor::{DistributorState, KafkaSink, router},
};
use object_store::{ObjectStore, memory::InMemory};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData},
};
use prost::Message as _;
use tower::ServiceExt as _;

#[tokio::test]
async fn otlp_lands_as_span_block() {
    let proc = support::start().await;
    create_topic(&proc.client, TRACES_WAL_TOPIC, 4).await;

    let producer = Producer::builder()
        .bootstrap(proc.bootstrap.clone())
        .client_id("crabka-traces-roundtrip-producer")
        .build()
        .await
        .unwrap();
    let state = Arc::new(DistributorState::new(Arc::new(KafkaSink::new(Arc::new(
        producer,
    )))));

    let resp = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::from(otlp_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(resp.status() == StatusCode::OK);

    let mut consumer = Consumer::builder()
        .bootstrap(proc.bootstrap.clone())
        .client_id("crabka-traces-roundtrip-consumer")
        .group_id("crabka-traces-roundtrip")
        .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let records = poll_until_records(&mut consumer, 2).await;
    let decoded = records
        .iter()
        .filter_map(|record| record.value.as_deref())
        .map(SpanRecord::decode)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    check!(
        (
            decoded
                .iter()
                .map(|record| (record.tenant.as_str(), record.span.trace_id))
                .collect::<Vec<_>>(),
            records
                .iter()
                .map(|record| record.partition)
                .collect::<Vec<_>>(),
        ) == (
            vec![("tenant-a", [1; 16]), ("tenant-a", [1; 16])],
            vec![records[0].partition; 2],
        )
    );

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store);
    let mut index = TraceIndex::new();
    let offset_range = (
        records.iter().map(|record| record.offset).min().unwrap(),
        records.iter().map(|record| record.offset).max().unwrap(),
    );
    let metas = blockbuilder::build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        records[0].partition,
        &decoded,
        offset_range,
    )
    .await
    .unwrap();

    check!(
        metas
            .iter()
            .map(|meta| (meta.tenant.as_str(), meta.row_count))
            .collect::<Vec<_>>()
            == vec![("tenant-a", 2)]
    );
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 10_000)
            == vec![metas[0].object_key.clone()]
    );
}

async fn create_topic(client: &crabka_client_core::Client, name: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
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

async fn poll_until_records(
    consumer: &mut Consumer,
    expected: usize,
) -> Vec<crabka_client_consumer::ConsumerRecord> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut out = Vec::new();
    while Instant::now() < deadline {
        out.extend(consumer.poll(crabka_units::millis(250)).await.unwrap());
        if out.len() >= expected {
            return out;
        }
    }
    panic!(
        "timed out waiting for {expected} records, got {}",
        out.len()
    );
}

fn otlp_body() -> Vec<u8> {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "checkout".into(),
                            ),
                        ),
                    }),
                    ..KeyValue::default()
                }],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "root".into(),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 1_500,
                        ..OtlpSpan::default()
                    },
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![3; 8],
                        parent_span_id: vec![2; 8],
                        name: "child".into(),
                        start_time_unix_nano: 1_100,
                        end_time_unix_nano: 1_200,
                        ..OtlpSpan::default()
                    },
                ],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}
