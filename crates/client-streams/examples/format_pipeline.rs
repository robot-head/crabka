//! End-to-end multi-format Streams pipeline, self-contained and self-asserting:
//! JSON -> Protobuf -> Arrow -> columnar Polars -> summary Protobuf, against an
//! in-process broker + in-process Schema Registry (no external services).
//!
//! Run: `cargo run -p crabka-client-streams --example format_pipeline --features polars,arrow`

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use ::arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_client_streams::{
    SchemaSerde,
    columnar::{
        serde::{arrow::ArrowIpcSerde, polars::PolarsIpcSerde},
        topology::{
            ColumnarTopology,
            codec::{BatchCodec, BatchError, BlobCodec, ConsumedRecord, ProduceRecord},
            operator::BuiltinOp,
        },
    },
    processor::serde::{Serde, SerdeRole},
};
use crabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    kafkastore::KafkaStore,
    rest::{self, AppState},
};
use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    format::{json::JsonSerde, protobuf::ProtobufSerde},
};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

// docs:begin types
/// Raw order, ingested as JSON (JSON-Schema serde).
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}
// docs:end types

pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] = include_bytes!("gen/file_descriptor_set.bin");
mod orders {
    include!("gen/orders.rs");
}
use orders::{OrderProto, OrderSummary};

// docs:begin arrow-codec
/// Source codec: each Kafka record value is an Arrow-IPC `RecordBatch`; decode
/// them into one Polars `DataFrame` the columnar engine can process. Bridges
/// arrow-rs -> polars explicitly (different Arrow memory libraries).
struct ArrowBlobCodec;

impl BatchCodec for ArrowBlobCodec {
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError> {
        let mut users: Vec<String> = Vec::new();
        let mut cents: Vec<i64> = Vec::new();
        for (i, rec) in records.iter().enumerate() {
            let batch = ArrowIpcSerde
                .deserialize("", &rec.value)
                .map_err(|e| BatchError(format!("arrow decode rec {i}: {e}")))?;
            let user_col = batch
                .column_by_name("user")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| BatchError("missing user column".into()))?;
            let cent_col = batch
                .column_by_name("amount_cents")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| BatchError("missing amount_cents column".into()))?;
            for row in 0..batch.num_rows() {
                users.push(user_col.value(row).to_string());
                cents.push(cent_col.value(row));
            }
        }
        df!("user" => users, "amount_cents" => cents).map_err(|e| BatchError(e.to_string()))
    }

    fn encode(&self, _df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError> {
        Err(BatchError("ArrowBlobCodec is source-only".into()))
    }
}
// docs:end arrow-codec

struct Boot {
    _broker: BrokerHandle,
    bootstrap: String,
    registry_url: String,
    cancel: CancellationToken,
    _dir: tempfile::TempDir,
}

// docs:begin setup
async fn boot() -> Boot {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();

    // In-process Schema Registry over a real HTTP port.
    let cancel = CancellationToken::new();
    let cfg = RegistryConfig {
        bootstrap: bootstrap.clone(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: "format-pipeline-sr".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        security: SecurityConfig::default(),
    };
    let store = KafkaStore::start(&cfg, cancel.clone())
        .await
        .expect("sr start");
    let app = rest::router(AppState { store });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sr");
    let sr_addr = listener.local_addr().expect("sr addr");
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        let _ = rest::serve::serve_http(listener, app, serve_cancel).await;
    });

    Boot {
        _broker: broker,
        bootstrap,
        registry_url: format!("http://{sr_addr}"),
        cancel,
        _dir: dir,
    }
}
// docs:end setup

async fn send_record(producer: &Producer, topic: &str, value: Bytes) {
    producer
        .send(ProducerRecord {
            topic: topic.into(),
            value: Some(value),
            ..Default::default()
        })
        .await
        .await
        .expect("send recv")
        .expect("send ack");
}

/// Poll a fresh consumer group until `want` records arrive (bounded retries).
async fn drain(bootstrap: &str, topic: &str, group: &str, want: usize) -> Vec<Bytes> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("consumer build");
    let mut out = Vec::new();
    for _ in 0..60 {
        if out.len() >= want {
            break;
        }
        let recs = consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll");
        for r in recs {
            if let Some(v) = r.value {
                out.push(v);
            }
        }
    }
    assert!(
        out.len() >= want,
        "drain {topic}: got {} want {want}",
        out.len()
    );
    out
}

fn extract_str(col: &Column, i: usize) -> String {
    col.str()
        .expect("utf8 column")
        .get(i)
        .unwrap_or("")
        .to_string()
}
fn extract_i64(col: &Column, i: usize) -> i64 {
    col.i64().expect("i64 column").get(i).unwrap_or(0)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let boot = boot().await;
    let bootstrap = boot.bootstrap.clone();

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin");
    for t in [
        "orders.json",
        "orders.proto",
        "orders.arrow",
        "orders.summary",
    ] {
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: t.into(),
                    partitions: 1,
                    replicas: 1,
                    configs: BTreeMap::new(),
                }],
                5_000,
            )
            .await
            .expect("create topic");
    }

    let cache = SchemaCache::new(
        RegistryClient::new(&boot.registry_url),
        CacheConfig::default(),
    );
    let json_serde = SchemaSerde::new(JsonSerde::<OrderEvent>::value(&cache, false));
    let proto_serde = SchemaSerde::new(ProtobufSerde::<OrderProto>::value(&cache));
    let summary_serde = SchemaSerde::new(ProtobufSerde::<OrderSummary>::value(&cache));

    // Intern each serde's subject for its topic, then resolve all ids against the
    // live registry in one pass (AutoRegister). The Streams serialize path is
    // infallible, so the id MUST be resolved before the first serialize.
    json_serde.prepare("orders.json", SerdeRole::Value);
    proto_serde.prepare("orders.proto", SerdeRole::Value);
    summary_serde.prepare("orders.summary", SerdeRole::Value);
    cache.prewarm().await.expect("registry prewarm");

    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .acks(Acks::All)
        .build()
        .await
        .expect("producer");

    // Seed orders.json (alice: 5.00 + 3.50; bob: 9.00).
    let events = vec![
        OrderEvent {
            order_id: "o1".into(),
            user: "alice".into(),
            amount: 5.00,
            currency: "usd".into(),
            ts_ms: 1,
        },
        OrderEvent {
            order_id: "o2".into(),
            user: "alice".into(),
            amount: 3.50,
            currency: "usd".into(),
            ts_ms: 2,
        },
        OrderEvent {
            order_id: "o3".into(),
            user: "bob".into(),
            amount: 9.00,
            currency: "usd".into(),
            ts_ms: 3,
        },
    ];
    for e in &events {
        let bytes = json_serde.serialize("orders.json", e);
        send_record(&producer, "orders.json", bytes).await;
    }
    producer.flush().await.expect("flush json");

    // docs:begin stage-a-json-proto
    // Stage A — JSON -> Protobuf: deserialize JSON, normalize, emit OrderProto.
    for v in drain(&bootstrap, "orders.json", "stage-a", events.len()).await {
        let ev: OrderEvent = json_serde
            .deserialize("orders.json", &v)
            .expect("json decode");
        let proto = OrderProto {
            order_id: ev.order_id,
            user: ev.user,
            amount_cents: (ev.amount * 100.0).round() as i64,
            currency: ev.currency.to_uppercase(),
            ts_ms: ev.ts_ms,
        };
        let bytes = proto_serde.serialize("orders.proto", &proto);
        send_record(&producer, "orders.proto", bytes).await;
    }
    producer.flush().await.expect("flush proto");
    // docs:end stage-a-json-proto

    // docs:begin stage-b-proto-arrow
    // Stage B — Protobuf -> Arrow: collect rows into one arrow-rs RecordBatch.
    let mut users = Vec::new();
    let mut cents = Vec::new();
    for v in drain(&bootstrap, "orders.proto", "stage-b", events.len()).await {
        let p: OrderProto = proto_serde
            .deserialize("orders.proto", &v)
            .expect("proto decode");
        users.push(p.user);
        cents.push(p.amount_cents);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("user", ArrowDataType::Utf8, false),
        Field::new("amount_cents", ArrowDataType::Int64, false),
    ]));
    let batch = ::arrow::array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(users)),
            Arc::new(Int64Array::from(cents)),
        ],
    )
    .expect("record batch");
    send_record(
        &producer,
        "orders.arrow",
        ArrowIpcSerde.serialize("orders.arrow", &batch),
    )
    .await;
    producer.flush().await.expect("flush arrow");
    // docs:end stage-b-proto-arrow

    // docs:begin stage-c-arrow-polars
    // Stage C — Arrow -> Polars: group-by-user sum + count in the columnar engine.
    let consumed: Vec<ConsumedRecord> = drain(&bootstrap, "orders.arrow", "stage-c", 1)
        .await
        .into_iter()
        .enumerate()
        .map(|(i, v)| ConsumedRecord {
            key: None,
            value: v,
            timestamp: 0,
            partition: 0,
            offset: i as i64,
        })
        .collect();

    let mut topo = ColumnarTopology::new();
    let src = topo.add_source("src", ["orders.arrow"], ArrowBlobCodec);
    let agg = topo.add_operator(
        "agg",
        BuiltinOp::GroupByAgg {
            keys: vec![col("user")],
            aggs: vec![
                col("amount_cents").sum().alias("total_cents"),
                col("amount_cents").count().alias("order_count"),
            ],
        },
        src,
    );
    topo.add_sink("out", "orders.summary.df", BlobCodec::default(), agg);
    let built = topo.build().expect("build columnar");
    let produced = built
        .run_batch("orders.arrow", &consumed)
        .expect("run_batch");
    // docs:end stage-c-arrow-polars

    // docs:begin stage-d-polars-proto
    // Stage D — Polars -> Protobuf: each aggregated row becomes an OrderSummary.
    for (_topic, rec) in produced {
        let df = PolarsIpcSerde
            .deserialize("orders.summary.df", &rec.value)
            .expect("polars decode");
        let user_col = df.column("user").expect("user");
        let total_col = df.column("total_cents").expect("total_cents");
        let count_col = df
            .column("order_count")
            .expect("order_count")
            .cast(&DataType::Int64)
            .expect("cast count");
        for i in 0..df.height() {
            let summary = OrderSummary {
                user: extract_str(user_col, i),
                total_cents: extract_i64(total_col, i),
                order_count: extract_i64(&count_col, i),
            };
            let bytes = summary_serde.serialize("orders.summary", &summary);
            send_record(&producer, "orders.summary", bytes).await;
        }
    }
    producer.flush().await.expect("flush summary");
    // docs:end stage-d-polars-proto

    // docs:begin assert
    // Verify the per-user rollup off the wire.
    let mut by_user = BTreeMap::new();
    for v in drain(&bootstrap, "orders.summary", "verify", 2).await {
        let s: OrderSummary = summary_serde
            .deserialize("orders.summary", &v)
            .expect("summary decode");
        by_user.insert(s.user.clone(), s);
    }
    let alice = by_user.get("alice").expect("alice summary");
    assert_eq!(alice.total_cents, 850, "alice total_cents");
    assert_eq!(alice.order_count, 2, "alice order_count");
    let bob = by_user.get("bob").expect("bob summary");
    assert_eq!(bob.total_cents, 900, "bob total_cents");
    assert_eq!(bob.order_count, 1, "bob order_count");
    // docs:end assert

    boot.cancel.cancel();
    println!("format_pipeline: OK");
}
