+++
title = "Streams and Data Formats"
description = "Build Rust stream-processing apps with crabka-client-streams: join a KIP-1071 Streams group, run a topology, and read and write Kafka topics with pluggable serdes."
weight = 10
template = "docs/page.html"
+++

`crabka-client-streams` is Crabka's Rust Streams client. It joins a KIP-1071
Streams rebalance group, runs a processing topology, and reads and writes Kafka
topics through pluggable serdes.

Use this page when you build stream-processing applications in Rust. Use
[Schema Registry Deployment](/docs/deploy/schema-registry/) when you need the
registry service that those schema-aware serdes use.

The client gives two processing models:

- **Row model.** The Processor API and the high-level DSL, `StreamsApp` and
  `streams_builder`, process one record at a time. Use `TopologyTestDriver` for
  broker-free tests.
- **Columnar model.** A `ColumnarTopology` has Polars `DataFrame` edges and does
  vectorized aggregation. Use `ColumnarTestDriver` for broker-free tests.

## Data formats

| Serde / codec | Rust type | Cargo feature | Use it for |
|---|---|---|---|
| `StringSerde` / `I64Serde` / `BytesSerde` | `String` / `i64` / `Bytes` | (built-in) | primitive keys/values |
| `SchemaSerde<T, JsonSerde<T>>` | any `serde` + `schemars::JsonSchema` | (built-in) | Confluent JSON Schema |
| `SchemaSerde<T, ProtobufSerde<T>>` | a prost `Message` | (built-in) | Confluent Protobuf (dynamic via `prost-reflect`) |
| `SchemaSerde<T, AvroSerde<T>>` | `apache_avro::AvroSchema` | (built-in) | Confluent Avro |
| `PolarsIpcSerde` | `polars::DataFrame` | `polars` | columnar values (Arrow IPC) |
| `ArrowIpcSerde` | `arrow::RecordBatch` | `arrow` | arrow-rs interchange |
| `ColumnarSerde<T>` | `columnar::Columnar` | `columnar` | zero-copy native columnar |

Schema serdes resolve schema IDs against a Confluent-compatible registry,
`crabka-schema-registry`. The columnar serdes are self-describing Arrow IPC.

## Getting started

Add the client and pick the columnar features you need:

```toml
[dependencies]
crabka-client-streams = { version = "0.3.7", features = ["polars", "arrow"] }
```

Define a type, then round-trip it through a schema serde. Any `serde` type that
also derives `schemars::JsonSchema` works as a JSON-Schema value:

<!-- snippet: client-streams/examples/format_json.rs#json-type -->
```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}
```
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_json.rs#json-roundtrip -->
```rust
let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
cache.seed_subject_id("orders.json-value", 1);
let serde = SchemaSerde::new(JsonSerde::<OrderEvent>::value(&cache, false));

let event = OrderEvent {
    order_id: "o-1".into(),
    user: "alice".into(),
    amount: 5.0,
    currency: "USD".into(),
    ts_ms: 1,
};
let bytes = serde.serialize("orders.json", &event);
let back: OrderEvent = serde.deserialize("orders.json", &bytes).unwrap();
```
<!-- /snippet -->

The high-level DSL wires types in with `DefaultSerde`:

<!-- snippet: client-streams/examples/format_dsl.rs#dsl-defaultserde -->
```rust
impl DefaultSerde for OrderEvent {
    type Serde = SchemaSerde<OrderEvent, JsonSerde<OrderEvent>>;
}
impl DefaultSerde for OrderProto {
    type Serde = SchemaSerde<OrderProto, ProtobufSerde<OrderProto>>;
}
```
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_dsl.rs#dsl-topology -->
```rust
let app = StreamsApp::builder()
    .bootstrap("127.0.0.1:9092")
    .application_id("orders-formats")
    .schema_registry("http://127.0.0.1:8081")
    .build();

let topology = app.streams_builder();
topology
    .stream::<String, OrderEvent>(["orders.json"])
    .map_values(|e: &OrderEvent| OrderProto {
        order_id: e.order_id.clone(),
        user: e.user.clone(),
        amount_cents: amount_cents(e.amount),
        currency: e.currency.to_uppercase(),
        ts_ms: e.ts_ms,
    })
    .to("orders.proto");
```
<!-- /snippet -->

### Columnar formats

For vectorized workloads, values are self-describing Arrow IPC. `ArrowIpcSerde`
round-trips an arrow-rs `RecordBatch`. `PolarsIpcSerde` does the same for a
Polars `DataFrame`:

<!-- snippet: client-streams/examples/format_arrow.rs#arrow-roundtrip -->
```rust
let schema = Arc::new(Schema::new(vec![
    Field::new("user", DataType::Utf8, false),
    Field::new("amount_cents", DataType::Int64, false),
]));
let batch = RecordBatch::try_new(
    schema,
    vec![
        Arc::new(StringArray::from(vec!["alice", "bob"])),
        Arc::new(Int64Array::from(vec![850_i64, 900])),
    ],
)
.unwrap();
let bytes = ArrowIpcSerde.serialize("orders.arrow", &batch);
let back = ArrowIpcSerde.deserialize("orders.arrow", &bytes).unwrap();
```
<!-- /snippet -->

## Worked pipeline: JSON → Protobuf → Arrow → Polars → summary Protobuf

The pipeline ingests order events as JSON and normalizes them to a canonical
Protobuf form. It then batches them into Arrow and aggregates them per user with
the Polars columnar engine. Last, it emits a Protobuf summary. Each topic hop
uses one format:

```text
orders.json   --JSON Schema-->  Stage A  --Protobuf-->  orders.proto
orders.proto  --Protobuf----->  Stage B  --Arrow IPC--> orders.arrow
orders.arrow  --Arrow IPC----->  Stage C (Polars group-by)
(agg rows)    --------------->  Stage D  --Protobuf-->  orders.summary
```

The full source is `crates/client-streams/examples/format_pipeline.rs`. It boots
an in-process broker and a Schema Registry, then asserts the result. CI runs it
as a test.

The harness is an in-process broker plus a Schema Registry on a real HTTP port:

<!-- snippet: client-streams/examples/format_pipeline.rs#setup -->
```rust
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
        runtime: RegistryRuntimeConfig::default(),
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
```
<!-- /snippet -->

The shared event type and the Arrow→Polars bridge codec:

<!-- snippet: client-streams/examples/format_pipeline.rs#types -->
```rust
/// Raw order, ingested as JSON (JSON-Schema serde).
#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}
```
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_pipeline.rs#arrow-codec -->
```rust
/// Source codec. Each Kafka record value is an Arrow-IPC `RecordBatch`, and this
/// codec decodes them into one Polars `DataFrame` that the columnar engine can
/// process. It bridges arrow-rs to polars explicitly, because the two use
/// different Arrow memory libraries.
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
```
<!-- /snippet -->

**Stage A: JSON → Protobuf**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-a-json-proto -->
```rust
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
```
<!-- /snippet -->

**Stage B: Protobuf → Arrow**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-b-proto-arrow -->
```rust
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
```
<!-- /snippet -->

**Stage C: Arrow → Polars, the columnar group-by**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-c-arrow-polars -->
```rust
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
```
<!-- /snippet -->

**Stage D: Polars → summary Protobuf**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-d-polars-proto -->
```rust
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
```
<!-- /snippet -->

**Verify the rollup**

<!-- snippet: client-streams/examples/format_pipeline.rs#assert -->
```rust
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
```
<!-- /snippet -->
