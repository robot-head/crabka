#![cfg(feature = "roundtrip")]

//! End-to-end round-trip differential test for the Kafka FDW.
//!
//! Everything runs in one test process — no spawned binaries:
//!
//! 1. an in-process crabka broker + schema registry ([`harness::KafkaStack`]),
//! 2. an Avro schema registered + 3 known records produced to `orders`,
//! 3. an `crabka_pgexec::SqlEngine` with [`crabka_gres_fdw::KafkaFdw`] registered, served
//!    over pgwire on an ephemeral port,
//! 4. a `tokio-postgres` client that runs `CREATE SERVER` + `CREATE USER
//!    MAPPING` + `IMPORT FOREIGN SCHEMA`, then `SELECT`s the rows back.
//!
//! Assertions: the projected values + envelope offsets match what was produced;
//! offset pushdown (`WHERE _partition = 0 AND _offset >= 1`) returns the
//! expected subset; and a topic produced as raw bytes (no registry subject)
//! comes back as `bytea` via the raw-fallback path.

mod harness;

use std::sync::Arc;

use bytes::Bytes;
use crabka_client_producer::Header;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use crabka_schema_registry::{ids::SchemaVersion, kafkastore::record::SchemaReference};
use harness::KafkaStack;
use prost_reflect::prost::Message as _;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

/// Avro schema for `orders`: `id` (int → int4) + `total` (double → float8).
const ORDERS_SCHEMA: &str = r#"{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "total", "type": "double"}
  ]
}"#;

/// The three known records produced to `orders`, in produce (== offset) order.
const ORDERS: [(i32, f64); 3] = [(1, 10.5), (2, 20.0), (3, 30.25)];

const JSON_EVENTS_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "label": {"type": "string"}
  }
}"#;

const PROTO_EVENTS_SCHEMA: &str = r#"
syntax = "proto3";
package demo;

message Ignored {
  string skipped = 1;
}

message ProtoEvent {
  int64 id = 1;
  string label = 2;
  bool active = 3;
}
"#;

const MONEY_SCHEMA: &str = r#"
syntax = "proto3";
package money;

enum Currency {
  CURRENCY_UNSPECIFIED = 0;
  USD = 1;
}
"#;

const PROTO_ORDER_SCHEMA: &str = r#"
syntax = "proto3";
package demo;
import "money.proto";

message ProtoOrder {
  int64 id = 1;
  money.Currency currency = 2;
}
"#;

/// Serve a pgwire engine (with the real `KafkaFdw` registered) on an ephemeral
/// port and return that port.
async fn serve_engine() -> u16 {
    serve_engine_with_default_bootstrap(None).await
}

async fn serve_engine_with_default_bootstrap(default_bootstrap: Option<String>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut engine = SqlEngine::new();
    engine.set_foreign_scanner(Arc::new(crabka_gres_fdw::KafkaFdw::with_defaults(
        default_bootstrap,
    )));
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(engine),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

/// Connect a `tokio-postgres` client to the in-process pgwire server.
async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

/// Confluent-frame an Avro record body under `schema_id`.
fn avro_frame(schema: &apache_avro::Schema, schema_id: u32, id: i32, total: f64) -> Bytes {
    let mut rec = apache_avro::types::Record::new(schema).expect("avro record");
    rec.put("id", id);
    rec.put("total", total);
    let body = apache_avro::to_avro_datum(schema, rec).expect("encode avro datum");
    crabka_schema_serde::wire::encode(schema_id, &body)
}

fn protobuf_frame(schema_id: u32, id: i64, label: &str, active: bool) -> Bytes {
    let descriptor = crabka_gres_fdw::decode::build_message_descriptor(
        PROTO_EVENTS_SCHEMA,
        Some("demo.ProtoEvent"),
    )
    .expect("protobuf descriptor");
    let mut message = prost_reflect::DynamicMessage::new(descriptor);
    message
        .try_set_field_by_name("id", prost_reflect::Value::I64(id))
        .expect("set id");
    message
        .try_set_field_by_name("label", prost_reflect::Value::String(label.to_string()))
        .expect("set label");
    message
        .try_set_field_by_name("active", prost_reflect::Value::Bool(active))
        .expect("set active");
    crabka_schema_serde::wire::encode_protobuf(schema_id, &[1], &message.encode_to_vec())
}

fn protobuf_order_frame(schema_id: u32) -> Bytes {
    let descriptor = crabka_gres_fdw::decode::build_message_descriptor_with_references(
        PROTO_ORDER_SCHEMA,
        &std::collections::HashMap::from([("money.proto".to_string(), MONEY_SCHEMA.to_string())]),
        Some("demo.ProtoOrder"),
    )
    .expect("protobuf order descriptor");
    let mut message = prost_reflect::DynamicMessage::new(descriptor);
    message
        .try_set_field_by_name("id", prost_reflect::Value::I64(7))
        .expect("set id");
    message
        .try_set_field_by_name("currency", prost_reflect::Value::EnumNumber(1))
        .expect("set currency");
    crabka_schema_serde::wire::encode_protobuf(schema_id, &[0], &message.encode_to_vec())
}

/// The whole round trip. `multi_thread` is required: the FDW scan drives async
/// fetch via `block_in_place`, and the broker/registry tasks must run
/// concurrently with the test body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_fdw_roundtrip_avro_and_raw_fallback() {
    let stack = KafkaStack::start().await;

    // ── produce: avro "orders" + raw-bytes "events" + protobuf "proto_events"
    stack.create_topic("orders", 1).await;
    stack.create_topic("events", 1).await;
    stack.create_topic("json_events", 1).await;
    stack.create_topic("proto_events", 1).await;
    stack.create_topic("proto_orders", 1).await;

    let avro_schema = apache_avro::Schema::parse_str(ORDERS_SCHEMA).expect("parse orders schema");
    let schema_id = stack.register_avro("orders-value", ORDERS_SCHEMA).await;
    let json_schema_id = stack
        .register_json("json_events-value", JSON_EVENTS_SCHEMA)
        .await;
    let json_offset = stack
        .produce(
            "json_events",
            0,
            crabka_schema_serde::wire::encode(json_schema_id, br#"{"id":17,"label":"json row"}"#),
        )
        .await;
    assert_eq!(json_offset, 0, "single JSON record lands at offset 0");

    let mut produced_offsets = Vec::new();
    for (id, total) in ORDERS {
        let frame = avro_frame(&avro_schema, schema_id, id, total);
        let offset = stack.produce("orders", 0, frame).await;
        produced_offsets.push(offset);
    }
    // Offsets must be the monotonic 0,1,2 the assertions below rely on.
    assert_eq!(
        produced_offsets,
        vec![0, 1, 2],
        "produced offsets must be a dense monotonic 0..3"
    );

    stack
        .register_protobuf("money-value", MONEY_SCHEMA, None)
        .await;
    let protobuf_order_schema_id = stack
        .register_protobuf_with_references(
            "proto_orders-value",
            PROTO_ORDER_SCHEMA,
            Some("demo.ProtoOrder"),
            &[SchemaReference {
                name: "money.proto".to_string(),
                subject: "money-value".to_string(),
                version: SchemaVersion(1),
            }],
        )
        .await;
    let protobuf_order_offset = stack
        .produce(
            "proto_orders",
            0,
            protobuf_order_frame(protobuf_order_schema_id),
        )
        .await;
    assert_eq!(
        protobuf_order_offset, 0,
        "single protobuf order lands at offset 0"
    );

    // Raw-fallback topic: no registry subject, verbatim payload.
    let raw_payload = Bytes::from_static(b"raw-event-payload");
    let raw_offset = stack
        .produce_with_headers(
            "events",
            0,
            raw_payload.clone(),
            vec![
                Header {
                    key: "z".to_string(),
                    value: Some(Bytes::from_static(&[0x00, 0xff])),
                },
                Header {
                    key: "dup".to_string(),
                    value: Some(Bytes::from_static(b"one")),
                },
                Header {
                    key: "dup".to_string(),
                    value: None,
                },
            ],
        )
        .await;
    assert_eq!(raw_offset, 0, "single raw record lands at offset 0");

    let protobuf_schema_id = stack
        .register_protobuf(
            "proto_events-value",
            PROTO_EVENTS_SCHEMA,
            Some("demo.ProtoEvent"),
        )
        .await;
    let protobuf_offset = stack
        .produce(
            "proto_events",
            0,
            protobuf_frame(protobuf_schema_id, 42, "protobuf row", true),
        )
        .await;
    assert_eq!(
        protobuf_offset, 0,
        "single protobuf record lands at offset 0"
    );

    // ── pgwire + FDW DDL ────────────────────────────────────────────────────
    let client = connect(serve_engine().await).await;

    client
        .batch_execute(&format!(
            "CREATE SERVER s FOREIGN DATA WRAPPER crabka_gres_fdw \
             OPTIONS (bootstrap '{}', registry_url '{}')",
            stack.bootstrap(),
            stack.registry_url(),
        ))
        .await
        .expect("create server");
    client
        .batch_execute("CREATE USER MAPPING FOR PUBLIC SERVER s")
        .await
        .expect("create user mapping");

    // IMPORT FOREIGN SCHEMA materializes `orders` (avro → id int4, total float8),
    // `events` (raw → value bytea), and `proto_events` (protobuf typed cols).
    client
        .batch_execute(
            "IMPORT FOREIGN SCHEMA kafka LIMIT TO (orders, events, json_events, proto_events, proto_orders) FROM SERVER s",
        )
        .await
        .expect("import foreign schema");

    // ── assertion 1: full read matches produced values + monotonic offsets ──
    let rows = client
        .query(
            "SELECT id, total, _partition, _offset FROM orders ORDER BY _offset",
            &[],
        )
        .await
        .expect("select orders");
    assert_eq!(rows.len(), 3, "all three avro records returned");
    for (i, (expect_id, expect_total)) in ORDERS.iter().enumerate() {
        let id: i32 = rows[i].get("id");
        let total: f64 = rows[i].get("total");
        let partition: i32 = rows[i].get("_partition");
        let offset: i64 = rows[i].get("_offset");
        assert_eq!(id, *expect_id, "row {i} id");
        assert!(
            (total - *expect_total).abs() < f64::EPSILON,
            "row {i} total: got {total}, want {expect_total}"
        );
        assert_eq!(partition, 0, "row {i} _partition");
        assert_eq!(
            offset,
            i64::try_from(i).expect("test row index fits in i64"),
            "row {i} _offset monotonic"
        );
    }

    // ── assertion 2: offset pushdown returns exactly the expected subset ─────
    let pushed = client
        .query(
            "SELECT id, _offset FROM orders WHERE _partition = 0 AND _offset >= 1 ORDER BY _offset",
            &[],
        )
        .await
        .expect("select pushdown");
    assert_eq!(pushed.len(), 2, "_offset >= 1 keeps offsets 1 and 2");
    let pushed_ids: Vec<i32> = pushed.iter().map(|r| r.get::<_, i32>("id")).collect();
    assert_eq!(pushed_ids, vec![2, 3], "offsets 1,2 → ids 2,3");

    // ── assertion 3: raw-fallback topic comes back as bytea ──────────────────
    let raw_rows = client
        .query(
            "SELECT value, _offset, _headers FROM events ORDER BY _offset",
            &[],
        )
        .await
        .expect("select events (raw)");
    assert_eq!(raw_rows.len(), 1, "one raw record");
    let value: Vec<u8> = raw_rows[0].get("value");
    assert_eq!(
        value,
        raw_payload.to_vec(),
        "raw value round-trips verbatim as bytea"
    );
    let headers: String = raw_rows[0].get("_headers");
    assert_eq!(
        headers, "{\"dup\":\"\\\\x6f6e65\",\"dup\":null,\"z\":\"\\\\x00ff\"}",
        "_headers preserves duplicate keys, nulls, and binary values"
    );

    // ── assertion 4: empty headers stay `{}`, and header projection works with
    // offset pushdown without fetching value columns in SQL.
    let empty_header_rows = client
        .query(
            "SELECT _headers FROM orders WHERE _partition = 0 AND _offset = 1",
            &[],
        )
        .await
        .expect("select empty order headers");
    assert_eq!(empty_header_rows.len(), 1, "pushdown keeps one order row");
    let empty_headers: String = empty_header_rows[0].get("_headers");
    assert_eq!(empty_headers, "{}", "records without headers stay {{}}");

    let json_rows = client
        .query(
            "SELECT id, label, _offset FROM json_events ORDER BY _offset",
            &[],
        )
        .await
        .expect("select framed JSON events");
    assert_eq!(json_rows.len(), 1, "one JSON record");
    assert_eq!(json_rows[0].get::<_, i64>("id"), 17);
    assert_eq!(json_rows[0].get::<_, String>("label"), "json row");
    assert_eq!(json_rows[0].get::<_, i64>("_offset"), 0);

    let proto_rows = client
        .query(
            "SELECT id, label, active, _partition, _offset FROM proto_events ORDER BY _offset",
            &[],
        )
        .await
        .expect("select protobuf events");
    assert_eq!(proto_rows.len(), 1, "one protobuf record");
    let proto_id: i64 = proto_rows[0].get("id");
    let proto_label: String = proto_rows[0].get("label");
    let proto_active: bool = proto_rows[0].get("active");
    let proto_partition: i32 = proto_rows[0].get("_partition");
    let proto_offset: i64 = proto_rows[0].get("_offset");
    assert_eq!(proto_id, 42, "protobuf int64 projects to int8");
    assert_eq!(
        proto_label, "protobuf row",
        "protobuf string projects to text"
    );
    assert!(proto_active, "protobuf bool projects to bool");
    assert_eq!(proto_partition, 0, "protobuf _partition");
    assert_eq!(proto_offset, 0, "protobuf _offset");

    let proto_order_rows = client
        .query(
            "SELECT id, currency FROM proto_orders ORDER BY _offset",
            &[],
        )
        .await
        .expect("select protobuf orders with import");
    assert_eq!(proto_order_rows.len(), 1, "one protobuf order record");
    assert_eq!(proto_order_rows[0].get::<_, i64>("id"), 7);
    assert_eq!(proto_order_rows[0].get::<_, i32>("currency"), 1);

    // ── assertion 6: a scanner with an own-cluster default bootstrap lets the
    // server omit the explicit `bootstrap` option while preserving registry_url.
    let default_client =
        connect(serve_engine_with_default_bootstrap(Some(stack.bootstrap().to_string())).await)
            .await;
    default_client
        .batch_execute(&format!(
            "CREATE SERVER default_s FOREIGN DATA WRAPPER crabka_gres_fdw \
             OPTIONS (registry_url '{}')",
            stack.registry_url(),
        ))
        .await
        .expect("create default-backed server");
    default_client
        .batch_execute("IMPORT FOREIGN SCHEMA kafka LIMIT TO (events) FROM SERVER default_s")
        .await
        .expect("import via default bootstrap");
    let default_rows = default_client
        .query("SELECT value FROM events ORDER BY _offset", &[])
        .await
        .expect("select events via default bootstrap");
    assert_eq!(default_rows.len(), 1, "default-backed server sees events");
    let default_value: Vec<u8> = default_rows[0].get("value");
    assert_eq!(default_value, raw_payload.to_vec());

    // An explicit server bootstrap must win even when the substrate-derived
    // default is unusable. This drives the precedence rule through real broker
    // metadata/fetch I/O instead of proving it only in config units.
    let override_client =
        connect(serve_engine_with_default_bootstrap(Some("127.0.0.1:1".to_string())).await).await;
    override_client
        .batch_execute(&format!(
            "CREATE SERVER override_s FOREIGN DATA WRAPPER crabka_gres_fdw \
             OPTIONS (bootstrap '{}', registry_url '{}')",
            stack.bootstrap(),
            stack.registry_url(),
        ))
        .await
        .expect("create explicit override server");
    override_client
        .batch_execute("IMPORT FOREIGN SCHEMA kafka LIMIT TO (events) FROM SERVER override_s")
        .await
        .expect("import via explicit bootstrap override");
    let override_rows = override_client
        .query("SELECT value FROM events ORDER BY _offset", &[])
        .await
        .expect("select events via explicit bootstrap override");
    assert_eq!(override_rows.len(), 1, "explicit override sees events");
    assert_eq!(override_rows[0].get::<_, Vec<u8>>("value"), raw_payload);

    stack.shutdown().await;
}
