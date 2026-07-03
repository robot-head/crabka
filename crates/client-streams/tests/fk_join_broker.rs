//! Broker integration test: KIP-213 foreign-key KTable-KTable join over a live
//! in-process broker.
//!
//! Mirrors `dsl_integration.rs` (the DSL counting + restart-restore broker test):
//! boots a single in-process broker (rf=1, no Docker), drives a real
//! `KafkaStreams` app, and reads the join output back off the sink topic via a
//! direct fetch.
//!
//! The FK-join topology is the most demanding DSL lowering exercised over a real
//! broker: it spans **two subtopologies** wired by **two internal repartition
//! topics** (subscription registration, keyed by foreign key; subscription
//! response, keyed by primary key) plus a subscription state store backed by its
//! own changelog. The broker's KIP-1071 group coordinator auto-creates and
//! copartitions those internal topics from the submitted topology, exactly as it
//! does for the simpler DSL count app.
//!
//! Coverage:
//!  - inner FK join: produce `a:(k1,"A")` + `b:(A,"X")` → join emits `k1="AX"`.
//!  - update propagation: a later `b:(A,"Y")` re-emits `k1="AY"`.
//!  - restart-restore: a fresh `KafkaStreams` instance on the same application id
//!    restores the subscription store + both table changelogs and resolves a new
//!    left record against the already-known foreign row without re-producing `b`.
//!
//! Known limitation (per the FK slice): the runtime has no null-valued
//! source-record path, so source-row tombstones (deleting an `a`/`b` row via a
//! null produce) are not exercised here.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, FetchedRecord, fetch_partition};
use crabka_client_streams::{KafkaStreams, StreamsBuilder, StringSerde};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};

// ─── broker helpers (mirrors dsl_integration.rs) ───────────────────────────────

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

// ─── FK-join topology ──────────────────────────────────────────────────────────

/// Build the inner foreign-key join topology:
/// `a` (primary key → foreign key string) FK-joins `b` (foreign key → value),
/// emitting `va ‖ vb` keyed by `a`'s primary key, streamed out to `fk-out`.
///
/// Both input tables are materialized **source** tables (`builder.table`) as the
/// FK join requires; the foreign-key extractor is identity on `a`'s value.
fn fk_join_topology(app_id: &str) -> crabka_client_streams::BuiltTopology {
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("fk-a", "fk-sa");
    let tb = b.table::<String, String>("fk-b", "fk-sb");
    ta.join_on_foreign_key(
        &tb,
        |va: &String| va.clone(),
        |va: &String, vb: &String| format!("{va}{vb}"),
        StringSerde,
    )
    .to_stream()
    .to("fk-out");
    // The builder refuses `build`/`build_optimized` while typed handles are still
    // live; the join result is consumed by `to_stream().to(...)`, but the two
    // source tables must be released explicitly (same as the FK exec tests).
    drop(ta);
    drop(tb);
    // `build_optimized` (optimization=all, the JVM default): the
    // `REUSE_KTABLE_SOURCE_TOPICS` pass reuses each `builder.table` source topic
    // (`fk-a` / `fk-b`) as its store's changelog. The runtime suppresses the
    // changelog write-back for such reuse-source stores (a store whose changelog
    // topic is one of the task's source topics is drained but not re-produced —
    // see `Graph::drain_changelogs`), so this no longer loops. Exercises the full
    // two-subtopology / two-repartition-hop FK topology under optimization.
    b.build_optimized(app_id).unwrap()
}

// ─── output collector (mirrors dsl_integration.rs) ─────────────────────────────

/// Poll `fk-out` partition 0 from offset 0 until the **latest** value observed
/// for `key` equals `want_value` (or panic via the caller's `timeout`). Returns
/// every `(key, value)` pair read, in arrival order.
///
/// FK joins are eventually-consistent: a single logical match can surface as the
/// same result emitted more than once (the subscription registration→response
/// round-trip re-evaluates the join), and updates land as a fresh changelog
/// record. So we assert on the **converged** latest value for the key rather
/// than on a specific offset, which makes the test robust to those legitimate
/// intermediate re-emissions.
async fn poll_until_latest(
    admin: &Client,
    bootstrap: &str,
    topic_name: &str,
    key: &str,
    want_value: &str,
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
            client_id: "fk-test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    let mut collected: Vec<(String, String)> = Vec::new();
    let mut next_offset = 0_i64;

    loop {
        let records: Vec<FetchedRecord> =
            fetch_partition(&conn, topic_name, topic_id, 0, next_offset, 500, 1 << 20)
                .await
                .unwrap_or_default();

        for r in &records {
            next_offset = r.offset + 1;
            let k = r
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
            collected.push((k, value));
        }

        // Converged once the most recent record for `key` carries `want_value`.
        if collected
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            == Some(want_value)
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    collected
}

/// Read every record currently on `topic_name` partition 0 (one fetch pass, no
/// polling). Used only to surface the topic's contents in a failure message.
async fn read_all(admin: &Client, bootstrap: &str, topic_name: &str) -> Vec<(String, String)> {
    let meta = admin.refresh_metadata().await.expect("metadata");
    let topic_id = meta
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic_name))
        .map_or_else(|| panic!("{topic_name} not found"), |t| t.topic_id);
    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .expect("resolve")
        .next()
        .expect("no addr");
    let conn = Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: "fk-test-dump".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");
    let mut out = Vec::new();
    let mut next = 0_i64;
    loop {
        let records = fetch_partition(&conn, topic_name, topic_id, 0, next, 500, 1 << 20)
            .await
            .unwrap_or_default();
        if records.is_empty() {
            break;
        }
        for r in &records {
            next = r.offset + 1;
            let k = r
                .key
                .as_ref()
                .and_then(|b| std::str::from_utf8(b).ok())
                .unwrap_or("")
                .to_string();
            let v = r
                .value
                .as_ref()
                .and_then(|b| std::str::from_utf8(b).ok())
                .unwrap_or("")
                .to_string();
            out.push((k, v));
        }
    }
    out
}

// ─── test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fk_join_resolves_and_restores_over_broker() {
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    // The two source tables + the sink topic. The FK join's two internal
    // repartition topics, the subscription-store changelog, and the two
    // `<app>-fk-s{a,b}-changelog` table changelogs are all auto-created (and
    // copartitioned) by the KIP-1071 coordinator from the submitted topology.
    create_topic(&admin, "fk-a", 1).await;
    create_topic(&admin, "fk-b", 1).await;
    create_topic(&admin, "fk-out", 1).await;

    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();

    // ── 1. Seed both tables: a:(k1 -> "A"), b:(A -> "X") ──────────────────────
    // fk_extractor is identity, so a's value "A" is the foreign key selecting
    // b's row keyed "A". The join should emit k1 -> "AX".
    produce(&producer, "fk-a", "k1", "A").await;
    produce(&producer, "fk-b", "A", "X").await;

    // ── 2. Start the FK-join KafkaStreams app ─────────────────────────────────
    let app_id = "fk-join-broker-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(fk_join_topology(app_id))
        .build()
        .await
        .unwrap();

    // ── 3. The join converges to k1 -> "AX" ──────────────────────────────────
    let got = tokio::time::timeout(
        Duration::from_secs(45),
        poll_until_latest(&admin, &bootstrap, "fk-out", "k1", "AX"),
    )
    .await
    .expect("inner FK join must converge k1 -> AX (a's fk 'A' resolves b's row 'X') within 45s");
    assert!(
        got.contains(&("k1".to_string(), "AX".to_string())),
        "expected k1 -> AX among the FK-join output; got {got:?}",
    );

    // ── 4. Update the foreign row: b:(A -> "Y") re-emits k1 -> "AY" ───────────
    produce(&producer, "fk-b", "A", "Y").await;

    let Ok(got2) = tokio::time::timeout(
        Duration::from_secs(45),
        poll_until_latest(&admin, &bootstrap, "fk-out", "k1", "AY"),
    )
    .await
    else {
        let dump = read_all(&admin, &bootstrap, "fk-out").await;
        panic!(
            "updating b:A from X to Y must re-emit k1 -> AY within 45s; \
             fk-out had: {dump:?}",
        );
    };
    assert!(
        got2.contains(&("k1".to_string(), "AY".to_string())),
        "expected k1 -> AY after the foreign row updated; got {got2:?}",
    );

    // ── 5. Close the first instance ───────────────────────────────────────────
    streams.close().await.unwrap();

    // ── 6. Restart-restore: a FRESH instance on the SAME application id ───────
    // Queue a NEW left record a:(k2 -> "A") before restart. After restoring the
    // subscription store + b's changelog, the new instance must resolve k2
    // against the already-known foreign row "A"->"Y" WITHOUT b being re-produced.
    produce(&producer, "fk-a", "k2", "A").await;

    let streams2 = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(fk_join_topology(app_id))
        .build()
        .await
        .unwrap();

    let got3 = tokio::time::timeout(
        Duration::from_secs(45),
        poll_until_latest(&admin, &bootstrap, "fk-out", "k2", "AY"),
    )
    .await
    .expect(
        "after restart-restore, k2 must resolve against the restored foreign row \
         (b:A->Y) and emit k2 -> AY (without b re-produced) within 45s",
    );
    assert!(
        got3.contains(&("k2".to_string(), "AY".to_string())),
        "expected k2 -> AY after restart-restore; got {got3:?}",
    );

    streams2.close().await.unwrap();
    broker.shutdown().await;
}
