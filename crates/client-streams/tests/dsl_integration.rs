//! Broker integration test: DSL counting topology + restart-restore.
//!
//! Proves that a `StreamsBuilder`-based counting app (DSL path) works
//! end-to-end against a real broker and that a fresh `KafkaStreams` instance
//! restores its `counts` store from the changelog so that counts continue from
//! where the previous instance left off rather than resetting to zero.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{Client, Connection, ConnectionOptions, FetchedRecord, fetch_partition};
use crabka_client_streams::{I64Serde, KafkaStreams, StreamsBuilder, StringSerde};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};
use crabka_units::prelude::*;

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

// ─── DSL counting topology ────────────────────────────────────────────────────

/// Build the DSL counting topology:
/// `dsl-in` → `group_by_key` → `count` → `to_stream` → `dsl-out`
/// No repartition needed because `group_by_key` is used directly on the source
/// stream (key is not changed upstream).
fn dsl_counting_topology(app_id: &str) -> crabka_client_streams::BuiltTopology {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["dsl-in"])
        .group_by_key()
        .count("counts")
        .to_stream()
        .to("dsl-out");
    b.build_optimized(app_id).unwrap()
}

// ─── output collector ────────────────────────────────────────────────────────

/// Poll `dsl-out` partition 0 until `want` records arrive.
/// Returns `(key, i64_value)` pairs in arrival order.
async fn collect_output_keyed(
    admin: &Client,
    bootstrap: &str,
    topic_name: &str,
    want: usize,
    start_offset: i64,
) -> Vec<(String, i64)> {
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
            client_id: "test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    let mut collected: Vec<(String, i64)> = Vec::new();
    let mut next_offset = start_offset;

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
                .filter(|b| b.len() == 8)
                .map_or(0, |b| i64::from_be_bytes(b.as_ref().try_into().unwrap()));
            collected.push((key, value));
        }

        if collected.len() >= want {
            break;
        }

        tokio::task::yield_now().await;
    }

    collected
}

/// Open the local `counts` KV store for interactive queries, retrying while the
/// app is still joining/rebalancing (the store isn't assigned the instant
/// `build()` returns). Panics if it never becomes available within 30s.
async fn open_counts_store(
    streams: &KafkaStreams,
) -> crabka_client_streams::ReadOnlyKeyValueStore<String, i64> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match streams
                .key_value_store("counts", StringSerde, I64Serde)
                .await
            {
                Ok(store) => return store,
                // Not yet assigned / still joining: retry.
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("counts store became queryable within 30s")
}

/// Send one `key`=`value` record to `dsl-in` partition 0 and flush.
async fn produce_one(producer: &crabka_client_producer::Producer, val: &str) {
    drop(
        producer
            .send(crabka_client_producer::ProducerRecord {
                topic: "dsl-in".into(),
                partition: Some(0),
                key: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                ..Default::default()
            })
            .await,
    );
    producer.flush().await.unwrap();
}

// ─── tests ────────────────────────────────────────────────────────────────────

/// **Cache-OFF path** (`cache_max_bytes(0)`): the count store emits *per record*
/// and logs every update to the changelog immediately (JVM emit-on-update). With
/// caching disabled there is no buffering/dedup, so `a,a,b` yields three outputs
/// `a→1, a→2, b→1`. After restart the store restores from the changelog, so the
/// next `a` is `a→3` (durability guard for the original restore bug).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dsl_count_restart_restore_emit_on_update() {
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "dsl-in", 1).await;
    create_topic(&admin, "dsl-out", 1).await;
    // The broker auto-creates <app>-counts-changelog when the streams app first
    // writes to it; no explicit creation needed.

    // ── 1. Produce ["a","a","b"] to dsl-in ───────────────────────────────────
    // key = value so group_by_key().count() counts per value.
    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();

    for val in ["a", "a", "b"] {
        drop(
            producer
                .send(crabka_client_producer::ProducerRecord {
                    topic: "dsl-in".into(),
                    partition: Some(0),
                    key: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();

    // ── 2. Start counting KafkaStreams app (DSL topology) ─────────────────────
    // cache_max_bytes(0) disables the record cache so the count store emits
    // per-record (a→1, a→2, b→1) and logs each update to the changelog
    // immediately. With the default 10 MiB cache, materialized writes are
    // buffered and only emitted/changelog-logged on a cache flush — and the
    // flush-on-commit wiring lands in a later record-caching sub-task, so this
    // emit-on-update test pins caching off until then.
    let app_id = "dsl-count-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(dsl_counting_topology(app_id))
        .cache_max_bytes(ByteSize::ZERO)
        .build()
        .await
        .unwrap();

    // ── 3. Collect 3 output records from dsl-out ─────────────────────────────
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output_keyed(&admin, &bootstrap, "dsl-out", 3, 0),
    )
    .await
    .expect("DSL counting streams produced 3 output records within 30s");

    // a→1, a→2, b→1 (in key-order within each key)
    let a_counts: Vec<i64> = got
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| *v)
        .collect();
    let b_counts: Vec<i64> = got
        .iter()
        .filter(|(k, _)| k == "b")
        .map(|(_, v)| *v)
        .collect();
    assert_eq!(a_counts, vec![1, 2], "a counts must be [1, 2]; got {got:?}");
    assert_eq!(b_counts, vec![1], "b count must be [1]; got {got:?}");

    // ── 4. Close the first instance ───────────────────────────────────────────
    streams.close().await.unwrap();

    // ── 5. Start a FRESH instance with the SAME application_id ───────────────
    // Produce one more "a" to dsl-in BEFORE starting so it's queued.
    produce_one(&producer, "a").await;

    let streams2 = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(dsl_counting_topology(app_id))
        .cache_max_bytes(ByteSize::ZERO) // see step 2: emit-on-update / immediate changelog
        .build()
        .await
        .unwrap();

    // ── 6. Collect the 4th output record (must be a→3, NOT a→1) ──────────────
    // We already collected 3 records; start reading from offset 3.
    let got2 = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output_keyed(&admin, &bootstrap, "dsl-out", 1, 3),
    )
    .await
    .expect("restarted DSL streams produced output within 30s");

    let a_restart = got2
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| *v)
        .next();

    assert_eq!(
        a_restart,
        Some(3),
        "after restart-restore, 'a' count must be 3 (restore from changelog), \
         not 1 (cold start); got {got2:?}",
    );

    streams2.close().await.unwrap();
    broker.shutdown().await;
}

/// **Cache-ON path** (default 10 MiB record cache): the count store buffers its
/// writes and only emits downstream + logs the changelog on a *cache flush*
/// (commit tick or close). The semantics are **emit-on-commit, deduped**:
///
/// - `a,a,b` is processed in a single poll batch, buffering `counts: a→2, b→1`
///   in the cache (NO per-record `a→1` emit).
/// - On the next flush the cache emits the deduped `a→2`, `b→1` to `dsl-out`
///   AND writes the same to the changelog — exactly **2** records, vs the **3**
///   per-record updates the cache-off path emits.
/// - After restart (same `app_id`) + one more `a`: restore from the changelog →
///   `a` resumes at 2 → next count is `a→3`.
///
/// This is the real end-to-end proof of the record cache over a broker, and is
/// distinct from the cache-off variant which emits the 3 per-record updates.
///
/// **Determinism.** I do NOT rely on "only flush on close": `tokio::time::interval`
/// fires its first tick immediately, so even a 60 s `commit_interval` produces an
/// early commit-flush (verified empirically — the first commit tick lands ~0.5–3.5 s
/// in, NOT at 60 s). That is *correct* emit-on-commit behaviour, not a bug. What
/// makes the deduped 2-record emit deterministic is that all three records are
/// produced+flushed BEFORE the app starts, so a single poll batch fetches and
/// processes `a,a,b` back-to-back into the cache before ANY flush — no commit tick
/// can interleave between `a→1` and `a→2`. We use a 60 s `commit_interval` so at
/// most one commit-flush fires (the immediate first tick), and we drive the
/// "records processed" signal off the live `counts` store via interactive queries
/// (which read through the cache, seeing the buffered values) rather than a blind
/// sleep. We then assert the deduped output is EXACTLY `a→2, b→1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// one self-contained end-to-end scenario:
// produce → cache-buffer → deduped emit → restart → restore → re-emit.
async fn dsl_count_restart_restore_caching_on() {
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "dsl-in", 1).await;
    create_topic(&admin, "dsl-out", 1).await;

    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();

    // ── 1. Produce ["a","a","b"] to dsl-in ───────────────────────────────────
    for val in ["a", "a", "b"] {
        produce_one(&producer, val).await;
    }

    // ── 2. Start the app with the DEFAULT (10 MiB) cache (cache ON). A 60 s
    //       commit interval keeps flushes to at most one (the immediate first
    //       tick); the deduped emit is guaranteed by single-batch processing. ──
    let app_id = "dsl-count-cache-app";
    let streams = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(dsl_counting_topology(app_id))
        .cache_max_bytes(mebibytes(10)) // default; explicit for clarity (cache ON)
        .commit_interval(Duration::from_mins(1))
        .build()
        .await
        .unwrap();

    // ── 3. Wait until all 3 records are processed & buffered in the cache by
    //       polling the live `counts` store via IQ (a deterministic "processed"
    //       signal, not a blind sleep). The cached KV store reads cache-first,
    //       so we observe the buffered a→2, b→1. ──────────────────────────────
    let store = open_counts_store(&streams).await;

    let buffered = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let a = store.get(&"a".to_string()).await.unwrap();
            let b = store.get(&"b".to_string()).await.unwrap();
            if a == Some(2) && b == Some(1) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        buffered.is_ok(),
        "counts store should buffer a→2, b→1 within 30s (cache read-through)"
    );

    // ── 4. Close → ensures the cache is flushed (if the immediate commit tick
    //       hasn't already) → emits deduped a→2, b→1 + changelog. ──────────────
    streams.close().await.unwrap();

    // ── 5. Collect from dsl-out: EXACTLY 2 records, a→2 and b→1. This proves
    //       dedup + emit-on-commit (NOT the 3 per-record outputs of cache-off).
    let got = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output_keyed(&admin, &bootstrap, "dsl-out", 2, 0),
    )
    .await
    .expect("cache-on streams emitted 2 deduped output records within 30s");

    assert_eq!(
        got.len(),
        2,
        "cache-on flush must emit EXACTLY 2 deduped records (a→2, b→1), not the \
         3 per-record updates of the cache-off path; got {got:?}"
    );
    let a_counts: Vec<i64> = got
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| *v)
        .collect();
    let b_counts: Vec<i64> = got
        .iter()
        .filter(|(k, _)| k == "b")
        .map(|(_, v)| *v)
        .collect();
    assert_eq!(
        a_counts,
        vec![2],
        "cache-on: deduped 'a' emit must be exactly [2]; got {got:?}"
    );
    assert_eq!(
        b_counts,
        vec![1],
        "cache-on: deduped 'b' emit must be exactly [1]; got {got:?}"
    );

    // ── 6. Restart with the SAME app_id (default cache) + one more "a" → a→3.
    //       Cold start would be a→1; a→3 proves restore from the changelog that
    //       the close-flush wrote. ────────────────────────────────────────────
    produce_one(&producer, "a").await;

    let streams2 = KafkaStreams::builder()
        .bootstrap(&bootstrap)
        .application_id(app_id)
        .topology(dsl_counting_topology(app_id))
        .cache_max_bytes(mebibytes(10))
        .commit_interval(Duration::from_mins(1))
        .build()
        .await
        .unwrap();

    // Wait until the restored store reaches a→3 (buffered), then close to flush.
    let store2 = open_counts_store(&streams2).await;
    let restored = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if store2.get(&"a".to_string()).await.unwrap() == Some(3) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        restored.is_ok(),
        "after restart-restore, 'a' must reach 3 (restore from changelog), not 1 \
         (cold start)"
    );
    streams2.close().await.unwrap();

    // And the close-flush emits the restored a→3 to dsl-out (offset 2 onward).
    let got2 = tokio::time::timeout(
        Duration::from_secs(30),
        collect_output_keyed(&admin, &bootstrap, "dsl-out", 1, 2),
    )
    .await
    .expect("restarted cache-on streams produced output within 30s");
    let a_restart = got2
        .iter()
        .filter(|(k, _)| k == "a")
        .map(|(_, v)| *v)
        .next();
    assert_eq!(
        a_restart,
        Some(3),
        "after restart-restore, emitted 'a' count must be 3; got {got2:?}",
    );

    broker.shutdown().await;
}
