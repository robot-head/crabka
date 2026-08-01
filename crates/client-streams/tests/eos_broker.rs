//! End-to-end exactly-once (EOS v2) broker integration test.
//!
//! Boots an in-process Crabka broker (whose transaction coordinator is already
//! wired) and runs a *stateful* counting `KafkaStreams` app under
//! [`ProcessingGuarantee::ExactlyOnceV2`]. Proves three things end-to-end:
//!
//! 1. **Atomic, `read_committed` output.** Reading the output topic with
//!    `isolation_level = 1` (`READ_COMMITTED`) returns exactly the expected
//!    aggregation — no duplicates, no aborted/uncommitted data leaking below the
//!    last stable offset.
//! 2. **Source-offset atomicity.** The committed source offsets for the
//!    application-id group advance to the end of the input atomically with the
//!    committed output (via `OffsetFetch` for the streams group). The producer
//!    folds the consumed offsets into the SAME transaction as the output
//!    (`AddOffsetsToTxn` + `TxnOffsetCommit`); the broker materializes those
//!    transactional `__consumer_offsets` writes into the group's committed
//!    offsets when the COMMIT marker lands, so `OffsetFetch` returns the
//!    committed end offset (not `-1`).
//! 3. **True cross-restart exactly-once.** A fresh `KafkaStreams` with the SAME
//!    `application_id` and `ExactlyOnceV2` resumes from the committed SOURCE
//!    offsets — it does NOT re-read the committed input. After one more `"a"` is
//!    produced post-restart, the committed output grows to EXACTLY four records,
//!    `[a→1, a→2, b→1, a→3]`: the original three (committed input processed
//!    exactly once across the restart) plus the single new `a→3` (the restored
//!    store `a=2` advanced by the one genuinely new `"a"`). This proves the
//!    invariant in both directions — no committed input is re-read (a re-read
//!    would re-emit `a→3` from the 2nd `"a"` and `b→2` at higher offsets, failing
//!    the exact-set assertion), AND the restarted instance correctly fetches,
//!    processes, and commits genuinely new input. This is the invariant the
//!    broker fix unlocks: pre-fix the source offset read back as `-1`, so the
//!    restarted consumer reset to `earliest`, re-read the input, and re-emitted
//!    committed records (double-counting); post-fix it resumes from the
//!    materialized offset and processes only the new record.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::{
    Client, Connection, ConnectionOptions, DEFAULT_FETCH_RESPONSE_MAX, FetchedRecord,
    IsolatedFetch, fetch_partition_with_isolation,
};
use crabka_client_streams::{
    I64Serde, KafkaStreams, NodeHandle, ProcessingGuarantee, Processor, ProcessorContext, Record,
    StringSerde, Topology,
};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    offset_fetch_request::{
        OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
        OffsetFetchRequestTopics,
    },
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

const IN_TOPIC: &str = "in";
const OUT_TOPIC: &str = "out";
const APP_ID: &str = "eos-count-app";
/// Kafka `Fetch.isolation_level` for `READ_COMMITTED`.
const READ_COMMITTED: i8 = 1;

// ─── broker helpers (mirrored from runtime/state_store_integration.rs) ────────

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

async fn produce(producer: &crabka_client_producer::Producer, vals: &[&str]) {
    for val in vals {
        drop(
            producer
                .send(crabka_client_producer::ProducerRecord {
                    topic: IN_TOPIC.into(),
                    partition: Some(0),
                    key: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    value: Some(bytes::Bytes::copy_from_slice(val.as_bytes())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();
}

// ─── stateful counting topology (mirrors state_store_integration.rs) ──────────

/// Counts per-value occurrences and forwards `(value_as_key, count)`.
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
    let src: NodeHandle<String, String> = topo.add_source("src", [IN_TOPIC]);
    let c = topo.add_processor("c", || Counter, [&src]);
    topo.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
    topo.add_sink("out", OUT_TOPIC, [&c]);
    topo.build(app_id).unwrap()
}

async fn eos_streams(bootstrap: &str) -> KafkaStreams {
    KafkaStreams::builder()
        .bootstrap(bootstrap)
        .application_id(APP_ID)
        .topology(counting_topology(APP_ID))
        .processing_guarantee(ProcessingGuarantee::ExactlyOnceV2)
        .build()
        .await
        .unwrap()
}

// ─── read_committed output collector ──────────────────────────────────────────

/// Poll `OUT_TOPIC` partition 0 with **`READ_COMMITTED`** isolation until `want`
/// records are visible (i.e. committed below the last stable offset). Returns
/// `(key, i64_value)` pairs in arrival order. Records from aborted transactions
/// are never returned by a `READ_COMMITTED` fetch.
async fn collect_committed(
    admin: &Client,
    bootstrap: &str,
    want: usize,
    start_offset: i64,
) -> Vec<(String, i64)> {
    let meta = admin.refresh_metadata().await.expect("metadata");
    let topic_id = meta
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(OUT_TOPIC))
        .map_or_else(
            || panic!("{OUT_TOPIC} not found in metadata"),
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
            client_id: "eos-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    let mut collected: Vec<(String, i64)> = Vec::new();
    let mut next_offset = start_offset;

    loop {
        let records: Vec<FetchedRecord> = fetch_partition_with_isolation(
            &conn,
            IsolatedFetch {
                topic: OUT_TOPIC,
                topic_id,
                partition: 0,
                fetch_offset: next_offset,
                max_wait: crabka_units::millis(500),
                max: DEFAULT_FETCH_RESPONSE_MAX,
                partition_max: crabka_units::mebibytes(1),
                fetch_min: crabka_client_core::FetchMinBytes::default(),
                isolation_level: READ_COMMITTED,
            },
        )
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

        // real-time wait (not a progress poll): left per the conservative directive for
        // this known-flaky EOS-restart test — each iteration is a full broker RPC
        // round-trip (READ_COMMITTED fetch), and busy-polling could perturb the restart
        // race timing. Deliberately kept as a real sleep.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    collected
}

/// Fetch the committed source offset for `(IN_TOPIC, 0)` from the streams
/// application-id group via `OffsetFetch` (v8+ `groups[]` + `topic_id` shape,
/// mirroring the runtime's own `BrokerOffsetStore`). Returns `None` if no offset
/// is committed yet.
async fn committed_source_offset(admin: &Client) -> Option<i64> {
    let meta = admin.refresh_metadata().await.expect("metadata");
    let topic_id = meta
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(IN_TOPIC))
        .map_or_else(
            || panic!("{IN_TOPIC} not found in metadata"),
            |t| t.topic_id,
        );

    let resp = admin
        .send(OffsetFetchRequest {
            // Legacy fields (v0-7): kept for version-negotiation fallback.
            group_id: APP_ID.to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: IN_TOPIC.to_string(),
                partition_indexes: vec![0],
                ..Default::default()
            }]),
            // v8+ groups[] shape (carries topic_id for v10).
            groups: vec![OffsetFetchRequestGroup {
                group_id: APP_ID.to_string(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: IN_TOPIC.to_string(),
                    topic_id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetFetch");

    // v8+ response: data lives in groups[].topics[].partitions[].
    for g in &resp.groups {
        for t in &g.topics {
            for p in &t.partitions {
                if p.partition_index == 0 {
                    return if p.committed_offset < 0 {
                        None
                    } else {
                        Some(p.committed_offset)
                    };
                }
            }
        }
    }
    // v0-7 fallback: data in top-level topics[].
    for t in &resp.topics {
        for p in &t.partitions {
            if p.partition_index == 0 {
                return if p.committed_offset < 0 {
                    None
                } else {
                    Some(p.committed_offset)
                };
            }
        }
    }
    None
}

/// Poll [`committed_source_offset`] until it is present, returning the committed
/// offset. The transactional offset is materialized into the group's committed
/// offsets at the COMMIT marker, which lands just after the committed output
/// becomes visible — so a short poll bridges that window without flaking.
async fn await_committed_source_offset(admin: &Client) -> i64 {
    loop {
        if let Some(off) = committed_source_offset(admin).await {
            return off;
        }
        // real-time wait (not a progress poll): left per the conservative directive for
        // this known-flaky EOS-restart test — each iteration is a full broker RPC
        // round-trip (OffsetFetch), and busy-polling could perturb the restart race
        // timing. Deliberately kept as a real sleep.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ─── test ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eos_v2_atomic_output_and_restart_resume() {
    // 1. Boot broker, finalize streams.version, create in (1p,rf1) + out.
    let (broker, bootstrap, _dir) = boot().await;
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("admin")
        .build()
        .await
        .unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, IN_TOPIC, 1).await;
    create_topic(&admin, OUT_TOPIC, 1).await;

    // 2. Produce ["a","a","b"] to `in`.
    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    produce(&producer, &["a", "a", "b"]).await;

    // 3. Start the stateful counting app under EXACTLY_ONCE_V2.
    let streams = eos_streams(&bootstrap).await;

    // 4. Read `out` with READ_COMMITTED until 3 committed records are visible.
    let got = tokio::time::timeout(
        Duration::from_secs(40),
        collect_committed(&admin, &bootstrap, 3, 0),
    )
    .await
    .expect("EOS streams committed 3 output records within 40s");

    // Expected committed aggregation: a→1, a→2, b→1 (no duplicates, no aborted
    // data). With EOS-v2 the only records below the LSO must be these three.
    assert_eq!(
        got.len(),
        3,
        "exactly 3 committed records expected (no duplicates/aborted leakage); got {got:?}",
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
        vec![1, 2],
        "committed 'a' counts must be [1,2]; got {got:?}"
    );
    assert_eq!(
        b_counts,
        vec![1],
        "committed 'b' count must be [1]; got {got:?}"
    );

    // 4b. Source-offset atomicity. The streams runtime folds the consumed source
    // offsets into the SAME transaction as the output (`AddOffsetsToTxn` +
    // `TxnOffsetCommit`), and the broker now materializes those transactional
    // `__consumer_offsets` writes into the group's committed offsets when the
    // COMMIT marker lands. So `OffsetFetch` for the application-id group must
    // return the committed END offset of the input (3 records consumed → next
    // offset 3) — NOT `-1`. Poll briefly: the marker materialization completes
    // just after the committed output becomes visible.
    let source_off = tokio::time::timeout(
        Duration::from_secs(20),
        await_committed_source_offset(&admin),
    )
    .await
    .expect("committed source offset surfaced within 20s");
    assert_ne!(
        source_off, -1,
        "committed source offset must be surfaced via OffsetFetch (txn offsets \
         materialized on COMMIT), not -1",
    );
    assert_eq!(
        source_off, 3,
        "committed source offset must equal the consumed count (3 input records \
         → next offset 3); got {source_off}",
    );

    // 5. True cross-restart EOS. Close the first instance, produce one more "a",
    // start a FRESH instance with the SAME application_id under EXACTLY_ONCE_V2.
    // The new instance resumes from the committed SOURCE offset (3, surfaced
    // above) and therefore does NOT re-read the original ["a","a","b"]. It then
    // processes ONLY the single new "a" at input offset 3, which — with the
    // restored store (a→2, b→1) — emits exactly `a→3`. So the committed output
    // must grow to EXACTLY four records: the original three plus the one new
    // `a→3`, proving BOTH no-reprocessing (committed input processed once across
    // the restart) AND that the restarted instance correctly fetches/processes/
    // commits genuinely new input.
    streams.close().await.unwrap();
    produce(&producer, &["a"]).await;

    let streams2 = eos_streams(&bootstrap).await;

    // Collect the FULL committed output from offset 0 (READ_COMMITTED) until the
    // 4th committed record appears. Reading from 0 is robust to the EOS control
    // (COMMIT-marker) batches that occupy output offsets between data records, so
    // we don't depend on the exact offset the new record lands at. Wait with the
    // same generous 40s budget as the within-run wait: the restarted instance has
    // to (re)join the streams group, get assigned, restore its store from the
    // changelog, seek to the committed source offset, then fetch + process +
    // commit the one new record — several round-trips before `a→3` is committed.
    let after_restart = tokio::time::timeout(
        Duration::from_secs(40),
        collect_committed(&admin, &bootstrap, 4, 0),
    )
    .await
    .expect(
        "restarted EOS streams must commit the 4th output record (a→3 from the new \
         input) within 40s",
    );

    // The committed output must be EXACTLY the original three records PLUS the
    // single new `a→3` — no more, no less. This is the true cross-restart
    // exactly-once invariant in both directions:
    //   * No reprocessing: because the restarted instance resumed from the
    //     committed SOURCE offset (3, surfaced above via the broker's txn-offset
    //     materialization), it did NOT re-read the original ["a","a","b"]. A
    //     re-read would re-emit a→3 (from the restored store) and b→2 at HIGHER
    //     output offsets — so a 5th/6th record, or an `a→3` arriving from the
    //     re-read 2nd "a" rather than the genuinely new "a", would fail this
    //     exact-set assertion. Pre-broker-fix the source offset read back as -1
    //     and the consumer reset to `earliest`, double-counting.
    //   * New input IS processed: the lone new "a" at input offset 3 advances the
    //     restored count a=2 → 3 and is committed as `a→3`.
    assert_eq!(
        after_restart,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 1),
            ("a".to_string(), 3),
        ],
        "after EOS restart the committed output must be EXACTLY [a→1, a→2, b→1, a→3]: \
         the original three (committed input processed exactly once across the \
         restart) plus the single new `a→3` (restored store a=2 + one new 'a'); \
         got {after_restart:?}",
    );

    streams2.close().await.unwrap();
    broker.shutdown().await;
}
