//! JVM golden-parity tests for Interactive-Query read semantics.
//!
//! The ground truth is `tests/testdata/iq/behavior.json`, captured from a real
//! JVM Kafka Streams 4.1 `TopologyTestDriver` run through
//! `jvm-capture/run.sh --iq`.
//!
//! Each test rebuilds the equivalent Rust DSL topology and replays the SAME
//! `records`, that is the same (value, timestamp) pairs, that the JVM read. It
//! then reads each materialized store back through the driver's IQ byte-layer
//! helpers and asserts parity with the captured reads: KV get, range, all, and
//! count, the window point fetch and range fetch, and the session fetch.
//!
//! One field is approximate: the KV `count`. On the JVM it is `RocksDB`'s
//! `approximateNumEntries()`, a write-count estimate, which gives `3` for
//! `a,a,b`. Our in-memory store gives the exact distinct-key count, `2`. The test
//! therefore asserts that our count lands in the contractually valid
//! `[distinct_keys, total_writes]` band, which brackets the JVM value, because
//! the documentation calls `approximateNumEntries` approximate. Every other field
//! is an exact match.

use crabka_client_streams::{
    Consumed, I64Serde, SessionWindows, StringSerde, TimeWindows, TopologyTestDriver,
    dsl::StreamsBuilder,
};
use crabka_units::prelude::*;
use serde_json::Value;

/// The captured JVM golden, parsed once per test.
fn golden() -> Value {
    let raw = include_str!("testdata/iq/behavior.json");
    serde_json::from_str(raw).expect("parse iq behavior.json golden")
}

/// `[[value, ts], ...]` → `Vec<(String, i64)>` of records to replay.
fn records(section: &Value) -> Vec<(String, i64)> {
    section["records"]
        .as_array()
        .expect("records array")
        .iter()
        .map(|r| {
            let a = r.as_array().expect("record pair");
            (
                a[0].as_str().expect("record value").to_string(),
                a[1].as_i64().expect("record ts"),
            )
        })
        .collect()
}

/// KV count. It replays `records` under each value as the key, then reads get,
/// range, all, and count.
#[tokio::test]
async fn iq_kv_golden_parity() {
    let g = golden();
    let kv = &g["kv"];

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count("counts");
    let built = b.build("app").unwrap();
    let mut d = TopologyTestDriver::new(&built).unwrap();
    // Each record's key equals its value (matches the JVM capture).
    for (v, ts) in records(kv) {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.clone()),
            v,
            ts,
        );
    }

    // get("a") and get("z")=null.
    let want_get_a = kv["get_a"].as_i64().expect("get_a");
    assert_eq!(
        d.iq_kv_get("counts", &"a".to_string(), &StringSerde, &I64Serde)
            .await,
        Some(want_get_a)
    );
    assert!(kv["get_z"].is_null());
    assert_eq!(
        d.iq_kv_get("counts", &"z".to_string(), &StringSerde, &I64Serde)
            .await,
        None
    );

    // range("a","b") inclusive — exact match against the golden pairs.
    let got_range = d
        .iq_kv_range(
            "counts",
            &"a".to_string(),
            &"b".to_string(),
            &StringSerde,
            &I64Serde,
        )
        .await;
    assert_eq!(got_range, pairs(&kv["range_a_b"]), "range_a_b parity");

    // all() — store order may differ, so compare as sorted sets.
    let mut got_all = d.iq_kv_all("counts", &StringSerde, &I64Serde).await;
    got_all.sort();
    let mut want_all = pairs(&kv["all"]);
    want_all.sort();
    assert_eq!(got_all, want_all, "all() parity");

    // count() — approximateNumEntries is approximate. The JVM (RocksDB) reports
    // total writes; our in-memory store reports distinct keys. Both must land in
    // the valid [distinct_keys, total_writes] band that brackets the golden.
    let jvm_count = kv["count"].as_u64().expect("count");
    let distinct = want_all.len() as u64;
    let total_writes = kv["records"].as_array().unwrap().len() as u64;
    assert!(
        (distinct..=total_writes).contains(&jvm_count),
        "golden count {jvm_count} must be within [{distinct}, {total_writes}]"
    );
    let our_count = d.iq_kv_count("counts").await;
    assert!(
        (distinct..=total_writes).contains(&our_count),
        "our count {our_count} must be within [{distinct}, {total_writes}] (JVM golden: {jvm_count})"
    );
}

/// Window count on a tumbling window. It replays the timestamped records under
/// key "k", then reads the point fetch and the range fetch.
#[tokio::test]
async fn iq_window_golden_parity() {
    let gold = golden();
    let win = &gold["window"];
    // The golden fixture keys stay `*_ms`; the value becomes an extent here.
    let size = Time::from_millis(win["size_ms"].as_i64().expect("size_ms"));

    let builder = StreamsBuilder::new();
    builder
        .stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(size))
        .count("wc");
    let built = builder.build("app").unwrap();
    let mut driver = TopologyTestDriver::new(&built).unwrap();
    // The capture keys every record "k"; the record's first field is that key.
    for (k, ts) in records(win) {
        driver.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k),
            "x".to_string(),
            ts,
        );
    }

    // Point fetch at window start 0.
    let want_single = win["fetch_single_k_0"].as_i64().expect("fetch_single_k_0");
    assert_eq!(
        driver
            .iq_window_fetch_single("wc", &"k".to_string(), 0, &StringSerde, &I64Serde)
            .await,
        Some(want_single)
    );

    // Range fetch over [0, size] → ascending (windowStart, count).
    let got = driver
        .iq_window_fetch(
            "wc",
            &"k".to_string(),
            0,
            size.millis_i64(),
            &StringSerde,
            &I64Serde,
        )
        .await;
    let want: Vec<(i64, i64)> = win["fetch_k_0_1000"]
        .as_array()
        .expect("fetch_k_0_1000")
        .iter()
        .map(|pair| {
            let cols = pair.as_array().unwrap();
            (cols[0].as_i64().unwrap(), cols[1].as_i64().unwrap())
        })
        .collect();
    assert_eq!(got, want, "window range fetch parity");
}

/// Session count. It replays the timestamped records under key "k", then reads
/// `fetch(key)`.
#[tokio::test]
async fn iq_session_golden_parity() {
    let gold = golden();
    let sess = &gold["session"];
    let gap = Time::from_millis(sess["gap_ms"].as_i64().expect("gap_ms"));

    let builder = StreamsBuilder::new();
    builder
        .stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(gap))
        .count("sc");
    let built = builder.build("app").unwrap();
    let mut driver = TopologyTestDriver::new(&built).unwrap();
    for (k, ts) in records(sess) {
        driver.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k),
            "x".to_string(),
            ts,
        );
    }

    let mut got = driver
        .iq_session_fetch("sc", &"k".to_string(), &StringSerde, &I64Serde)
        .await;
    got.sort_by_key(|((st, en), _)| (*st, *en));

    let mut want: Vec<((i64, i64), i64)> = sess["fetch_k"]
        .as_array()
        .expect("fetch_k")
        .iter()
        .map(|entry| {
            let cols = entry.as_array().unwrap();
            let bounds = cols[0].as_array().unwrap();
            (
                (bounds[0].as_i64().unwrap(), bounds[1].as_i64().unwrap()),
                cols[1].as_i64().unwrap(),
            )
        })
        .collect();
    want.sort_by_key(|((st, en), _)| (*st, *en));

    assert_eq!(got, want, "session fetch parity");
}

/// `[[key, value], ...]` JSON → `Vec<(String, i64)>`.
fn pairs(v: &Value) -> Vec<(String, i64)> {
    v.as_array()
        .expect("pairs array")
        .iter()
        .map(|p| {
            let a = p.as_array().expect("pair");
            (
                a[0].as_str().expect("pair key").to_string(),
                a[1].as_i64().expect("pair value"),
            )
        })
        .collect()
}
