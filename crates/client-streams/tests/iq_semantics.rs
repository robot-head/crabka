//! Execution-level tests for Interactive-Query *read semantics* over the three
//! materialized store kinds (KV / window / session). Each test builds a counting
//! topology with the Rust DSL, pipes deterministic input through the broker-free
//! [`TopologyTestDriver`], then reads the materialized store back through the
//! driver's IQ helpers — which go through the byte-level `IqQueryable` layer the
//! supervisor serves real `KafkaStreams::*_store` queries from.
//!
//! Expectations here are hand-computed over our own runtime (this is an EXECUTION
//! assertion, not a wire golden). The cross-check against the JVM lives in
//! `iq_golden.rs`, which replays the same inputs and asserts parity with a
//! captured JVM `TopologyTestDriver` run.

use crabka_client_streams::{
    Consumed, I64Serde, SessionWindows, StringSerde, TimeWindows, dsl::StreamsBuilder,
};

/// KV count store: `get(present)` returns the count, `get(absent)` is `None`,
/// `range` is inclusive `[lo, hi]`, `all`/`count` cover every key.
#[tokio::test]
async fn iq_kv_count_read_semantics() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count("counts");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // a,a,b → count(a)=2, count(b)=1.
    for v in ["a", "a", "b"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.to_string()),
            v.to_string(),
            0,
        );
    }

    // get(present) → count, get(absent) → None.
    assert2::assert!(
        d.iq_kv_get("counts", &"a".to_string(), &StringSerde, &I64Serde)
            .await
            == Some(2)
    );
    assert2::assert!(
        d.iq_kv_get("counts", &"b".to_string(), &StringSerde, &I64Serde)
            .await
            == Some(1)
    );
    assert2::assert!(
        d.iq_kv_get("counts", &"z".to_string(), &StringSerde, &I64Serde)
            .await
            == None
    );

    // range is inclusive [lo, hi]: ["a","b"] covers both keys.
    let range = d
        .iq_kv_range(
            "counts",
            &"a".to_string(),
            &"b".to_string(),
            &StringSerde,
            &I64Serde,
        )
        .await;
    assert2::assert!(range == vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    // A range that excludes b.
    let range_a = d
        .iq_kv_range(
            "counts",
            &"a".to_string(),
            &"a".to_string(),
            &StringSerde,
            &I64Serde,
        )
        .await;
    assert2::assert!(range_a == vec![("a".to_string(), 2)]);

    // all() returns every entry; count() the cardinality.
    let mut all = d.iq_kv_all("counts", &StringSerde, &I64Serde).await;
    all.sort();
    assert2::assert!(all == vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    assert2::assert!(d.iq_kv_count("counts").await == 2);

    // An absent store name reads as empty / None.
    assert2::assert!(
        d.iq_kv_get("nope", &"a".to_string(), &StringSerde, &I64Serde)
            .await
            == None
    );
    assert2::assert!(d.iq_kv_count("nope").await == 0);
}

/// Window count store (tumbling, size 10): `fetch_single(key, start)` reads one
/// window's count; `fetch(key, from, to)` returns ascending `(windowStart, count)`
/// over the inclusive time span.
#[tokio::test]
async fn iq_window_count_read_semantics() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(10))
        .count("wc");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // ts 3,7 → window [0,10) count 2; ts 12 → window [10,20) count 1.
    for ts in [3i64, 7, 12] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("k".to_string()),
            "x".to_string(),
            ts,
        );
    }

    // Point reads on each window start.
    assert2::assert!(
        d.iq_window_fetch_single("wc", &"k".to_string(), 0, &StringSerde, &I64Serde)
            .await
            == Some(2)
    );
    assert2::assert!(
        d.iq_window_fetch_single("wc", &"k".to_string(), 10, &StringSerde, &I64Serde)
            .await
            == Some(1)
    );
    // A start with no window → None.
    assert2::assert!(
        d.iq_window_fetch_single("wc", &"k".to_string(), 5, &StringSerde, &I64Serde)
            .await
            == None
    );

    // Range fetch over [0, 20] returns both windows ascending by start.
    let windows = d
        .iq_window_fetch("wc", &"k".to_string(), 0, 20, &StringSerde, &I64Serde)
        .await;
    assert2::assert!(windows == vec![(0, 2), (10, 1)]);

    // A narrower span only sees the first window.
    let first = d
        .iq_window_fetch("wc", &"k".to_string(), 0, 5, &StringSerde, &I64Serde)
        .await;
    assert2::assert!(first == vec![(0, 2)]);
}

/// Session count store (inactivity gap 60): `fetch(key)` returns each session as
/// `((start, end), count)`. Records within the gap merge; records beyond it form
/// a separate session.
#[tokio::test]
async fn iq_session_count_read_semantics() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(60))
        .count("sc");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // ts 0,30 merge into session [0,30] count 2; ts 200 is a new session [200,200] count 1.
    for ts in [0i64, 30, 200] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            "x".to_string(),
            ts,
        );
    }

    let mut sessions = d
        .iq_session_fetch("sc", &"a".to_string(), &StringSerde, &I64Serde)
        .await;
    sessions.sort_by_key(|((s, e), _)| (*s, *e));
    assert2::assert!(sessions == vec![((0, 30), 2), ((200, 200), 1)]);

    // A key with no sessions reads empty.
    let none = d
        .iq_session_fetch("sc", &"z".to_string(), &StringSerde, &I64Serde)
        .await;
    assert2::assert!(none.is_empty());
}
