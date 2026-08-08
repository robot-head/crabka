//! Execution-level tests for the KStream and KTable DSL.
//!
//! Each test builds a counting app with `StreamsBuilder`, runs it through the
//! broker-free `TopologyTestDriver`, and asserts the forwarded running count
//! and the materialized store contents.
//!
//! The byte-exact golden validation of the store-name index and the repartition
//! topic names is Task 8. This test's gate is *execution correctness*, so it
//! uses `group_by_key` with no preceding key change. That gives one subtopology
//! and no repartition, which keeps the test robust.
use crabka_client_streams::{
    Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde, dsl::StreamsBuilder,
};
use crabka_units::prelude::*;

#[test]
fn dsl_count_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count("counts")
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["a", "a", "b"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.to_string()),
            v.to_string(),
            0,
        );
    }
    // count forwards the running count per key
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 2))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("b".into()), 1))
    );
    assert_eq!(
        d.store_get::<String, i64>("counts", &"a".to_string()),
        Some(2)
    );
}

/// `group_by` is key-changing, so `count` must insert a repartition
/// (sink → internal repartition topic → source) and split into 2 subtopologies.
/// The test driver loops the repartition topic back, so the count is still
/// correct end to end. Byte-exact repartition topic naming is Task 8.
#[test]
fn dsl_count_with_repartition_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        // re-key to the value → key-changing → forces a repartition
        .group_by(
            |_k: &String, v: &String| v.clone(),
            Grouped::with(StringSerde, StringSerde),
        )
        .count("counts")
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // keys are irrelevant; the new key is the value
    for v in ["x", "x", "y"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("orig".to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("x".into()), 1))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("x".into()), 2))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("y".into()), 1))
    );
    assert_eq!(
        d.store_get::<String, i64>("counts", &"x".to_string()),
        Some(2)
    );
}

/// `reduce`: the first value per key seeds the accumulator, and later values
/// fold with the reducer. This test concatenates the string values per key.
#[test]
fn dsl_reduce_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .reduce(|acc: &String, v: &String| format!("{acc}{v}"), "reduced")
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, v) in [("a", "1"), ("a", "2"), ("b", "9")] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            v.to_string(),
            0,
        );
    }
    // first value for "a" seeds "1", second folds to "12"
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("a".into()), "1".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("a".into()), "12".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("b".into()), "9".into()))
    );
    assert_eq!(
        d.store_get::<String, String>("reduced", &"a".to_string()),
        Some("12".to_string())
    );
}

/// `split` and `branch` route records to the matching branch children.
///
/// The test uses mutually-exclusive predicates, so each record reaches exactly
/// one branch, and it merges both branches into a single output. The
/// implementation routes a record to EVERY branch whose predicate matches, not
/// to the first match only. With mutually-exclusive predicates the behaviour is
/// identical.
#[test]
fn dsl_branch_executes() {
    let b = StreamsBuilder::new();
    let src = b.stream::<String, String>(["in"]);
    let split = src.split();
    // b1 matches records with value "a"; b2 matches anything else.
    let b1 = split.branch(|_k: &String, v: &String| v == "a");
    let b2 = split.branch(|_k: &String, v: &String| v != "a");
    b1.merge(&b2).to("out");
    drop(b1);
    drop(b2);
    drop(src);
    drop(split);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["a", "b", "a"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.to_string()),
            v.to_string(),
            0,
        );
    }
    // All three records reach the output (each exactly once via its branch).
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("a".into()), "a".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("b".into()), "b".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("a".into()), "a".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `repartition()` must not panic. Records must flow through the internal
/// loop-back repartition topic and arrive at the sink.
///
/// Topology: stream("in") → repartition → `map_values(upper)` → to("out").
/// The test driver loops the repartition topic back automatically.
#[test]
fn dsl_repartition_executes() {
    use crabka_client_streams::Repartitioned;
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .repartition(Repartitioned::with(StringSerde, StringSerde))
        .map_values(|v: &String| v.to_uppercase())
        .to("out");
    // build must succeed (no missing thunk panic)
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["hello", "world"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("hello".into()), "HELLO".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("world".into()), "WORLD".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

// ── New execution tests for previously untested operators ────────────────────

/// `map` rewrites both the key and the value end to end through the driver.
///
/// Topology: `stream("in")` → `map(key=len(k), value=upper(v))` → `to("out")`.
/// The test checks that the driver forwards both the new key, which comes from
/// the original key, and the new value.
#[test]
fn dsl_map_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .map(|k: &String, v: &String| (i64::try_from(k.len()).unwrap(), v.to_uppercase()))
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("hello".to_string()),
        "world".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(I64Serde, StringSerde)),
        Some((Some(5_i64), "WORLD".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(I64Serde, StringSerde)),
        None
    );
}

/// `select_key` rewrites only the key and leaves the value unchanged.
///
/// Topology: `stream("in")` → `select_key(value as key)` → `to("out")`.
/// The test asserts that the outgoing key is the original value and that the
/// value is unmodified.
#[test]
fn dsl_select_key_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .select_key(|_k: &String, v: &String| v.clone())
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("old-key".to_string()),
        "new-key".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("new-key".to_string()), "new-key".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `filter_not` is the complement of `filter`. Records where the predicate is
/// false pass through, and records where it is true are dropped.
///
/// Topology: `stream("in")` → `filter_not(value == "drop")` → `to("out")`.
/// The test pipes three records: "keep", "drop", and "also-keep". Only "keep"
/// and "also-keep" must appear in the output.
#[test]
fn dsl_filter_not_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .filter_not(|_k: &String, v: &String| v == "drop")
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["keep", "drop", "also-keep"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("k".to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".into()), "keep".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".into()), "also-keep".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `flat_map` expands one input record into several `(K2, V2)` output records.
///
/// Topology: `stream("in")` → `flat_map(split value on '-')` → `to("out")`.
/// Input "a-b-c" with key "k" expands to three output records each keyed by
/// the fragment index.
#[test]
fn dsl_flat_map_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .flat_map(|_k: &String, v: &String| {
            v.split('-')
                .enumerate()
                .map(|(i, part)| (i.to_string(), part.to_string()))
                .collect::<Vec<_>>()
        })
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a-b-c".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("0".into()), "a".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("1".into()), "b".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("2".into()), "c".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `flat_map_values` expands one value into several values and keeps the key.
///
/// Topology: `stream("in")` → `flat_map_values(chars)` → `to("out")`.
/// Input "hi" with key "k" expands to two records with values "h" and "i",
/// both with key "k".
#[test]
fn dsl_flat_map_values_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .flat_map_values(|v: &String| v.chars().map(|c| c.to_string()).collect::<Vec<_>>())
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "hi".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".into()), "h".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".into()), "i".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `peek` runs a side effect for each record, and the records pass through
/// unchanged.
///
/// Topology: stream("in") → peek(collect into shared vec) → to("out").
/// The test pipes two records and asserts two things. First, both records
/// appear at "out" unchanged. Second, the shared vec collected the two
/// (key, value) pairs.
#[test]
fn dsl_peek_executes() {
    use std::sync::{Arc, Mutex};

    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = Arc::clone(&seen);

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .peek(move |k: &String, v: &String| {
            seen_clone.lock().unwrap().push((k.clone(), v.clone()));
        })
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, v) in [("k1", "v1"), ("k2", "v2")] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            v.to_string(),
            0,
        );
    }
    // Records pass through unchanged.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k1".into()), "v1".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k2".into()), "v2".into()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    // Side-effect fired for each record.
    let got = seen.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "v2".to_string()),
        ]
    );
}

/// `foreach` is a terminal side effect. A shared vec collects the records, and
/// no output topic exists.
///
/// The test checks that the closure runs for each record and that nothing is
/// forwarded, because no sink is wired after `foreach`.
#[test]
fn dsl_foreach_executes() {
    use std::sync::{Arc, Mutex};

    let collected: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let collected_clone = Arc::clone(&collected);

    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .foreach(move |k: &String, v: &String| {
            collected_clone.lock().unwrap().push((k.clone(), v.clone()));
        });
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, v) in [("a", "1"), ("b", "2"), ("a", "3")] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            v.to_string(),
            0,
        );
    }
    let got = collected.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "3".to_string()),
        ]
    );
}

/// `aggregate` is a generic aggregation with a caller-supplied `init` and `agg`
/// function, materialized as a `KTable`.
///
/// Topology: `stream("in")` → `group_by_key` → `aggregate(init=0, agg=sum of
/// value lengths)` → `to_stream` → `to("out")`. Each record adds to the sum of
/// the string value lengths per key, and the topology forwards the running
/// sum.
#[test]
fn dsl_aggregate_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .aggregate(
            || 0i64,
            |_k: &String, v: &String, acc: i64| acc + i64::try_from(v.len()).unwrap(),
            "agg-store",
        )
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // "a" gets "hi" (len=2) then "world" (len=5) → running sums 2, 7
    // "b" gets "x" (len=1) → running sum 1
    for (k, v) in [("a", "hi"), ("b", "x"), ("a", "world")] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 2_i64))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("b".into()), 1_i64))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 7_i64))
    );
    assert_eq!(
        d.store_get::<String, i64>("agg-store", &"a".to_string()),
        Some(7)
    );
    assert_eq!(
        d.store_get::<String, i64>("agg-store", &"b".to_string()),
        Some(1)
    );
}

/// `KTable::filter` forwards and materializes the matching rows. It removes the
/// non-matching rows from the store and does not forward them.
///
/// Topology: `table("in")` → `filter(v > 10)` → `to_stream` → `to("out")`.
/// The test pipes value 42 for "a", which matches, then value 5 for "b", which
/// is dropped. Only the "a" record must appear at "out". The store must contain
/// "a" but not "b".
#[test]
fn dsl_ktable_filter_executes() {
    let b = StreamsBuilder::new();
    b.table::<String, i64>("in", "src-tbl")
        .filter(
            |_k: &String, v: &i64| *v > 10,
            Materialized::with(StringSerde, I64Serde).as_store("filtered-tbl"),
        )
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("a".to_string()),
        42_i64,
        0,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("b".to_string()),
        5_i64,
        0,
    );
    // Only the matching record is forwarded.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 42_i64))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None
    );
    // Matching key is in the filtered store; non-matching key is absent.
    assert_eq!(
        d.store_get::<String, i64>("filtered-tbl", &"a".to_string()),
        Some(42)
    );
    assert!(
        d.store_get::<String, i64>("filtered-tbl", &"b".to_string())
            .is_none()
    );
}

/// `KTable::map_values` in the non-materialized view form forwards the
/// rewritten values. It does not materialize a store and does not emit a
/// changelog topic.
///
/// Topology: `table("in")` → `map_values(v*2, non-materialized)` → `to_stream` →
/// `to("out")`. The test asserts that the doubled value reaches the sink and
/// that the topology holds no store named for this step.
#[test]
fn dsl_ktable_map_values_view_executes() {
    let b = StreamsBuilder::new();
    b.table::<String, i64>("in", "src-tbl")
        .map_values(|v: &i64| v * 2)
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        7_i64,
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("k".into()), 14_i64))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None
    );
    // The source table store is present; no separate store for the view step.
    assert!(
        d.get_key_value_store::<String, i64>("src-tbl").is_some(),
        "source table store must exist"
    );
}

/// `Materialized::with_logging(false)` must keep the changelog topic out of the
/// wire topology. The store still works and keeps its in-memory state, but
/// `state_changelog_topics` must be empty.
#[test]
fn dsl_count_no_logging_omits_changelog() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count_explicit(
            Materialized::with(StringSerde, I64Serde)
                .as_store("counts")
                .with_logging(false),
        )
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    // No changelog topics anywhere in the wire topology.
    let wire = built.to_wire();
    let any_changelog = wire
        .subtopologies
        .iter()
        .any(|s| !s.state_changelog_topics.is_empty());
    assert!(
        !any_changelog,
        "expected no changelog topics but found some"
    );
    // The store still works at runtime.
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["a", "a", "b"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(v.to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.store_get::<String, i64>("counts", &"a".to_string()),
        Some(2)
    );
}

/// `KTable::filter` tombstone propagates to a downstream materialized store.
///
/// Topology: `table("in")` → `filter(v != "drop")` → `map_values_materialized(identity)`.
/// After the test writes "k"="keep", the downstream "view" store holds "keep".
/// After it writes "k"="drop", which fails the filter, the filter emits a
/// tombstone and `map_values_materialized` must delete "k" from "view".
#[test]
fn dsl_ktable_filter_tombstone_propagates_downstream() {
    let b = StreamsBuilder::new();
    b.table::<String, String>("in", "src")
        .filter(
            |_k: &String, v: &String| v != "drop",
            Materialized::with(StringSerde, StringSerde).as_store("filt"),
        )
        .map_values_materialized(
            |v: &String| v.clone(),
            Materialized::with(StringSerde, StringSerde).as_store("view"),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // 1. "k" = "keep" → present in the downstream materialized "view" store.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "keep".to_string(),
        0,
    );
    assert_eq!(
        d.store_get::<String, String>("view", &"k".to_string()),
        Some("keep".to_string())
    );

    // 2. update "k" to a value that FAILS the filter → tombstone must DELETE "k"
    //    from BOTH the filter store and the downstream "view" store.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "drop".to_string(),
        1,
    );
    assert_eq!(
        d.store_get::<String, String>("filt", &"k".to_string()),
        None,
        "filter store must not hold the key after tombstone"
    );
    assert_eq!(
        d.store_get::<String, String>("view", &"k".to_string()),
        None,
        "downstream view store must delete the key when tombstone propagates"
    );
}

/// `StreamsBuilder::table` materializes a source topic into a `KTable`, and
/// `map_values` rewrites and re-materializes it.
///
/// The test drives the table-source path and the table map-values path end to
/// end through `to_stream`.
#[test]
fn dsl_table_map_values_executes() {
    let b = StreamsBuilder::new();
    b.table::<String, i64>("in", "tbl")
        .map_values_materialized(
            |v: &i64| v * 10,
            Materialized::with(StringSerde, I64Serde).as_store("tbl-x10"),
        )
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        4_i64,
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("k".into()), 40))
    );
    assert_eq!(d.store_get::<String, i64>("tbl", &"k".to_string()), Some(4));
    assert_eq!(
        d.store_get::<String, i64>("tbl-x10", &"k".to_string()),
        Some(40)
    );
}

/// `to_table` materializes a stream into a `KTable`, and the test then converts
/// it back to a stream.
///
/// Each input record overwrites the previous value for its key in the store.
/// The `KTable` change-stream forwards the new value, `to_stream` extracts it,
/// and the sink receives it. The materialized store holds the latest value per
/// key.
#[test]
fn dsl_to_table_executes() {
    use crabka_client_streams::{Consumed, Produced, StringSerde, dsl::StreamsBuilder};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .to_table("store")
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a".to_string(),
        0,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "b".to_string(),
        1,
    );
    // The table forwards each new value; to_stream extracts it to the sink.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "a".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "b".to_string()))
    );
    // The store holds the latest value for the key.
    assert_eq!(
        d.store_get::<String, String>("store", &"k".to_string()),
        Some("b".to_string())
    );
}

/// `to_table` with an **unnamed** `Materialized`, that is without
/// `.as_store()`, auto-mints a store name from the
/// `KSTREAM-TOTABLE-STATE-STORE-` counter.
///
/// The store name is opaque here. The test asserts only that the output is
/// correct and that records flow through, which covers the
/// `store_name = None → auto-mint` branch in `to_table`.
#[test]
fn dsl_to_table_unnamed_store_executes() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        // No `.as_store(...)` — store gets an auto-minted name.
        .to_table_explicit(Materialized::with(StringSerde, StringSerde))
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a".to_string(),
        0,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "b".to_string(),
        1,
    );
    // The table forwards each new value through to_stream to the sink.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "a".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "b".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `KStream::join`, the inner stream-table join, joins a stream record against
/// the materialized table store.
///
/// The test populates the table FIRST with a `right` record, then drives the
/// stream side. A key present in the table produces an output. A key absent
/// from the table is dropped, because this is an inner join.
#[test]
fn dsl_stream_table_inner_join_executes() {
    let b = StreamsBuilder::new();
    let table = b.table::<String, String>("right", "store");
    b.stream::<String, String>(["left"])
        .join_table(&table, |v: &String, vt: &String| format!("{v}{vt}"))
        .to("out");
    drop(table);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // 1. Populate the table store via the `right` source: ("k", "T").
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "T".to_string(),
        0,
    );
    // 2. Stream record with a key present in the table → join emits "ST".
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "S".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "ST".to_string()))
    );
    // 3. Stream record with a key ABSENT from the table → inner join drops it.
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("x".to_string()),
        "S2".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `KStream::left_join`, the left stream-table join, forwards every stream
/// record. On a table hit the joiner receives `Some`. On a miss it receives
/// `None`, which this test renders as the empty string.
#[test]
fn dsl_stream_table_left_join_executes() {
    let b = StreamsBuilder::new();
    let table = b.table::<String, String>("right", "store");
    b.stream::<String, String>(["left"])
        .left_join_table(&table, |v: &String, opt: Option<&String>| {
            format!("{v}{}", opt.cloned().unwrap_or_default())
        })
        .to("out");
    drop(table);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Populate the table: ("k", "T").
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "T".to_string(),
        0,
    );
    // Hit: ("k", "S") → "ST".
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "S".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "ST".to_string()))
    );
    // Miss: ("x", "S2") → joiner gets None → "S2".
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("x".to_string()),
        "S2".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("x".to_string()), "S2".to_string()))
    );
}

/// `KTable::join`, the inner KTable-KTable join, puts a row in the join output
/// only when BOTH source tables have a value for the key.
///
/// The test populates the left table first, which produces no output yet, then
/// the right table, and the join emits "AB".
#[test]
fn dsl_ktable_ktable_inner_join_executes() {
    use crabka_client_streams::{Consumed, Produced, StringSerde, dsl::StreamsBuilder};
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("a", "sa");
    let tb = b.table::<String, String>("b", "sb");
    ta.join(&tb, |va: &String, vb: &String| format!("{va}{vb}"))
        .to_stream()
        .to("out");
    drop(ta);
    drop(tb);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Left side present, right side absent → inner join emits nothing.
    d.pipe_input(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "A".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    // Right side now present → join emits "AB".
    d.pipe_input(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "B".to_string(),
        1,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "AB".to_string()))
    );
}

/// `KTable::left_join` emits a row whenever the LEFT side, that is this side, is
/// present. The right side is optional. The test pipes only the left table, so
/// the output holds the left value with an empty right side.
#[test]
fn dsl_ktable_ktable_left_join_executes() {
    use crabka_client_streams::{Consumed, Produced, StringSerde, dsl::StreamsBuilder};
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("a", "sa");
    let tb = b.table::<String, String>("b", "sb");
    ta.left_join(&tb, |va: &String, ob: Option<&String>| {
        format!("{va}{}", ob.cloned().unwrap_or_default())
    })
    .to_stream()
    .to("out");
    drop(ta);
    drop(tb);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Only the left side present → left join emits the left value (right empty).
    d.pipe_input(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "A".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "A".to_string()))
    );
}

/// `KTable::outer_join` emits a row whenever EITHER side is present. The test
/// pipes only the right table, so the output holds the right value with an
/// empty left side.
#[test]
fn dsl_ktable_ktable_outer_join_executes() {
    use crabka_client_streams::{Consumed, Produced, StringSerde, dsl::StreamsBuilder};
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("a", "sa");
    let tb = b.table::<String, String>("b", "sb");
    ta.outer_join(&tb, |oa: Option<&String>, ob: Option<&String>| {
        format!(
            "{}{}",
            oa.cloned().unwrap_or_default(),
            ob.cloned().unwrap_or_default()
        )
    })
    .to_stream()
    .to("out");
    drop(ta);
    drop(tb);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Only the right side present → outer join emits the right value (left empty).
    d.pipe_input(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "B".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "B".to_string()))
    );
}

/// `to_table` with `with_logging(false)` must keep the changelog topic out of
/// the wire topology through the `add_state_store_no_changelog` branch. The
/// store still works at runtime.
#[test]
fn dsl_to_table_no_logging_omits_changelog() {
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .to_table_explicit(
            Materialized::with(StringSerde, StringSerde)
                .as_store("s")
                .with_logging(false),
        )
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();

    // Wire topology must have no changelog topics for the to_table store.
    let wire = built.to_wire();
    let any_changelog = wire
        .subtopologies
        .iter()
        .any(|sub| !sub.state_changelog_topics.is_empty());
    assert!(
        !any_changelog,
        "expected no changelog topics when with_logging(false) but found some: {wire:?}"
    );

    // The store still functions at runtime.
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a".to_string(),
        0,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "b".to_string(),
        1,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "a".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "b".to_string()))
    );
    assert_eq!(
        d.store_get::<String, String>("s", &"k".to_string()),
        Some("b".to_string())
    );
}

// ── Windowed aggregations (windowedBy) ──────────────────────────────────────

/// `windowedBy(TimeWindows).count` on a tumbling window keeps a per-window
/// running count. A record at ts=12 falls into a new window `[10,20)`, so its
/// count restarts.
#[test]
fn dsl_windowed_count_tumbling_executes() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, StringSerde, TimeWindowedSerde, TimeWindows, Window,
        Windowed, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(10)))
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        3,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        7,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        12,
    );
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde);
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 0, end: 10 }
            }),
            1
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 0, end: 10 }
            }),
            2
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 10, end: 20 }
            }),
            1
        ))
    );
}

/// `windowedBy(TimeWindows.advance_by)` on a hopping window puts a record at
/// ts=12 with a size-10 and advance-5 window into both `[5,15)` and `[10,20)`.
/// It emits one count per overlapping window.
#[test]
fn dsl_windowed_count_hopping_executes() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, StringSerde, TimeWindowedSerde, TimeWindows, Window,
        Windowed, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(10)).advance_by(millis(5)))
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        12,
    );
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde);
    // windows_for(12) for size=10 advance=5 = [5, 10] → two emissions
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 5, end: 15 }
            }),
            1
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 10, end: 20 }
            }),
            1
        ))
    );
}

/// `windowedBy(TimeWindows).reduce` concatenates the string values within a
/// window. The first value in a window seeds the accumulator, and later values
/// fold into it.
#[test]
fn dsl_windowed_reduce_executes() {
    use crabka_client_streams::{
        Consumed, Produced, StringSerde, TimeWindowedSerde, TimeWindows, Window, Windowed,
        dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(10)))
        .reduce(|acc: &String, v: &String| format!("{acc}{v}"), "w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), StringSerde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "1".to_string(),
        3,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "2".to_string(),
        7,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "9".to_string(),
        12,
    );
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), StringSerde);
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 0, end: 10 }
            }),
            "1".to_string()
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 0, end: 10 }
            }),
            "12".to_string()
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 10, end: 20 }
            }),
            "9".to_string()
        ))
    );
}

/// `windowedBy(TimeWindows).aggregate` is the general init and agg form. It
/// sums the integer values per window.
#[test]
fn dsl_windowed_aggregate_executes() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, StringSerde, TimeWindowedSerde, TimeWindows, Window,
        Windowed, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, i64>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(10)))
        .aggregate(|| 0i64, |_k: &String, v: &i64, acc: i64| acc + *v, "w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        5i64,
        3,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        7i64,
        7,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        2i64,
        12,
    );
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde);
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 0, end: 10 }
            }),
            5
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 0, end: 10 }
            }),
            12
        ))
    );
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 10, end: 20 }
            }),
            2
        ))
    );
}

// ---------------------------------------------------------------------------
// Windowed KStream-KStream inner join (#4d-iii Task B3)
// ---------------------------------------------------------------------------

/// `KStream::join`, the windowed inner stream-stream join, scans the matching
/// window of the OTHER side's store for each record on either side and emits one
/// joined record per match. A left record at `t` matches right records with a
/// timestamp in `[t - before, t + after]`, and the other side behaves
/// symmetrically.
#[test]
fn dsl_stream_stream_inner_join_executes() {
    use crabka_client_streams::{
        Consumed, JoinWindows, Produced, StreamJoined, StringSerde, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(millis(10)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Left record ("k", "a") at t=5: no matching right record yet → no output.
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a".to_string(),
        5,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    // Right record ("k", "b") at t=8: 8 ∈ [5-10, 5+10] → joins with the left "a".
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "b".to_string(),
        8,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "ab".to_string()))
    );
    // Right record ("k", "c") at t=20: 20 ∉ [5-10, 5+10] → no join with "a".
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "c".to_string(),
        20,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// Asymmetric `JoinWindows::of(millis(10)).before(millis(0)).after(millis(20))` proves the OTHER-side
/// fetch-window swap. A record at `t` matches the other side over `[t-before,
/// t+after]` *from this record's perspective*. The per-side processor swaps
/// `before` and `after`, so this holds for whichever side drives the record.
#[test]
fn dsl_stream_stream_join_swap_asymmetric() {
    use crabka_client_streams::{
        Consumed, JoinWindows, Produced, StreamJoined, StringSerde, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(millis(10))
            .before(millis(0))
            .after(millis(20)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Key "k1": A first at t=0, then B at t=15. When B@15 drives, the OTHER (B)
    // processor fetches A over the SWAPPED window [15-after, 15+before] =
    // [15-20, 15+0] = [-5, 15], which includes A@0 → joins.
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k1".to_string()),
        "a".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k1".to_string()),
        "b".to_string(),
        15,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k1".to_string()), "ab".to_string()))
    );

    // Key "k2": B first at t=0, then A at t=15. When A@15 drives, the THIS (A)
    // processor fetches B over [15-before, 15+after] = [15-0, 15+20] = [15, 35],
    // which does NOT include B@0 → no join (forward-only). Proves the swap: had
    // the OTHER processor not swapped, the symmetric reasoning would have matched.
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k2".to_string()),
        "b2".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k2".to_string()),
        "a2".to_string(),
        15,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// A windowed join emits one output per matching record on the other side. Two
/// left records at the same timestamp, then one right record in the window,
/// give TWO joins.
#[test]
fn dsl_stream_stream_join_duplicates() {
    use crabka_client_streams::{
        Consumed, JoinWindows, Produced, StreamJoined, StringSerde, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(millis(10)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Two left records for "k" at t=5 (retainDuplicates store keeps both).
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a1".to_string(),
        5,
    );
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "a2".to_string(),
        5,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    // One right record at t=8 ∈ [5-10, 5+10]: matches BOTH left records → two outputs.
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "b".to_string(),
        8,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "a1b".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "a2b".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

// ---------------------------------------------------------------------------
// Windowed KStream-KStream left/outer join (#4d-iii Phase C, KIP-633)
// ---------------------------------------------------------------------------

/// `KStream::left_join`, the windowed left stream-stream join, buffers an
/// unmatched LEFT record and emits it as `joiner(a, None)` once its window
/// closes. A later left record advances stream-time and drives that close. A
/// matched left record emits `joiner(a, Some(b))` and is NOT re-emitted later
/// as a null.
#[test]
fn dsl_stream_stream_left_join_executes() {
    use crabka_client_streams::{
        Consumed, JoinWindows, Produced, StreamJoined, StringSerde, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.left_join(
        &right,
        |a: &String, b: Option<&String>| format!("{a}{}", b.cloned().unwrap_or_default()),
        JoinWindows::of(millis(10)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Matched case: A("k1","a")@5 then B("k1","b")@8 ∈ [5-10,5+10] → "ab" once.
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k1".to_string()),
        "a".to_string(),
        5,
    );
    // No B yet, window still open → buffered, no output.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k1".to_string()),
        "b".to_string(),
        8,
    );
    // The match fires AND deletes k1@5 from the outer buffer → no later null.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k1".to_string()), "ab".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );

    // Unmatched left: A("k2","x")@5 with no B → buffered (window 5+10 open at st=5).
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k2".to_string()),
        "x".to_string(),
        5,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    // Later left A("k2","z")@100 advances stream-time past 5+after(10) → close-scan
    // emits the buffered (x, None) = "x" at ts=5. (The k1@5 match was already
    // removed, so it does NOT re-emit a null.)
    d.pipe_input(
        "left",
        Consumed::with(StringSerde, StringSerde),
        Some("k2".to_string()),
        "z".to_string(),
        100,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k2".to_string()), "x".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `KStream::outer_join`, the windowed outer stream-stream join, buffers an
/// unmatched RIGHT record and emits it as `joiner(None, Some(b))` once its
/// window closes. A later right record advances stream-time and drives that
/// close.
#[test]
fn dsl_stream_stream_outer_join_executes() {
    use crabka_client_streams::{
        Consumed, JoinWindows, Produced, StreamJoined, StringSerde, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.outer_join(
        &right,
        |a: Option<&String>, b: Option<&String>| {
            format!(
                "{}{}",
                a.cloned().unwrap_or_default(),
                b.cloned().unwrap_or_default()
            )
        },
        JoinWindows::of(millis(10)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Unmatched right: B("k","b")@5 with no A → buffered (window 5+before(10) open).
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "b".to_string(),
        5,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
    // Later right B("k","z")@100 advances stream-time past 5+before(10) → close-scan
    // emits the buffered (None, b) = "b" at ts=5.
    d.pipe_input(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "z".to_string(),
        100,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "b".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// Session window count: two records within the inactivity gap merge into one
/// session. The merge also emits a tombstone for the intermediate session,
/// which `to_stream` drops. A record beyond the gap starts a new session. The
/// test drives the JVM session-merge in the DSL execution path.
#[test]
fn dsl_session_count_merges_within_gap() {
    use crabka_client_streams::{SessionWindowedSerde, SessionWindows, Window, Windowed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(60)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for ts in [0i64, 30, 200] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            "x".to_string(),
            ts,
        );
    }
    let out = Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde);
    // @0 → new session [0,0] count 1.
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 0 }
            }),
            1
        ))
    );
    // @30 (within gap) → merged session [0,30] count 2 (the [0,0] tombstone is
    // dropped by to_stream).
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 30 }
            }),
            2
        ))
    );
    // @200 (beyond gap) → new session [200,200] count 1.
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window {
                    start: 200,
                    end: 200
                }
            }),
            1
        ))
    );
    assert_eq!(d.read_output("out", out), None);
}

/// Two records separated by more than the inactivity gap form two independent
/// sessions, with no merge and no tombstone.
#[test]
fn dsl_session_count_separate_beyond_gap() {
    use crabka_client_streams::{SessionWindowedSerde, SessionWindows, Window, Windowed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(60)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for ts in [0i64, 500] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            "x".to_string(),
            ts,
        );
    }
    let out = Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde);
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 0 }
            }),
            1
        ))
    );
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window {
                    start: 500,
                    end: 500
                }
            }),
            1
        ))
    );
    assert_eq!(d.read_output("out", out), None);
}

/// Session window `reduce` folds the values per session with the reducer. Two
/// records within the gap merge into one session whose value is the reduced
/// concatenation.
#[test]
fn dsl_session_reduce_executes() {
    use crabka_client_streams::{SessionWindowedSerde, SessionWindows, Window, Windowed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(60)))
        .reduce_explicit(
            |a: &String, c: &String| format!("{a}{c}"),
            Materialized::with(StringSerde, StringSerde),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), StringSerde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (v, ts) in [("x", 0i64), ("y", 30)] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            v.to_string(),
            ts,
        );
    }
    let out = Produced::with(SessionWindowedSerde::new(StringSerde), StringSerde);
    // @0 → [0,0]="x"; @30 (within gap) → merged [0,30]="xy" (the [0,0] tombstone is
    // dropped by to_stream).
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 0 }
            }),
            "x".to_string()
        ))
    );
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 30 }
            }),
            "xy".to_string()
        ))
    );
    assert_eq!(d.read_output("out", out), None);
}

/// Session window `aggregate` with an explicit init, aggregator, and merger
/// gives a count-equivalent over a session. The test drives the generic
/// session-aggregate lowering, which differs from the `count` convenience
/// path.
#[test]
fn dsl_session_aggregate_executes() {
    use crabka_client_streams::{SessionWindowedSerde, SessionWindows, Window, Windowed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(60)))
        .aggregate_explicit(
            || 0i64,
            |_k: &String, _v: &String, acc: i64| acc + 1,
            |_k: &String, a: i64, b: i64| a + b,
            Materialized::with(StringSerde, I64Serde),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for ts in [0i64, 30] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            "x".to_string(),
            ts,
        );
    }
    let out = Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde);
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 0 }
            }),
            1
        ))
    );
    // merged [0,30] aggregate = 2 (merger(0,1)=1 over the buffered session, + agg=2).
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 30 }
            }),
            2
        ))
    );
    assert_eq!(d.read_output("out", out), None);
}

/// Suppress(untilWindowCloses) buffers a window's count and emits it exactly
/// once, when stream-time passes the window's end. Records in window [0,60000)
/// produce no output until a later-window record advances stream-time past
/// 60000.
#[test]
fn dsl_suppress_until_window_closes_emits_final_only() {
    use crabka_client_streams::{
        BufferConfig, I64Serde, Suppressed, TimeWindowedSerde, TimeWindows, Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Two records in window [0,60000): count -> 2. No output yet (buffered).
    for ts in [1_000i64, 3_000] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("a".to_string()),
            "x".to_string(),
            ts,
        );
    }
    let out = Produced::with(
        TimeWindowedSerde::new(StringSerde, millis(60_000)),
        I64Serde,
    );
    assert_eq!(d.read_output("out", out), None); // buffered, window not yet closed
    // A record in window [60000,120000) advances stream-time to 65000 >= 60000 ->
    // window [0,60000) closes, emitting its final count (2) exactly once.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        65_000,
    );
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window {
                    start: 0,
                    end: 60_000
                }
            }),
            2
        ))
    );
    // The [60000,120000) window is still buffered -> no further output.
    assert_eq!(d.read_output("out", out), None);
}

/// Suppress closes several buffered windows at once. Two keys buffered in
/// window [0,60000) are both emitted in buffer order when a later-window record
/// advances stream-time past 60000. The test drives the end-to-end multi-entry
/// eviction path.
#[test]
fn dsl_suppress_closes_multiple_windows_in_order() {
    use crabka_client_streams::{
        BufferConfig, I64Serde, Suppressed, TimeWindowedSerde, TimeWindows, Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    let out = Produced::with(
        TimeWindowedSerde::new(StringSerde, millis(60_000)),
        I64Serde,
    );
    // "a" and "b" both land in window [0,60000) → two buffered entries, no output.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        1_000,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("b".to_string()),
        "x".to_string(),
        2_000,
    );
    assert_eq!(d.read_output("out", out), None);
    // "a" in window [60000,120000) advances stream-time to 70000 ≥ 60000 → both
    // [0,60000) entries close, emitted in buffer (insertion) order: a then b.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        70_000,
    );
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "a".into(),
                window: Window {
                    start: 0,
                    end: 60_000
                }
            }),
            1
        ))
    );
    assert_eq!(
        d.read_output("out", out),
        Some((
            Some(Windowed {
                key: "b".into(),
                window: Window {
                    start: 0,
                    end: 60_000
                }
            }),
            1
        ))
    );
    // The [60000,120000) "a" entry stays buffered → no further output.
    assert_eq!(d.read_output("out", out), None);
}

/// Suppress with a record cap shuts the task down when the buffer exceeds
/// `maxRecords`, which is the shutDownWhenFull policy. Three distinct keys land
/// in one still-open window [0,60000) with a cap of 2, so the third overflows
/// and the task panics.
#[test]
#[should_panic(expected = "max capacity")]
fn dsl_suppress_max_records_shuts_down_when_full() {
    use crabka_client_streams::{
        BufferConfig, I64Serde, Suppressed, TimeWindowedSerde, TimeWindows,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(
            BufferConfig::unbounded().with_max_records(2),
        ))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Three distinct keys in window [0,60000) (ts < 60000 → none close) → the third
    // brings the buffer to 3 > cap 2 → panic.
    for (k, ts) in [("a", 1_000i64), ("b", 2_000), ("c", 3_000)] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            "x".to_string(),
            ts,
        );
    }
}

/// Suppress `with_max_bytes` on a STRICT (`until_window_closes`) buffer applies
/// the shutDownWhenFull policy. Each buffered entry is a `TimeWindowedSerde`
/// key, which is a 1-char key plus an 8-byte window start, so 9 bytes, plus an
/// i64 value of 8 bytes, so 17 bytes in total. With a 20-byte cap the first key
/// fits, because 17 ≤ 20. The second key overflows the still-open window,
/// because 34 > 20, so the task panics. The test drives the full DSL →
/// `BufferConfig::byte_cap` → processor path.
#[test]
#[should_panic(expected = "bytes")]
fn dsl_suppress_max_bytes_shuts_down_when_full() {
    use crabka_client_streams::{
        BufferConfig, I64Serde, Suppressed, TimeWindowedSerde, TimeWindows,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(
            BufferConfig::unbounded().with_max_bytes(bytes(20)),
        ))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Two distinct keys in the open window [0,60000): the second pushes the buffer
    // to 34 bytes > 20 → shutdown.
    for (k, ts) in [("a", 1_000i64), ("b", 2_000)] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some(k.to_string()),
            "x".to_string(),
            ts,
        );
    }
}

/// Suppress `max_bytes` on an EAGER (`until_time_limit`) buffer applies the
/// emitEarlyWhenFull policy. An over-full byte buffer evicts the oldest entry
/// and emits it early. Non-windowed count keys serialize to 1 byte plus an
/// 8-byte i64, so 9 bytes each. A 10-byte cap holds one, so the second key
/// evicts the first early.
#[test]
fn dsl_suppress_max_bytes_emit_early() {
    use crabka_client_streams::{BufferConfig, I64Serde, Materialized, Suppressed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_time_limit(
            millis(1_000_000),
            BufferConfig::max_bytes(bytes(10)),
        ))
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // "a"@1 buffers (9 ≤ 10), nothing emits.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        1,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None
    );
    // "b"@2 pushes the buffer to 18 > 10 → evict + emit the oldest ("a", count 1).
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("b".to_string()),
        "x".to_string(),
        2,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
}

/// Suppress `until_time_limit` buffers a key and emits it once stream-time
/// advances past `record_ts + wait`. This is the rate limiter for a non-windowed
/// table.
#[test]
fn dsl_suppress_until_time_limit_rate_limits() {
    use crabka_client_streams::{BufferConfig, I64Serde, Materialized, Suppressed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_time_limit(
            millis(50),
            BufferConfig::unbounded(),
        ))
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // "a"@10 → count 1, buffered (buffer_time 10, would emit at 10+50=60). No output.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        10,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None
    );
    // "b"@100 advances stream-time to 100 ≥ 60 → "a" emits its final count (1).
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("b".to_string()),
        "x".to_string(),
        100,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
}

/// Suppress `emit_early_when_full`: an over-full eager buffer evicts the oldest
/// entry and emits it early, with no panic. With a cap of 1 and two keys, the
/// first key emits when the second key lands.
#[test]
fn dsl_suppress_emit_early_when_full_evicts_oldest() {
    use crabka_client_streams::{BufferConfig, I64Serde, Materialized, Suppressed};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_time_limit(
            millis(100_000),
            BufferConfig::max_records(1),
        )) // eager cap 1
        .to_stream()
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("a".to_string()),
        "x".to_string(),
        1,
    );
    // "b" overflows cap 1 → "a" is evicted + emitted early (no panic), even though
    // its 100s time-limit hasn't elapsed.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("b".to_string()),
        "x".to_string(),
        2,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
}

/// `until_window_closes` needs a strict buffer. An eager config panics at
/// construction.
#[test]
#[should_panic(expected = "strict")]
fn dsl_until_window_closes_rejects_eager_buffer() {
    use crabka_client_streams::{BufferConfig, Suppressed, Windowed};
    let _ = Suppressed::<Windowed<String>>::until_window_closes(BufferConfig::max_records(2));
}

// ── stream-globaltable join (GlobalKTable, G-i) ─────────────────────────────
//
// The global store/source/processor are invisible in the wire, so the executable
// graph has no global-update processor. The `TopologyTestDriver` materializes the
// global store directly (`pipe_global`) and the join processor reads it via the
// per-task registry. `as_store("global-store")` names the store so `pipe_global`
// can target it; the key-mapper derives the lookup key from the record value, so a
// stream value of "v1" looks up global key "v1".

/// Inner stream-globaltable join: a global hit forwards `joiner(sv, gv)` keyed
/// by the stream key. With `key_mapper = |_k, v| v.clone()`, a stream value "v1"
/// looks up global key "v1", which is a NON-stream-key lookup, and gives
/// "G1" → "v1G1".
#[test]
fn dsl_global_join_inner_hit_executes() {
    use crabka_client_streams::GlobalKTable;
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> =
        b.global_table::<String, String>("global", "global-store");
    b.stream::<String, String>(["in"])
        .join_global(
            &g,
            |_k: &String, v: &String| v.clone(),
            |sv: &String, gv: &String| format!("{sv}{gv}"),
        )
        .to("out");
    drop(g);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Seed the global store, then drive a record whose value maps to that key.
    d.pipe_global("global-store", "v1".to_string(), "G1".to_string());
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "v1".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "v1G1".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// Inner join miss: a stream value with no matching global key is dropped.
#[test]
fn dsl_global_join_inner_miss_drops() {
    use crabka_client_streams::GlobalKTable;
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> =
        b.global_table::<String, String>("global", "global-store");
    b.stream::<String, String>(["in"])
        .join_global(
            &g,
            |_k: &String, v: &String| v.clone(),
            |sv: &String, gv: &String| format!("{sv}{gv}"),
        )
        .to("out");
    drop(g);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_global("global-store", "v1".to_string(), "G1".to_string());
    // value "absent" has no matching global key → dropped.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "absent".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// Left join miss: a global miss still forwards, and the joiner receives
/// `None`.
#[test]
fn dsl_global_left_join_miss_emits_none() {
    use crabka_client_streams::GlobalKTable;
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> =
        b.global_table::<String, String>("global", "global-store");
    b.stream::<String, String>(["in"])
        .left_join_global(
            &g,
            |_k: &String, v: &String| v.clone(),
            |sv: &String, gv: Option<&String>| match gv {
                Some(g) => format!("{sv}{g}"),
                None => format!("{sv}-none"),
            },
        )
        .to("out");
    drop(g);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // No global seed for "v2" → miss → "v2-none".
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "v2".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "v2-none".to_string()))
    );
}

/// Mid-stream global update: a later record sees the latest global value after
/// the test calls `pipe_global` again with a new value for the same key.
#[test]
fn dsl_global_join_sees_midstream_update() {
    use crabka_client_streams::GlobalKTable;
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> =
        b.global_table::<String, String>("global", "global-store");
    b.stream::<String, String>(["in"])
        .join_global(
            &g,
            |_k: &String, v: &String| v.clone(),
            |sv: &String, gv: &String| format!("{sv}{gv}"),
        )
        .to("out");
    drop(g);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // First value, first record → "v1G1".
    d.pipe_global("global-store", "v1".to_string(), "G1".to_string());
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "v1".to_string(),
        0,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "v1G1".to_string()))
    );
    // Update the same global key, then re-drive → sees the NEW value "G2".
    d.pipe_global("global-store", "v1".to_string(), "G2".to_string());
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "v1".to_string(),
        1,
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "v1G2".to_string()))
    );
}

/// The key mapper builds a key from BOTH the stream key and the value, and not
/// from either one alone. The lookup key is `"<k>:<v>"`. It differs from the
/// stream key, from the value, and from the emitted output key, which stays the
/// stream key.
#[test]
fn dsl_global_join_key_mapper_derives_compound_key() {
    use crabka_client_streams::GlobalKTable;
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> =
        b.global_table::<String, String>("global", "global-store");
    b.stream::<String, String>(["in"])
        .join_global(
            &g,
            |k: &String, v: &String| format!("{k}:{v}"),
            |sv: &String, gv: &String| format!("{sv}{gv}"),
        )
        .to("out");
    drop(g);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_global("global-store", "k1:v1".to_string(), "G".to_string());
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k1".to_string()),
        "v1".to_string(),
        0,
    );
    // Output key is the stream key "k1", value is joiner(sv="v1", gv="G").
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k1".to_string()), "v1G".to_string()))
    );
}

// ---------------------------------------------------------------------------
// KStream::process (custom Processor-API node + connected state stores)
// ---------------------------------------------------------------------------

/// `KStream::process` with a stateful custom processor: a `Counter` reads and
/// writes a connected `counts` store per record and forwards
/// `(value, running_count)`.
///
/// Topology: `stream("in")` → `process(Counter, ["counts"])` → `to("out")`.
/// The test pipes `("k","a")`, `("k","a")`, and `("k","b")`. The per-VALUE
/// running count gives `("a",1)`, `("a",2)`, and `("b",1)`, and the store holds
/// 2 for "a".
#[test]
fn dsl_process_stateful_counter_executes() {
    use crabka_client_streams::{Processor, ProcessorContext, Record};
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
    let b = StreamsBuilder::new();
    b.add_state_store("counts", StringSerde, I64Serde);
    b.stream::<String, String>(["in"])
        .process(|| Counter, ["counts"])
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["a", "a", "b"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("k".to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 1))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("a".into()), 2))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("b".into()), 1))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        None
    );
    assert_eq!(
        d.store_get::<String, i64>("counts", &"a".to_string()),
        Some(2)
    );
}

/// `KStream::process` is **key-changing**, so a downstream aggregation must
/// insert a repartition.
///
/// The test builds `stream("in").process(Fwd,["store"]).group_by_key().count()`
/// and asserts that the wire carries a repartition topic. The process result
/// re-keys the records, so the count repartitions before it aggregates.
#[test]
fn dsl_process_is_key_changing_forces_repartition() {
    use crabka_client_streams::{Processor, ProcessorContext, Record};
    struct Fwd;
    #[async_trait::async_trait]
    impl Processor<String, String, String, String> for Fwd {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(r);
        }
    }
    let b = StreamsBuilder::new();
    b.add_state_store("store", StringSerde, StringSerde);
    b.stream::<String, String>(["in"])
        .process(|| Fwd, ["store"])
        .group_by_key()
        .count("counts")
        .to_stream()
        .to("out");
    let wire = b.build("app").unwrap().to_wire();
    // The process result is key-changing → the count inserts a repartition. Assert
    // SOME subtopology has a non-empty repartition sink + source.
    let has_sink = wire
        .subtopologies
        .iter()
        .any(|s| !s.repartition_sink_topics.is_empty());
    let has_source = wire
        .subtopologies
        .iter()
        .any(|s| !s.repartition_source_topics.is_empty());
    assert!(
        has_sink && has_source,
        "expected a repartition topic from the key-changing process; wire = {wire:?}"
    );
}

// ---------------------------------------------------------------------------
// KStream::process_values (fixed-key custom Processor-API node)
// ---------------------------------------------------------------------------

/// `KStream::process_values` with a fixed-key processor uppercases the value and
/// forwards it. The KEY is preserved, which is KIP-820 fixed-key semantics.
///
/// Topology: `stream("in")` → `process_values(Upper, ["store"])` → `to("out")`.
/// The test pipes `("k","hi")` and the output is `("k","HI")`, the SAME key with
/// an uppercased value. The store is connected, so the changelog appears, but
/// the processor does not read it.
#[test]
fn dsl_process_values_preserves_key_executes() {
    use crabka_client_streams::{FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord};
    struct Upper;
    #[async_trait::async_trait]
    impl FixedKeyProcessor<String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut FixedKeyProcessorContext<'_, '_, '_, String, String>,
            r: FixedKeyRecord<String, String>,
        ) {
            // Capture the value before `with_value` consumes the record.
            let v = r.value.clone();
            ctx.forward(r.with_value(v.to_uppercase()));
        }
    }
    let b = StreamsBuilder::new();
    b.add_state_store("store", StringSerde, StringSerde);
    b.stream::<String, String>(["in"])
        .process_values(|| Upper, ["store"])
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "hi".to_string(),
        0,
    );
    // SAME key "k", value uppercased to "HI".
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "HI".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        None
    );
}

/// `KStream::process_values` is **non-key-changing**, because KIP-820 preserves
/// the key, so a downstream aggregation must NOT insert a repartition.
///
/// The test builds
/// `stream("in").process_values(Upper,["store"]).group_by_key().count()` and
/// asserts that the wire carries NO repartition topic anywhere. This is the
/// CONTRAST with the `process` case
/// (`dsl_process_is_key_changing_forces_repartition`), which DOES repartition.
#[test]
fn dsl_process_values_is_not_key_changing_no_repartition() {
    use crabka_client_streams::{FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord};
    struct Upper;
    #[async_trait::async_trait]
    impl FixedKeyProcessor<String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut FixedKeyProcessorContext<'_, '_, '_, String, String>,
            r: FixedKeyRecord<String, String>,
        ) {
            let v = r.value.clone();
            ctx.forward(r.with_value(v.to_uppercase()));
        }
    }
    let b = StreamsBuilder::new();
    b.add_state_store("store", StringSerde, StringSerde);
    b.stream::<String, String>(["in"])
        .process_values(|| Upper, ["store"])
        .group_by_key()
        .count("counts")
        .to_stream()
        .to("out");
    let wire = b.build("app").unwrap().to_wire();
    // process_values preserves the key → NO repartition. Every subtopology's
    // repartition sink AND source lists must be empty.
    let any_sink = wire
        .subtopologies
        .iter()
        .any(|s| !s.repartition_sink_topics.is_empty());
    let any_source = wire
        .subtopologies
        .iter()
        .any(|s| !s.repartition_source_topics.is_empty());
    assert!(
        !any_sink && !any_source,
        "expected NO repartition topic from non-key-changing process_values; wire = {wire:?}"
    );
}

/// `process_values` reads a CONNECTED store through `get_state_store` and the
/// source `record_context`. A `Tagger` counts the per-key occurrences in the
/// `seen` store and forwards `value#count`, and it keeps the key.
///
/// The test drives the fixed-key context's store accessor and record-context
/// accessor over the real runtime path. It pipes `("k","a")` and `("k","b")`,
/// which give `("k","a#1")` and `("k","b#2")`.
#[test]
fn dsl_process_values_reads_store_and_record_context() {
    use crabka_client_streams::{FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord};
    struct Tagger;
    #[async_trait::async_trait]
    impl FixedKeyProcessor<String, String, String> for Tagger {
        async fn process(
            &mut self,
            ctx: &mut FixedKeyProcessorContext<'_, '_, '_, String, String>,
            r: FixedKeyRecord<String, String>,
        ) {
            // Source metadata is available on every record.
            assert!(!ctx.record_context().topic.is_empty());
            let n = {
                let store = ctx.get_state_store::<String, i64>("seen").unwrap();
                let n = store.get(&r.key).await.unwrap_or(0) + 1;
                store.put(r.key.clone(), n).await;
                n
            };
            let v = r.value.clone();
            ctx.forward(r.with_value(format!("{v}#{n}")));
        }
    }
    let b = StreamsBuilder::new();
    b.add_state_store("seen", StringSerde, I64Serde);
    b.stream::<String, String>(["in"])
        .process_values(|| Tagger, ["seen"])
        .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for v in ["a", "b"] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some("k".to_string()),
            v.to_string(),
            0,
        );
    }
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "a#1".to_string()))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, StringSerde)),
        Some((Some("k".to_string()), "b#2".to_string()))
    );
}

/// `process` panics at call time when it references a store that no
/// `add_state_store` call registered. This is the missing-store guard. The code
/// looks up the store thunk when it calls `process`, and does not defer the
/// lookup to lowering.
#[test]
#[should_panic(expected = "was not added via add_state_store")]
fn dsl_process_unknown_store_panics() {
    use crabka_client_streams::{Processor, ProcessorContext, Record};
    struct Fwd;
    #[async_trait::async_trait]
    impl Processor<String, String, String, String> for Fwd {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(r);
        }
    }
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .process(|| Fwd, ["missing"]);
}

/// `process_values` panics at call time when it references an unadded store,
/// through the same guard.
#[test]
#[should_panic(expected = "was not added via add_state_store")]
fn dsl_process_values_unknown_store_panics() {
    use crabka_client_streams::{FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord};
    struct FixedFwd;
    #[async_trait::async_trait]
    impl FixedKeyProcessor<String, String, String> for FixedFwd {
        async fn process(
            &mut self,
            ctx: &mut FixedKeyProcessorContext<'_, '_, '_, String, String>,
            r: FixedKeyRecord<String, String>,
        ) {
            ctx.forward(r);
        }
    }
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .process_values(|| FixedFwd, ["missing"]);
}

/// Sliding-window (KIP-450) behavioral golden: run the same out-of-order script
/// against the Rust runtime and compare every emission (key, window, value) to the
/// JVM `TopologyTestDriver` capture in `testdata/sliding_window/behavior.json`.
///
/// The input script deliberately includes an out-of-order record, `("a", 3)`
/// after stream-time has advanced to 12, and a cross-key record, `("b", 7)`,
/// that falls entirely in closed-window territory for that key. This matches the
/// exact JVM `KStreamSlidingWindowAggregate` behavior.
#[test]
fn sliding_window_count_matches_jvm_behavior() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, SlidingWindows, StringSerde, TimeWindowedSerde,
        dsl::StreamsBuilder,
    };
    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Row {
        key: String,
        #[serde(rename = "windowStart")]
        window_start: i64,
        #[serde(rename = "windowEnd")]
        window_end: i64,
        value: i64,
    }

    let inputs: &[(&str, i64)] = &[("a", 0), ("a", 5), ("a", 12), ("a", 3), ("b", 7), ("a", 30)];
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, ts) in inputs {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some((*k).to_string()),
            "v".to_string(),
            *ts,
        );
    }
    let mut got: Vec<Row> = Vec::new();
    while let Some((Some(wk), v)) = d.read_output(
        "out",
        Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
    ) {
        got.push(Row {
            key: wk.key,
            window_start: wk.window.start,
            window_end: wk.window.end,
            value: v,
        });
    }
    let golden: Vec<Row> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/sliding_window/behavior.json").unwrap(),
    )
    .unwrap();
    assert_eq!(
        got, golden,
        "sliding-window output sequence != JVM behavioral golden"
    );
}

/// Row shape shared by the emit-final behavioral goldens.
#[derive(serde::Deserialize, PartialEq, Debug)]
struct EmitFinalRow {
    key: String,
    #[serde(rename = "windowStart")]
    window_start: i64,
    #[serde(rename = "windowEnd")]
    window_end: i64,
    value: i64,
}

/// Emit-final (KIP-825) TIME-window count must match the JVM 4.1.0 capture in
/// `testdata/emit_final/time.json` for `EmitStrategy.onWindowClose()`.
///
/// This test pins the strict close boundary. A window emits only once
/// stream-time moves PAST its end.
#[test]
fn emit_final_time_window_matches_jvm_behavior() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, StringSerde, TimeWindowedSerde, TimeWindows,
        dsl::{EmitStrategy, StreamsBuilder},
    };
    let inputs: &[(&str, i64)] = &[("a", 1), ("a", 5), ("a", 11), ("a", 21), ("a", 40)];
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(10)))
        .emit_strategy(EmitStrategy::on_window_close())
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, ts) in inputs {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some((*k).to_string()),
            "v".to_string(),
            *ts,
        );
    }
    let mut got: Vec<EmitFinalRow> = Vec::new();
    while let Some((Some(wk), v)) = d.read_output(
        "out",
        Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
    ) {
        got.push(EmitFinalRow {
            key: wk.key,
            window_start: wk.window.start,
            window_end: wk.window.end,
            value: v,
        });
    }
    let golden: Vec<EmitFinalRow> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/emit_final/time.json").unwrap(),
    )
    .unwrap();
    assert_eq!(got, golden, "emit-final time-window sequence != JVM golden");
}

/// Emit-final SLIDING-window count must match `testdata/emit_final/sliding.json`.
#[test]
fn emit_final_sliding_window_matches_jvm_behavior() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, SlidingWindows, StringSerde, TimeWindowedSerde,
        dsl::{EmitStrategy, StreamsBuilder},
    };
    let inputs: &[(&str, i64)] = &[("a", 1), ("a", 5), ("a", 11), ("a", 21), ("a", 40)];
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .emit_strategy(EmitStrategy::on_window_close())
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, ts) in inputs {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some((*k).to_string()),
            "v".to_string(),
            *ts,
        );
    }
    let mut got: Vec<EmitFinalRow> = Vec::new();
    while let Some((Some(wk), v)) = d.read_output(
        "out",
        Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
    ) {
        got.push(EmitFinalRow {
            key: wk.key,
            window_start: wk.window.start,
            window_end: wk.window.end,
            value: v,
        });
    }
    let golden: Vec<EmitFinalRow> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/emit_final/sliding.json").unwrap(),
    )
    .unwrap();
    assert_eq!(
        got, golden,
        "emit-final sliding-window sequence != JVM golden"
    );
}

/// Emit-final SESSION-window count must match `testdata/emit_final/session.json`.
///
/// The grace-0 script is the discriminator that pinned the strict close
/// boundary: `a@0,a@4` merge into `[0,4]`, `a@20` opens `[20,20]`, and `a@100`
/// closes both. The JVM emits NO zero-width `[0,0]` at stream-time 0.
#[test]
fn emit_final_session_window_matches_jvm_behavior() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, SessionWindowedSerde, SessionWindows, StringSerde,
        dsl::{EmitStrategy, StreamsBuilder},
    };
    let inputs: &[(&str, i64)] = &[("a", 0), ("a", 4), ("a", 20), ("a", 100)];
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(10)))
        .emit_strategy(EmitStrategy::on_window_close())
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, ts) in inputs {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some((*k).to_string()),
            "v".to_string(),
            *ts,
        );
    }
    let mut got: Vec<EmitFinalRow> = Vec::new();
    while let Some((Some(wk), v)) = d.read_output(
        "out",
        Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
    ) {
        got.push(EmitFinalRow {
            key: wk.key,
            window_start: wk.window.start,
            window_end: wk.window.end,
            value: v,
        });
    }
    let golden: Vec<EmitFinalRow> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/emit_final/session.json").unwrap(),
    )
    .unwrap();
    assert_eq!(
        got, golden,
        "emit-final session-window sequence != JVM golden"
    );
}

#[test]
fn sliding_window_count_builds() {
    use crabka_client_streams::{
        I64Serde, Materialized, Produced, SlidingWindows, StringSerde, TimeWindowedSerde,
        dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    // Building must not panic and must yield a wire topology.
    let _ = b.build_optimized("app").unwrap().to_wire();
}

/// Sliding-window (KIP-450) reduce behavioral golden: run the same out-of-order
/// script as count against the Rust reduce runtime and compare every emission
/// (key, window, value) to the JVM `TopologyTestDriver` capture in
/// `testdata/sliding_window/behavior_reduce.json`.
///
/// The reduce closure concatenates with `|`, so each window accumulates
/// "v", "v|v", "v|v|v", … matching the JVM `(a, v) -> a + "|" + v` reducer.
#[test]
fn sliding_window_reduce_matches_jvm_behavior() {
    use crabka_client_streams::{
        Consumed, Produced, SlidingWindows, StringSerde, TimeWindowedSerde, dsl::StreamsBuilder,
    };
    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Row {
        key: String,
        #[serde(rename = "windowStart")]
        window_start: i64,
        #[serde(rename = "windowEnd")]
        window_end: i64,
        value: String,
    }

    let inputs: &[(&str, i64)] = &[("a", 0), ("a", 5), ("a", 12), ("a", 3), ("b", 7), ("a", 30)];
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .reduce(|a: &String, v: &String| format!("{a}|{v}"), "w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), StringSerde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, ts) in inputs {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, StringSerde),
            Some((*k).to_string()),
            "v".to_string(),
            *ts,
        );
    }
    let mut got: Vec<Row> = Vec::new();
    while let Some((Some(wk), v)) = d.read_output(
        "out",
        Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), StringSerde),
    ) {
        got.push(Row {
            key: wk.key,
            window_start: wk.window.start,
            window_end: wk.window.end,
            value: v,
        });
    }
    let golden: Vec<Row> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/sliding_window/behavior_reduce.json").unwrap(),
    )
    .unwrap();
    assert_eq!(
        got, golden,
        "sliding-window reduce output sequence != JVM behavioral golden"
    );
}

/// Sliding-window aggregate through the ergonomic non-explicit `.aggregate()`
/// form. It uses a count-style aggregator (`+1`) to assert that the first
/// left-window emission is correct for two in-order records.
#[test]
fn sliding_window_aggregate_executes() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, SlidingWindows, StringSerde, TimeWindowedSerde, Window,
        Windowed, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .aggregate(|| 0i64, |_k: &String, _v: &String, a: i64| a + 1, "w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // t=20: first record, process_normal → left window [10,20] count=1.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        20,
    );
    // t=25: second record, process_normal → left window [15,25] seeded by [10,20] → count=2.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        25,
    );
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde);
    // First emission: left window [10,20] with count=1.
    assert_eq!(
        d.read_output("out", p()),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 10, end: 20 }
            }),
            1i64
        ))
    );
}

/// Sliding-window emit-final (KIP-825): `.emit_strategy(on_window_close())`
/// must suppress all per-update emits and forward the finals only once the
/// windows close.
///
/// With a large grace the windows from the two in-order records stay open and
/// emit nothing. A far-future record advances stream-time past their close and
/// flushes the finals. The discriminator against emit-on-update is that the
/// first two records produce NO output.
#[test]
fn sliding_window_emit_final_emits_only_on_close() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, SlidingWindows, StringSerde, TimeWindowedSerde,
        dsl::{EmitStrategy, StreamsBuilder},
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_and_grace(
            millis(10),
            millis(100),
        ))
        .emit_strategy(EmitStrategy::on_window_close())
        .aggregate(|| 0i64, |_k: &String, _v: &String, a: i64| a + 1, "w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    let consume = || Consumed::with(StringSerde, StringSerde);
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde);
    // Two in-window records (grace 100 keeps their windows open).
    d.pipe_input("in", consume(), Some("k".to_string()), "x".to_string(), 20);
    d.pipe_input("in", consume(), Some("k".to_string()), "x".to_string(), 25);
    // Emit-final: nothing forwarded while windows are open.
    assert_eq!(
        d.read_output("out", p()),
        None,
        "emit-final must not emit while windows are open"
    );
    // Far-future record advances stream-time → close_time 900 closes the
    // earlier windows, flushing their finals.
    d.pipe_input(
        "in",
        consume(),
        Some("k".to_string()),
        "x".to_string(),
        1000,
    );
    assert!(
        d.read_output("out", p()).is_some(),
        "closed windows must flush their finals once stream-time passes their close"
    );
}

/// Sliding-window count through the ergonomic non-explicit `.count()` form.
/// The test drives the `count` → `count_explicit` lowering path, which differs
/// from a direct `count_explicit` call.
#[test]
fn sliding_window_count_nonexplicit_builds_and_runs() {
    use crabka_client_streams::{
        Consumed, I64Serde, Produced, SlidingWindows, StringSerde, TimeWindowedSerde, Window,
        Windowed, dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .count("w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Single record at t=15, process_normal → left window [5,15] count=1.
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "x".to_string(),
        15,
    );
    assert_eq!(
        d.read_output(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), I64Serde)
        ),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 5, end: 15 }
            }),
            1i64
        ))
    );
}

/// Sliding-window reduce through the ergonomic non-explicit `.reduce()` form.
/// One record seeds the first value, because no earlier value is in the window,
/// so the result is `value.clone()`. The test asserts the single left-window
/// emission.
#[test]
fn sliding_window_reduce_nonexplicit() {
    use crabka_client_streams::{
        Consumed, Produced, SlidingWindows, StringSerde, TimeWindowedSerde, Window, Windowed,
        dsl::StreamsBuilder,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(10)))
        .reduce(|a: &String, v: &String| format!("{a}|{v}"), "w")
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), StringSerde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Single record at t=15: no prior record in window, seeds with value.clone() → "hello".
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Some("k".to_string()),
        "hello".to_string(),
        15,
    );
    assert_eq!(
        d.read_output(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, millis(10)), StringSerde)
        ),
        Some((
            Some(Windowed {
                key: "k".into(),
                window: Window { start: 5, end: 15 }
            }),
            "hello".to_string()
        ))
    );
}

/// `table_explicit` with `Materialized::as_versioned` materializes records into
/// a versioned key-value store. The store keeps out-of-order records under their
/// own timestamp and does not overwrite the latest pointer. On `get`, the store
/// returns the most recent record by commit timestamp.
#[test]
fn versioned_table_keeps_latest_on_out_of_order() {
    use crabka_client_streams::{I64Serde, Materialized, StringSerde};
    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    )
    .to_stream()
    .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // Pipe @ts=200 first, then the earlier @ts=100 (out-of-order).
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        20_i64,
        200,
    );
    d.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        10_i64,
        100,
    );
    // to_stream extracts Change.new; two records were piped so two outputs.
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("k".into()), 20_i64))
    );
    assert_eq!(
        d.read_output("out", Produced::with(StringSerde, I64Serde)),
        Some((Some("k".into()), 10_i64))
    );
    // The versioned store's latest (highest-timestamp) value must be 20.
    assert_eq!(
        d.store_get_versioned::<String, i64>("vt", &"k".to_string()),
        Some(20)
    );
}

/// The bytes the versioned table's changelog produces must match the JVM 4.1
/// capture exactly. KEY is the raw key, VALUE is the bare serialized value, and
/// the version timestamp is in the record-timestamp field (KIP-889). The test
/// builds UNOPTIMIZED, so the changelog is the derived `app-vt-changelog` topic
/// that the JVM oracle captured.
#[test]
fn versioned_table_changelog_matches_jvm() {
    fn hex(b: &[u8]) -> String {
        use std::fmt::Write as _;
        b.iter().fold(String::new(), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
    }
    let golden: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/golden/dsl/behavioral/versioned_changelog.json")
            .expect("changelog golden present"),
    )
    .unwrap();
    let expected: Vec<(String, Option<String>, i64)> = golden
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["keyHex"].as_str().unwrap().to_string(),
                e["valueHex"].as_str().map(str::to_string),
                e["ts"].as_i64().unwrap(),
            )
        })
        .collect();

    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, v, ts) in [
        ("k", 10, 100),
        ("k", 20, 200),
        ("k", 15, 150),
        ("k", 30, 300),
        ("j", 5, 120),
    ] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            i64::from(v),
            ts,
        );
    }
    let actual: Vec<(String, Option<String>, i64)> = d
        .drain_changelog()
        .into_iter()
        .filter(|(topic, _, _, _)| topic == "app-vt-changelog")
        .map(|(_, k, v, ts)| (hex(&k), v.as_ref().map(|b| hex(b)), ts.expect("version ts")))
        .collect();
    assert_eq!(actual, expected);
}

/// Replaying the JVM behavioral battery through the Rust versioned table must
/// reproduce the JVM's emitted (key, value) sequence on `out`.
#[test]
fn versioned_table_behavioral_matches_jvm() {
    let golden: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/golden/dsl/behavioral/versioned_table.json")
            .expect("behavioral golden present"),
    )
    .unwrap();
    let expected: Vec<(Option<String>, i64)> = golden
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["key"].as_str().map(str::to_string),
                e["value"].as_i64().unwrap(),
            )
        })
        .collect();

    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    )
    .to_stream()
    .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    for (k, v, ts) in [
        ("k", 10, 100),
        ("k", 20, 200),
        ("k", 15, 150),
        ("k", 30, 300),
        ("j", 5, 120),
    ] {
        d.pipe_input(
            "in",
            Consumed::with(StringSerde, I64Serde),
            Some(k.to_string()),
            i64::from(v),
            ts,
        );
    }
    let mut actual = Vec::new();
    while let Some((k, v)) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        actual.push((k, v));
    }
    assert_eq!(actual, expected);
}
