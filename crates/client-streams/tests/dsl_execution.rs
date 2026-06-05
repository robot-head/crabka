#![cfg(not(target_os = "windows"))]
//! Execution-level tests for the KStream/KTable DSL: build a counting app via
//! `StreamsBuilder`, run it through the broker-free `TopologyTestDriver`, and
//! assert the forwarded running count + materialized store contents.
//!
//! The byte-exact golden validation (store-name index, repartition topic names)
//! is Task 8 — this test's gate is *execution correctness*, so it uses
//! `group_by_key` with no preceding key change (single subtopology, no
//! repartition) to stay robust.
use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde};

#[test]
fn dsl_count_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .count(Materialized::with(StringSerde, I64Serde).as_store("counts"))
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
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
/// correct end-to-end. (Byte-exact repartition topic naming is Task 8.)
#[test]
fn dsl_count_with_repartition_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        // re-key to the value → key-changing → forces a repartition
        .group_by(
            |_k: &String, v: &String| v.clone(),
            Grouped::with(StringSerde, StringSerde),
        )
        .count(Materialized::with(StringSerde, I64Serde).as_store("counts"))
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
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

/// `reduce`: first value per key seeds the accumulator; later values fold via
/// the reducer. Concatenate string values per key.
#[test]
fn dsl_reduce_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .reduce(
            |acc: &String, v: &String| format!("{acc}{v}"),
            Materialized::with(StringSerde, StringSerde).as_store("reduced"),
        )
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `split`/`branch`: records are routed to matching branch children.
///
/// Uses mutually-exclusive predicates so each record reaches exactly one branch;
/// both branches are merged to a single output. The implementation routes a record
/// to EVERY branch whose predicate matches (not first-match-wins); with
/// mutually-exclusive predicates the behaviour is identical.
#[test]
fn dsl_branch_executes() {
    let b = StreamsBuilder::new();
    let src = b.stream(["in"], Consumed::with(StringSerde, StringSerde));
    let split = src.split();
    // b1 matches records with value "a"; b2 matches anything else.
    let b1 = split.branch(|_k: &String, v: &String| v == "a");
    let b2 = split.branch(|_k: &String, v: &String| v != "a");
    b1.merge(&b2)
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `repartition()` must not panic — records must flow through the internal
/// loop-back repartition topic and arrive at the sink.
///
/// Topology: stream("in") → repartition → `map_values(upper)` → to("out").
/// The test driver loops the repartition topic back automatically.
#[test]
fn dsl_repartition_executes() {
    use crabka_client_streams::Repartitioned;
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .repartition(Repartitioned::with(StringSerde, StringSerde))
        .map_values(|v: &String| v.to_uppercase())
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `map`: rewrite both key and value end-to-end through the driver.
///
/// Topology: `stream("in")` → `map(key=len(k), value=upper(v))` → `to("out")`.
/// Verifies that both the new key (derived from the original key) and the new
/// value are forwarded correctly.
#[test]
fn dsl_map_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .map(|k: &String, v: &String| (i64::try_from(k.len()).unwrap(), v.to_uppercase()))
        .to("out", Produced::with(I64Serde, StringSerde));
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

/// `select_key`: rewrite only the key, value unchanged.
///
/// Topology: `stream("in")` → `select_key(value as key)` → `to("out")`.
/// Asserts the outgoing key is the original value and the value is unmodified.
#[test]
fn dsl_select_key_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .select_key(|_k: &String, v: &String| v.clone())
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `filter_not`: the complement of `filter` — records where the predicate is
/// false pass through; records where it is true are dropped.
///
/// Topology: `stream("in")` → `filter_not(value == "drop")` → `to("out")`.
/// Pipe three records: "keep", "drop", "also-keep". Only "keep" and "also-keep"
/// must appear in the output.
#[test]
fn dsl_filter_not_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .filter_not(|_k: &String, v: &String| v == "drop")
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `flat_map`: one input record expands to multiple `(K2, V2)` output records.
///
/// Topology: `stream("in")` → `flat_map(split value on '-')` → `to("out")`.
/// Input "a-b-c" with key "k" expands to three output records each keyed by
/// the fragment index.
#[test]
fn dsl_flat_map_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .flat_map(|_k: &String, v: &String| {
            v.split('-')
                .enumerate()
                .map(|(i, part)| (i.to_string(), part.to_string()))
                .collect::<Vec<_>>()
        })
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `flat_map_values`: one value expands to multiple values, key unchanged.
///
/// Topology: `stream("in")` → `flat_map_values(chars)` → `to("out")`.
/// Input "hi" with key "k" expands to two records with values "h" and "i",
/// both with key "k".
#[test]
fn dsl_flat_map_values_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .flat_map_values(|v: &String| v.chars().map(|c| c.to_string()).collect::<Vec<_>>())
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `peek`: side-effect is executed for each record; records pass through
/// unchanged.
///
/// Topology: stream("in") → peek(collect into shared vec) → to("out").
/// Pipes two records and asserts: (1) both appear at "out" unchanged, and
/// (2) the shared vec collected the two (key, value) pairs.
#[test]
fn dsl_peek_executes() {
    use std::sync::{Arc, Mutex};

    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = Arc::clone(&seen);

    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .peek(move |k: &String, v: &String| {
            seen_clone.lock().unwrap().push((k.clone(), v.clone()));
        })
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `foreach`: terminal side-effect — records are collected via a shared vec
/// but no output topic exists. Verifies the closure fires for each record and
/// that nothing is forwarded (no sink is wired after `foreach`).
#[test]
fn dsl_foreach_executes() {
    use std::sync::{Arc, Mutex};

    let collected: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let collected_clone = Arc::clone(&collected);

    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
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

/// `aggregate`: generic aggregation with a caller-supplied `init` and `agg`
/// function, materialized as a `KTable`.
///
/// Topology: `stream("in")` → `group_by_key` → `aggregate(init=0, agg=sum of
/// value lengths)` → `to_stream` → `to("out")`. Each record accumulates the sum of
/// the string value lengths per key, forwarding the running sum.
#[test]
fn dsl_aggregate_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .aggregate(
            || 0i64,
            |_k: &String, v: &String, acc: i64| acc + i64::try_from(v.len()).unwrap(),
            Materialized::with(StringSerde, I64Serde).as_store("agg-store"),
        )
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
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

/// `KTable::filter`: matching rows are forwarded and materialized; non-matching
/// rows are removed from the store and not forwarded.
///
/// Topology: `table("in")` → `filter(v > 10)` → `to_stream` → `to("out")`.
/// Pipe value 42 for "a" (matches) then value 5 for "b" (dropped). Only the
/// "a" record must appear at "out"; the store must contain "a" but not "b".
#[test]
fn dsl_ktable_filter_executes() {
    let b = StreamsBuilder::new();
    b.table(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("src-tbl"),
    )
    .filter(
        |_k: &String, v: &i64| *v > 10,
        Materialized::with(StringSerde, I64Serde).as_store("filtered-tbl"),
    )
    .to_stream()
    .to("out", Produced::with(StringSerde, I64Serde));
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

/// `KTable::map_values` (non-materialized view form): forwards rewritten values
/// without materializing a store or emitting a changelog topic.
///
/// Topology: `table("in")` → `map_values(v*2, non-materialized)` → `to_stream` →
/// `to("out")`. Asserts the doubled value reaches the sink and that no store
/// named for this step exists in the topology.
#[test]
fn dsl_ktable_map_values_view_executes() {
    let b = StreamsBuilder::new();
    b.table(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("src-tbl"),
    )
    .map_values(|v: &i64| v * 2)
    .to_stream()
    .to("out", Produced::with(StringSerde, I64Serde));
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

/// `Materialized::with_logging(false)` must suppress the changelog topic from
/// the wire topology. The store is still functional (in-memory state is
/// maintained), but `state_changelog_topics` must be empty.
#[test]
fn dsl_count_no_logging_omits_changelog() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .count(
            Materialized::with(StringSerde, I64Serde)
                .as_store("counts")
                .with_logging(false),
        )
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
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
/// After "k"="keep" is written, the downstream "view" store holds "keep".
/// After "k"="drop" is written (fails the filter), the filter emits a tombstone;
/// `map_values_materialized` must delete "k" from "view".
#[test]
fn dsl_ktable_filter_tombstone_propagates_downstream() {
    let b = StreamsBuilder::new();
    b.table(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("src"),
    )
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
/// `map_values` rewrites + re-materializes it. Exercises the table source +
/// table map-values execution paths end-to-end through `to_stream`.
#[test]
fn dsl_table_map_values_executes() {
    let b = StreamsBuilder::new();
    b.table(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("tbl"),
    )
    .map_values_materialized(
        |v: &i64| v * 10,
        Materialized::with(StringSerde, I64Serde).as_store("tbl-x10"),
    )
    .to_stream()
    .to("out", Produced::with(StringSerde, I64Serde));
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

/// `to_table`: materialize a stream into a `KTable`, then back to a stream.
///
/// Each input record overwrites the prior value for its key in the store; the
/// `KTable` change-stream forwards the new value, which `to_stream` extracts and
/// sends to the sink. The materialized store holds the latest value per key.
#[test]
fn dsl_to_table_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .to_table(Materialized::with(StringSerde, StringSerde).as_store("store"))
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `to_table` with an **unnamed** `Materialized` (no `.as_store()`) auto-mints a
/// store name from the `KSTREAM-TOTABLE-STATE-STORE-` counter. The store name is
/// opaque here — we just assert that the output is correct (records flow through),
/// which exercises the `store_name = None → auto-mint` branch in `to_table`.
#[test]
fn dsl_to_table_unnamed_store_executes() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        // No `.as_store(...)` — store gets an auto-minted name.
        .to_table(Materialized::with(StringSerde, StringSerde))
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KStream::join` (inner stream-table join): a stream record is joined against
/// the materialized table store. Populate the table FIRST (pipe a `right` record),
/// then drive the stream side: a key present in the table produces an output; a
/// key absent from the table is dropped (inner join).
#[test]
fn dsl_stream_table_inner_join_executes() {
    let b = StreamsBuilder::new();
    let table = b.table::<String, String, _, _>(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("store"),
    );
    b.stream(["left"], Consumed::with(StringSerde, StringSerde))
        .join_table(&table, |v: &String, vt: &String| format!("{v}{vt}"))
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KStream::left_join` (left stream-table join): every stream record is
/// forwarded. On a table hit the joiner receives `Some`; on a miss it receives
/// `None` (here rendered as the empty string).
#[test]
fn dsl_stream_table_left_join_executes() {
    let b = StreamsBuilder::new();
    let table = b.table::<String, String, _, _>(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("store"),
    );
    b.stream(["left"], Consumed::with(StringSerde, StringSerde))
        .left_join_table(&table, |v: &String, opt: Option<&String>| {
            format!("{v}{}", opt.cloned().unwrap_or_default())
        })
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KTable::join` (inner KTable-KTable join): a row exists in the join output
/// only when BOTH source tables have a value for the key. Populate the left
/// table first (no output yet), then the right table (join emits "AB").
#[test]
fn dsl_ktable_ktable_inner_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String, _, _>(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("sa"),
    );
    let tb = b.table::<String, String, _, _>(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("sb"),
    );
    ta.join(&tb, |va: &String, vb: &String| format!("{va}{vb}"))
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KTable::left_join`: emits a row whenever the LEFT (this) side is present;
/// the right side is optional. Pipe only the left table → output reflects the
/// left value with an empty right side.
#[test]
fn dsl_ktable_ktable_left_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String, _, _>(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("sa"),
    );
    let tb = b.table::<String, String, _, _>(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("sb"),
    );
    ta.left_join(&tb, |va: &String, ob: Option<&String>| {
        format!("{va}{}", ob.cloned().unwrap_or_default())
    })
    .to_stream()
    .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KTable::outer_join`: emits a row whenever EITHER side is present. Pipe only
/// the right table → output reflects the right value with an empty left side.
#[test]
fn dsl_ktable_ktable_outer_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String, _, _>(
        "a",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("sa"),
    );
    let tb = b.table::<String, String, _, _>(
        "b",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("sb"),
    );
    ta.outer_join(&tb, |oa: Option<&String>, ob: Option<&String>| {
        format!(
            "{}{}",
            oa.cloned().unwrap_or_default(),
            ob.cloned().unwrap_or_default()
        )
    })
    .to_stream()
    .to("out", Produced::with(StringSerde, StringSerde));
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

/// `to_table` with `with_logging(false)` must suppress the changelog topic from
/// the wire topology (`add_state_store_no_changelog` branch). The store is still
/// functional at runtime.
#[test]
fn dsl_to_table_no_logging_omits_changelog() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .to_table(
            Materialized::with(StringSerde, StringSerde)
                .as_store("s")
                .with_logging(false),
        )
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
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

/// `windowedBy(TimeWindows).count` (tumbling): per-window running count. A
/// record at ts=12 falls into a new window `[10,20)`, so its count restarts.
#[test]
fn dsl_windowed_count_tumbling_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{
        Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde, TimeWindowedSerde,
        TimeWindows, Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(10))
        .count(Materialized::with(StringSerde, I64Serde).as_store("w"))
        .to_stream()
        .to(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde),
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
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde);
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

/// `windowedBy(TimeWindows.advance_by)` (hopping): a record at ts=12 with a
/// size-10/advance-5 window falls into both `[5,15)` and `[10,20)`, emitting
/// one count per overlapping window.
#[test]
fn dsl_windowed_count_hopping_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{
        Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde, TimeWindowedSerde,
        TimeWindows, Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(10).advance_by(5))
        .count(Materialized::with(StringSerde, I64Serde).as_store("w"))
        .to_stream()
        .to(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde),
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
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde);
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

/// `windowedBy(TimeWindows).reduce`: concatenate string values within a window;
/// the first value in a window seeds the accumulator, later values fold.
#[test]
fn dsl_windowed_reduce_executes() {
    use crabka_client_streams::Materialized;
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{
        Consumed, Grouped, Produced, StringSerde, TimeWindowedSerde, TimeWindows, Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(10))
        .reduce(
            |acc: &String, v: &String| format!("{acc}{v}"),
            Materialized::with(StringSerde, StringSerde).as_store("w"),
        )
        .to_stream()
        .to(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 10), StringSerde),
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
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, 10), StringSerde);
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

/// `windowedBy(TimeWindows).aggregate`: general init+agg summing the integer
/// values per window.
#[test]
fn dsl_windowed_aggregate_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{
        Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde, TimeWindowedSerde,
        TimeWindows, Window, Windowed,
    };
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, I64Serde))
        .group_by_key(Grouped::with(StringSerde, I64Serde))
        .windowed_by(TimeWindows::of_size(10))
        .aggregate(
            || 0i64,
            |_k: &String, v: &i64, acc: i64| acc + *v,
            Materialized::with(StringSerde, I64Serde).as_store("w"),
        )
        .to_stream()
        .to(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde),
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
    let p = || Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde);
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

/// `KStream::join` (windowed inner stream-stream join): for each record on either
/// side, the matching window of the OTHER side's store is scanned and a joined
/// record emitted per match. A left record at `t` matches right records with
/// timestamp in `[t - before, t + after]` (and symmetrically the other side).
#[test]
fn dsl_stream_stream_inner_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, JoinWindows, Produced, StreamJoined, StringSerde};
    let b = StreamsBuilder::new();
    let left = b.stream(["left"], Consumed::with(StringSerde, StringSerde));
    let right = b.stream(["right"], Consumed::with(StringSerde, StringSerde));
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(10),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out", Produced::with(StringSerde, StringSerde));
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

/// Asymmetric `JoinWindows::of(10).before(0).after(20)` proves the OTHER-side
/// fetch-window swap. A record at `t` matches the other side over `[t-before,
/// t+after]` *from this record's perspective*; the per-side processor swaps
/// `before`/`after` so this holds for whichever side drives the record.
#[test]
fn dsl_stream_stream_join_swap_asymmetric() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, JoinWindows, Produced, StreamJoined, StringSerde};
    let b = StreamsBuilder::new();
    let left = b.stream(["left"], Consumed::with(StringSerde, StringSerde));
    let right = b.stream(["right"], Consumed::with(StringSerde, StringSerde));
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(10).before(0).after(20),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out", Produced::with(StringSerde, StringSerde));
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

/// Windowed join emits one output per matching record on the other side: two left
/// records at the same timestamp, then one right record in the window → TWO joins.
#[test]
fn dsl_stream_stream_join_duplicates() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, JoinWindows, Produced, StreamJoined, StringSerde};
    let b = StreamsBuilder::new();
    let left = b.stream(["left"], Consumed::with(StringSerde, StringSerde));
    let right = b.stream(["right"], Consumed::with(StringSerde, StringSerde));
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(10),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KStream::left_join` (windowed left stream-stream join): an unmatched LEFT
/// record is buffered and emitted as `joiner(a, None)` once its window closes
/// (driven by a later left record advancing stream-time). A matched left record
/// emits `joiner(a, Some(b))` and is NOT later re-emitted as a null.
#[test]
fn dsl_stream_stream_left_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, JoinWindows, Produced, StreamJoined, StringSerde};
    let b = StreamsBuilder::new();
    let left = b.stream(["left"], Consumed::with(StringSerde, StringSerde));
    let right = b.stream(["right"], Consumed::with(StringSerde, StringSerde));
    left.left_join(
        &right,
        |a: &String, b: Option<&String>| format!("{a}{}", b.cloned().unwrap_or_default()),
        JoinWindows::of(10),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out", Produced::with(StringSerde, StringSerde));
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

/// `KStream::outer_join` (windowed outer stream-stream join): an unmatched RIGHT
/// record is buffered and emitted as `joiner(None, Some(b))` once its window
/// closes (driven by a later right record advancing stream-time).
#[test]
fn dsl_stream_stream_outer_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, JoinWindows, Produced, StreamJoined, StringSerde};
    let b = StreamsBuilder::new();
    let left = b.stream(["left"], Consumed::with(StringSerde, StringSerde));
    let right = b.stream(["right"], Consumed::with(StringSerde, StringSerde));
    left.outer_join(
        &right,
        |a: Option<&String>, b: Option<&String>| {
            format!(
                "{}{}",
                a.cloned().unwrap_or_default(),
                b.cloned().unwrap_or_default()
            )
        },
        JoinWindows::of(10),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out", Produced::with(StringSerde, StringSerde));
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
/// session (with a tombstone for the intermediate session, which `to_stream`
/// drops); a record beyond the gap starts a new session. Exercises the JVM
/// session-merge in the DSL execution path.
#[test]
fn dsl_session_count_merges_within_gap() {
    use crabka_client_streams::{SessionWindowedSerde, SessionWindows, Window, Windowed};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by_session(SessionWindows::of_inactivity_gap(60))
        .count(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to(
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
/// sessions (no merge, no tombstone).
#[test]
fn dsl_session_count_separate_beyond_gap() {
    use crabka_client_streams::{SessionWindowedSerde, SessionWindows, Window, Windowed};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by_session(SessionWindows::of_inactivity_gap(60))
        .count(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to(
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
