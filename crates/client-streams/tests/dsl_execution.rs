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
        d.get_key_value_store::<String, i64>("counts")
            .unwrap()
            .get(&"a".to_string()),
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
        d.get_key_value_store::<String, i64>("counts")
            .unwrap()
            .get(&"x".to_string()),
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
        d.get_key_value_store::<String, String>("reduced")
            .unwrap()
            .get(&"a".to_string()),
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
        d.get_key_value_store::<String, i64>("agg-store")
            .unwrap()
            .get(&"a".to_string()),
        Some(7)
    );
    assert_eq!(
        d.get_key_value_store::<String, i64>("agg-store")
            .unwrap()
            .get(&"b".to_string()),
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
        d.get_key_value_store::<String, i64>("filtered-tbl")
            .unwrap()
            .get(&"a".to_string()),
        Some(42)
    );
    assert!(
        d.get_key_value_store::<String, i64>("filtered-tbl")
            .unwrap()
            .get(&"b".to_string())
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
        d.get_key_value_store::<String, i64>("counts")
            .unwrap()
            .get(&"a".to_string()),
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
        d.get_key_value_store::<String, String>("view")
            .unwrap()
            .get(&"k".to_string()),
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
        d.get_key_value_store::<String, String>("filt")
            .unwrap()
            .get(&"k".to_string()),
        None,
        "filter store must not hold the key after tombstone"
    );
    assert_eq!(
        d.get_key_value_store::<String, String>("view")
            .unwrap()
            .get(&"k".to_string()),
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
    assert_eq!(
        d.get_key_value_store::<String, i64>("tbl")
            .unwrap()
            .get(&"k".to_string()),
        Some(4)
    );
    assert_eq!(
        d.get_key_value_store::<String, i64>("tbl-x10")
            .unwrap()
            .get(&"k".to_string()),
        Some(40)
    );
}
