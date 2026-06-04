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
