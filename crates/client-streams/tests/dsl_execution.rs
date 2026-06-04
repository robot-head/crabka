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
    .map_values(
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
