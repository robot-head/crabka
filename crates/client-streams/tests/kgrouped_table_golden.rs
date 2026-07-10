//! `KTable.groupBy` / `KGroupedTable` — JVM 4.1 wire-topology + behavioral goldens.
//!
//! Mirrors `Capture.java::kgroupedTable` and `KGroupedTableBehavior.java`:
//!   table("in", String/Long, Materialized "src-store")
//!     -> filter(v > 0, Materialized "filter-store")
//!     -> groupBy(key = even/odd by v%2, value = v) called THREE times, each terminating in
//!          count("count-store") / reduce(+,-,"reduce-store") / aggregate(0,+,-,"agg-store"),
//!        each .toStream().to("{count,reduce,agg}-out").
//!
//! Built with `build_optimized("app")` so `REUSE_KTABLE_SOURCE_TOPICS` reuses the
//! source topic `in` as `src-store`'s changelog (matching the JVM ground truth).
use crabka_client_streams::{Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde};

/// Build the combined topology shared by both tests and return the optimized build.
fn build_combined() -> crabka_client_streams::topology::BuiltTopology {
    use crabka_client_streams::dsl::StreamsBuilder;

    let b = StreamsBuilder::new();
    let src = b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("src-store"),
    );
    let pos = src.filter(
        |_k: &String, v: &i64| *v > 0,
        Materialized::with(StringSerde, I64Serde).as_store("filter-store"),
    );

    // Three groupBy branches on the SAME filtered table — shares the upstream
    // source+filter nodes (the JVM does the same: one filter, three repartition sinks).
    let mapper = |_k: &String, v: &i64| {
        (
            if v % 2 == 0 {
                "even".to_string()
            } else {
                "odd".to_string()
            },
            *v,
        )
    };

    pos.group_by_explicit(mapper, Grouped::with(StringSerde, I64Serde))
        .count_explicit(Materialized::with(StringSerde, I64Serde).as_store("count-store"))
        .to_stream()
        .to_explicit("count-out", Produced::with(StringSerde, I64Serde));

    pos.group_by_explicit(mapper, Grouped::with(StringSerde, I64Serde))
        .reduce_explicit(
            |a: &i64, v: &i64| a + v,
            |a: &i64, v: &i64| a - v,
            Materialized::with(StringSerde, I64Serde).as_store("reduce-store"),
        )
        .to_stream()
        .to_explicit("reduce-out", Produced::with(StringSerde, I64Serde));

    pos.group_by_explicit(mapper, Grouped::with(StringSerde, I64Serde))
        .aggregate_explicit(
            || 0i64,
            |_k: &String, v: &i64, a: i64| a + v,
            |_k: &String, v: &i64, a: i64| a - v,
            Materialized::with(StringSerde, I64Serde).as_store("agg-store"),
        )
        .to_stream()
        .to_explicit("agg-out", Produced::with(StringSerde, I64Serde));

    drop(src);
    drop(pos);
    b.build_optimized("app").unwrap()
}

#[test]
fn kgrouped_table_topology_matches_jvm() {
    let built = build_combined();
    let actual = serde_json::to_value(built.to_wire()).unwrap();
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/golden/dsl/kgrouped_table.topology.json").unwrap(),
    )
    .unwrap();
    assert2::assert!(actual == expected);
}

/// Build the auto-named topology: `table("in","src-store") -> groupBy -> count()`
/// with NO explicit result-store name, so the store name is minted from the
/// shared node-name counter. Pins the lowering's mint order against the JVM
/// (`Capture.java::kgroupedTableAutoNamed`), which explicit-store goldens cannot.
fn build_autonamed() -> crabka_client_streams::topology::BuiltTopology {
    use crabka_client_streams::dsl::StreamsBuilder;

    let b = StreamsBuilder::new();
    let src = b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_store("src-store"),
    );
    src.group_by_explicit(
        |_k: &String, v: &i64| {
            (
                if v % 2 == 0 {
                    "even".to_string()
                } else {
                    "odd".to_string()
                },
                *v,
            )
        },
        Grouped::with(StringSerde, I64Serde),
    )
    // No `.as_store(...)` → the result store name is auto-minted.
    .count_explicit(Materialized::with(StringSerde, I64Serde))
    .to_stream()
    .to_explicit("out", Produced::with(StringSerde, I64Serde));
    drop(src);
    b.build_optimized("app").unwrap()
}

#[test]
fn kgrouped_table_autonamed_topology_matches_jvm() {
    let built = build_autonamed();
    let actual = serde_json::to_value(built.to_wire()).unwrap();
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            "tests/testdata/golden/dsl/kgrouped_table_autonamed.topology.json",
        )
        .unwrap(),
    )
    .unwrap();
    assert2::assert!(actual == expected);
}

#[derive(serde::Deserialize, PartialEq, Debug)]
struct Row {
    key: String,
    value: i64,
}

#[test]
fn kgrouped_table_behavior_matches_jvm() {
    let built = build_combined();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    let s = StringSerde;
    for (k, v, ts) in [
        ("a", 2i64, 0),
        ("b", 4, 1),
        ("a", 6, 2),
        ("c", 3, 3),
        ("b", 5, 4),
        ("a", -1, 5),
    ] {
        d.pipe_input(
            "in",
            Consumed::with(s, I64Serde),
            Some(k.to_string()),
            v,
            ts,
        );
    }
    let golden: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/kgrouped_table/behavior.json").unwrap(),
    )
    .unwrap();
    // The golden keys count/reduce/aggregate map to sink topics count-out/reduce-out/agg-out.
    for (_name, golden_key, out) in [
        ("count output", "count", "count-out"),
        ("reduce output", "reduce", "reduce-out"),
        ("aggregate output", "aggregate", "agg-out"),
    ] {
        let mut got: Vec<Row> = Vec::new();
        while let Some((Some(k), v)) = d.read_output(out, Produced::with(StringSerde, I64Serde)) {
            got.push(Row { key: k, value: v });
        }
        let want: Vec<Row> = serde_json::from_value(golden[golden_key].clone()).unwrap();
        assert2::assert!(got == want);
    }
}
