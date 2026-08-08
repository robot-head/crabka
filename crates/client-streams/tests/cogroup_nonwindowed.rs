//! KIP-150 non-windowed cogroup: the JVM 4.1 wire-topology and behavioral
//! goldens.
use crabka_client_streams::{
    Consumed, I64Serde, Materialized, Produced, StringSerde, dsl::StreamsBuilder,
};

fn assert_matches_fixture(wire: &crabka_client_streams::topology::WireTopology, fixture: &str) {
    let path = format!("tests/testdata/golden/dsl/{fixture}.topology.json");
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}")),
    )
    .unwrap();
    let actual = serde_json::to_value(wire).unwrap();
    assert_eq!(actual, expected, "wire topology != JVM fixture {fixture}");
}

#[test]
fn cogroup_matches_jvm() {
    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg-store"),
        )
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "cogroup");
}

#[test]
fn cogroup_matches_jvm_behavior() {
    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Row {
        key: String,
        value: i64,
    }

    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg-store"),
        )
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Same interleaved script as CogroupBehavior.java.
    let s = StringSerde;
    d.pipe_input(
        "in1",
        Consumed::with(s, s),
        Some("a".into()),
        "xx".into(),
        0,
    );
    d.pipe_input("in2", Consumed::with(s, s), Some("a".into()), "z".into(), 1);
    d.pipe_input("in1", Consumed::with(s, s), Some("a".into()), "y".into(), 2);
    d.pipe_input(
        "in1",
        Consumed::with(s, s),
        Some("b".into()),
        "qqqq".into(),
        3,
    );
    d.pipe_input("in2", Consumed::with(s, s), Some("b".into()), "z".into(), 4);

    let mut got: Vec<Row> = Vec::new();
    while let Some((Some(k), v)) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        got.push(Row { key: k, value: v });
    }
    let golden: Vec<Row> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/cogroup/behavior.json").unwrap(),
    )
    .unwrap();
    assert_eq!(
        got, golden,
        "cogroup output sequence != JVM behavioral golden"
    );
}

/// The default-serde `aggregate(init, store_name)` convenience, which takes no
/// explicit `Materialized`, folds across both inputs the same as
/// `aggregate_explicit`.
#[test]
fn cogroup_default_serde_aggregate_runs() {
    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate(|| 0i64, "cg-store")
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    let s = StringSerde;
    d.pipe_input(
        "in1",
        Consumed::with(s, s),
        Some("a".into()),
        "xx".into(),
        0,
    );
    d.pipe_input("in2", Consumed::with(s, s), Some("a".into()), "z".into(), 1);
    let mut got: Vec<(String, i64)> = Vec::new();
    while let Some((Some(k), v)) = d.read_output("out", Produced::with(StringSerde, I64Serde)) {
        got.push((k, v));
    }
    // in1 "xx" (len 2) → 2; in2 "z" (+1) → 3.
    assert_eq!(got, vec![("a".to_string(), 2), ("a".to_string(), 3)]);
}

/// `Materialized::with_logging(false)` registers the shared cogroup store with no
/// changelog, so the wire topology has no `*-cg-store-changelog` entry.
#[test]
fn cogroup_logging_false_omits_changelog() {
    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde)
                .as_store("cg-store")
                .with_logging(false),
        )
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    let json = serde_json::to_string(&serde_json::to_value(&wire).unwrap()).unwrap();
    assert!(
        !json.contains("cg-store-changelog"),
        "with_logging(false) must omit the shared-store changelog topic"
    );
}
