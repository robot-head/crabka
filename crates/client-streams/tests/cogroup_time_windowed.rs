//! KIP-150 time-windowed cogroup — JVM 4.1 wire-topology + behavioral goldens.
use crabka_client_streams::{
    Consumed, I64Serde, Materialized, Produced, StringSerde, TimeWindowedSerde, TimeWindows,
    dsl::StreamsBuilder,
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
fn cogroup_time_matches_jvm() {
    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by(TimeWindows::of_size(100))
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg-store"),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 100), I64Serde),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "cogroup_time");
}

#[test]
fn cogroup_time_matches_jvm_behavior() {
    #[derive(serde::Deserialize, PartialEq, Debug)]
    struct Row {
        key: String,
        #[serde(rename = "windowStart")]
        window_start: i64,
        #[serde(rename = "windowEnd")]
        window_end: i64,
        value: i64,
    }

    let b = StreamsBuilder::new();
    let g1 = b.stream::<String, String>(["in1"]).group_by_key();
    let g2 = b.stream::<String, String>(["in2"]).group_by_key();
    g1.cogroup::<i64, _>(|_k, v: &String, acc| acc + i64::try_from(v.len()).unwrap_or(i64::MAX))
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .windowed_by(TimeWindows::of_size(100))
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg-store"),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(TimeWindowedSerde::new(StringSerde, 100), I64Serde),
        );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    // Same interleaved script as CogroupBehavior.java cogroupTime block.
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
    while let Some((Some(wk), v)) = d.read_output(
        "out",
        Produced::with(TimeWindowedSerde::new(StringSerde, 100), I64Serde),
    ) {
        got.push(Row {
            key: wk.key,
            window_start: wk.window.start,
            window_end: wk.window.end,
            value: v,
        });
    }
    let golden: Vec<Row> = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/cogroup/behavior_time.json").unwrap(),
    )
    .unwrap();
    assert_eq!(
        got, golden,
        "cogroup-time output sequence != JVM behavioral golden"
    );
}
