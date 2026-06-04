//! DSL golden frame: the wire `Topology` the DSL lowers to must byte-match the
//! captured JVM 4.x fixture for the same logical pipeline.
#![cfg(not(target_os = "windows"))]
use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{Consumed, Produced, StringSerde};

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
fn stateless_chain_matches_jvm() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .map_values(|v: &String| v.clone())
        .filter(|_k: &String, _v: &String| true)
        .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stateless_chain");
}

#[test]
fn count_matches_jvm() {
    use crabka_client_streams::{Grouped, I64Serde, Materialized};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .select_key(|k: &String, _v: &String| k.clone())
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .count(Materialized::with(StringSerde, I64Serde)) // UNNAMED → store = KSTREAM-AGGREGATE-STATE-STORE-0000000002
        .to_stream()
        .to("out", Produced::with(StringSerde, I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "count");
}

#[test]
fn repartition_merge_matches_jvm() {
    use crabka_client_streams::{Grouped, I64Serde, Materialized};
    // The JVM `repartitionMerge()` app: one `selectKey` feeds TWO bare aggregations
    // (no toStream/to). Under `optimization=all` the two repartitions collapse into
    // a single repartition topic, named after the FIRST aggregation's store.
    let b = StreamsBuilder::new();
    let s = b
        .stream(["in"], Consumed::with(StringSerde, StringSerde))
        .select_key(|k: &String, _v: &String| k.clone());
    // First aggregation: count → store KSTREAM-AGGREGATE-STATE-STORE-0000000002.
    drop(
        s.group_by_key(Grouped::with(StringSerde, StringSerde))
            .count(Materialized::with(StringSerde, I64Serde)),
    );
    // Second aggregation: reduce → store KSTREAM-REDUCE-STATE-STORE-0000000007.
    drop(
        s.group_by_key(Grouped::with(StringSerde, StringSerde))
            .reduce(
                |a: &String, _b: &String| a.clone(),
                Materialized::with(StringSerde, StringSerde),
            ),
    );
    drop(s); // release the shared Rc so build_optimized can unwrap it
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "repartition_merge");
}
