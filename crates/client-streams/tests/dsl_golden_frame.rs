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
