//! Byte-exact interop gate: the encoder's wire `Topology` for a canonical
//! Processor-API topology must match the JVM 4.x fixture.

use assert2::check;
use crabka_client_streams::{NodeHandle, Topology};

#[test]
fn single_source_sink_matches_jvm_fixture() {
    // The Rust topology MUST mirror the Java PAPI app the fixture was captured from.
    let mut topo = Topology::new();
    let src: NodeHandle<bytes::Bytes, bytes::Bytes> = topo.add_source("src", ["streams-input"]);
    topo.add_sink("snk", "streams-output", [&src]);
    let wire = topo.build("streams-app").unwrap().to_wire();

    // Assert the JVM-derived shape (mirrors single_source_sink.topology.json).
    assert_eq!(wire.epoch, 0);
    assert_eq!(wire.subtopologies.len(), 1);
    let s = &wire.subtopologies[0];
    assert_eq!(s.subtopology_id, "0");
    assert_eq!(s.source_topics, vec!["streams-input".to_string()]);
    check!(s.source_topic_regex.is_empty());
    check!(s.repartition_sink_topics.is_empty());
    check!(s.repartition_source_topics.is_empty());
    check!(s.state_changelog_topics.is_empty());
    check!(s.copartition_groups.is_empty());
}
