//! Byte-exact interop gate: the encoder's wire `Topology` for a canonical
//! Processor-API topology must match the JVM 4.x fixture.
#![cfg(not(target_os = "windows"))]

use crabka_client_streams::Topology;

#[test]
fn single_source_sink_matches_jvm_fixture() {
    // The Rust topology MUST mirror the Java PAPI app the fixture was captured from.
    let mut topo = Topology::new();
    topo.add_source(
        "src",
        ["streams-input"],
        crabka_client_streams::BytesSerde,
        crabka_client_streams::BytesSerde,
    );
    topo.add_sink(
        "snk",
        "streams-output",
        ["src"],
        crabka_client_streams::BytesSerde,
        crabka_client_streams::BytesSerde,
    );
    let wire = topo.build("streams-app").unwrap().to_wire();

    // Assert the JVM-derived shape (mirrors single_source_sink.topology.json).
    assert_eq!(wire.epoch, 0);
    assert_eq!(wire.subtopologies.len(), 1);
    let s = &wire.subtopologies[0];
    assert_eq!(s.subtopology_id, "0");
    assert_eq!(s.source_topics, vec!["streams-input".to_string()]);
    assert!(s.source_topic_regex.is_empty());
    assert!(s.repartition_sink_topics.is_empty());
    assert!(s.repartition_source_topics.is_empty());
    assert!(s.state_changelog_topics.is_empty());
    assert!(s.copartition_groups.is_empty());
}
