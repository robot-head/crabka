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
fn table_reuse_matches_jvm() {
    use crabka_client_streams::Materialized;
    // The JVM `tableReuse()` app: `builder.table("in", Materialized.as("store"))`
    // followed by a NON-materialized `mapValues`, then `.toStream().to("out")`.
    // Under `optimization=all` the REUSE_KTABLE_SOURCE_TOPICS pass makes the
    // table store's changelog the SOURCE topic ("in") instead of
    // "app-store-changelog", and the non-materialized mapValues adds no store —
    // so the single subtopology carries exactly one changelog topic named "in".
    let b = StreamsBuilder::new();
    b.table(
        "in",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("store"),
    )
    .map_values(|v: &String| v.clone()) // NON-materialized
    .to_stream()
    .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "table_reuse");
}

#[test]
fn branch_merge_matches_jvm() {
    // Mirrors Capture.java `branchMerge()`:
    //   stream("in").split()
    //     .branch((k,v)->true, grab)
    //     .branch((k,v)->false, grab)
    //     .noDefaultBranch()
    //   captured[0].merge(captured[1]).to("out")
    //
    // Wire result: ONE subtopology "0", source_topics=["in"], everything else empty.
    // Branch/merge are stateless (no internal/repartition/changelog topics).
    let b = StreamsBuilder::new();
    let src = b.stream(["in"], Consumed::with(StringSerde, StringSerde));
    let split = src.split();
    let b1 = split.branch(|_k: &String, _v: &String| true);
    let b2 = split.branch(|_k: &String, _v: &String| false);
    b1.merge(&b2)
        .to("out", Produced::with(StringSerde, StringSerde));
    drop(b1);
    drop(b2);
    drop(src);
    drop(split);
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "branch_merge");
}

#[test]
fn to_table_matches_jvm() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    // Mirrors Capture.java `toTable()`:
    //   stream("in").toTable(Materialized.as("store")).toStream().to("out")
    //
    // The key is unchanged through the source, so `toTable` must NOT insert a
    // repartition. The materialized store ("store") gets an implicit
    // `app-store-changelog` (compact KV-store changelog). Wire result: ONE
    // subtopology "0", source_topics=["in"], one changelog "app-store-changelog".
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .to_table(Materialized::with(StringSerde, StringSerde).as_store("store"))
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "to_table");
}

#[test]
fn stream_table_join_matches_jvm() {
    use crabka_client_streams::{Materialized, StringSerde};
    // Mirrors Capture.java `streamTableJoin()`:
    //   stream("left").join(table("right", Materialized.as("store")), (v,vt)->v+vt).to("out")
    //
    // Both the stream source ("left") and the table source ("right") land in ONE
    // subtopology (the join's `connect_processor_store` unions the join with the
    // table store), with a copartition group binding "left" and "right" as the
    // int16 indices [0, 1] into the sorted source_topics. Under optimization=all
    // (`build_optimized`) REUSE_KTABLE_SOURCE_TOPICS makes the "store" changelog
    // the source topic "right" — matching the JVM ground truth.
    let b = StreamsBuilder::new();
    let table = b.table::<String, String, _, _>(
        "right",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("store"),
    );
    b.stream(["left"], Consumed::with(StringSerde, StringSerde))
        .join(&table, |v: &String, vt: &String| format!("{v}{vt}"))
        .to("out", Produced::with(StringSerde, StringSerde));
    drop(table);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stream_table_join");
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
