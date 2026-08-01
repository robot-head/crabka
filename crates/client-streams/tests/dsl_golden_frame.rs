//! DSL golden frame: the wire `Topology` the DSL lowers to must byte-match the
//! captured JVM 4.x fixture for the same logical pipeline.
use crabka_client_streams::{Consumed, Produced, StringSerde, dsl::StreamsBuilder};
use crabka_units::prelude::*;

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
    b.stream::<String, String>(["in"])
        .map_values(|v: &String| v.clone())
        .filter(|_k: &String, _v: &String| true)
        .to("out");
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stateless_chain");
}

#[test]
fn windowed_count_matches_jvm() {
    use crabka_client_streams::{I64Serde, Materialized, TimeWindowedSerde, TimeWindows};
    // Mirrors Capture.java `windowedCount()`:
    //   stream("in").groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(60s)).count()
    //     .toStream().to("out")
    //
    // No selectKey → no key change → no repartition. The aggregate store is a
    // WINDOW store (auto-named), so its changelog gets cleanup.policy=compact,delete
    // + retention.ms = 60_000 + 0 + 86_400_000 = 86_460_000. Store lands at index 1
    // (source=0, store=1). UNNAMED store → KSTREAM-AGGREGATE-STATE-STORE-0000000001.
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "windowed_count");
}

#[test]
fn stream_stream_join_matches_jvm() {
    use crabka_client_streams::{JoinWindows, StreamJoined, StringSerde};
    // Mirrors Capture.java `streamStreamJoin()`:
    //   stream("left").join(stream("right"), (a,c)->a+c, JoinWindows 60s, StreamJoined).to("out")
    //
    // One subtopology, source_topics ["left","right"], copartition [0,1]. Two
    // retainDuplicates window stores named after the JVM join processors —
    // KSTREAM-JOINTHIS-0000000004-store / KSTREAM-JOINOTHER-0000000005-store (the
    // lowering burns two KSTREAM-WINDOWED- indices so the join processors land at
    // 4/5). Each store's changelog is cleanup.policy=delete (NOT compact,delete) +
    // retention.ms = 60_000 + 60_000 + 0 + 86_400_000 = 86_520_000. No outer store.
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.join(
        &right,
        |a: &String, c: &String| format!("{a}{c}"),
        JoinWindows::of(millis(60_000)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stream_stream_join");
}

#[test]
fn stream_stream_outer_join_matches_jvm() {
    use crabka_client_streams::{JoinWindows, StreamJoined, StringSerde};
    // Mirrors Capture.java `streamStreamOuterJoin()`:
    //   stream("left").outerJoin(stream("right"), (a,c)->a+c, JoinWindows 60s, StreamJoined).to("out")
    //
    // Like the inner join but KIP-633 left/outer renames the per-side join
    // processors (THIS → KSTREAM-OUTERTHIS-0000000004, OTHER →
    // KSTREAM-OUTEROTHER-0000000005) and adds a SHARED outer-join KV store whose
    // name reuses the THIS index: KSTREAM-OUTERSHARED-0000000004-store. The two
    // window-store changelogs stay cleanup.policy=delete + retention.ms=86520000;
    // the shared store's changelog is cleanup.policy=compact (a KV changelog).
    // Three changelogs, sorted by name: OUTEROTHER < OUTERSHARED < OUTERTHIS.
    let b = StreamsBuilder::new();
    let left = b.stream::<String, String>(["left"]);
    let right = b.stream::<String, String>(["right"]);
    left.outer_join(
        &right,
        |a: Option<&String>, c: Option<&String>| {
            format!(
                "{}{}",
                a.cloned().unwrap_or_default(),
                c.cloned().unwrap_or_default()
            )
        },
        JoinWindows::of(millis(60_000)),
        StreamJoined::with(StringSerde, StringSerde, StringSerde),
    )
    .to("out");
    drop(left);
    drop(right);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stream_stream_outer_join");
}

#[test]
fn session_count_matches_jvm() {
    use crabka_client_streams::{I64Serde, Materialized, SessionWindowedSerde, SessionWindows};
    // Mirrors Capture.java `sessionCount()`:
    //   stream("in").groupByKey().windowedBy(SessionWindows gap 60s).count().toStream().to("out")
    //
    // Session store (the third typed store), auto-named at the JVM aggregate-store
    // counter position with the `count` name-burn; changelog cleanup.policy=
    // compact,delete + retention.ms = gap 60s + 0 grace + 1 day. No selectKey → no
    // repartition. The wire topology is the same shape as windowed_count (session
    // vs time window is not wire-visible) — this pins the session lowering.
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_session(SessionWindows::of_inactivity_gap(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "session_count");
}

#[test]
fn suppress_until_window_closes_matches_jvm() {
    use crabka_client_streams::{
        BufferConfig, I64Serde, Materialized, Suppressed, TimeWindowedSerde, TimeWindows,
    };
    // Mirrors Capture.java `suppressUntilWindowCloses()` (logging DISABLED). With the
    // suppress buffer's changelog off it adds no topic → the wire is byte-identical to
    // windowed_count (the suppress processor is not wire-visible, and the aggregate
    // store naming/counter is unperturbed). Pins that a logging-off suppress introduces
    // no spurious topic. (Default logging is ON as of slice D — see fixture #14.)
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(
            Suppressed::until_window_closes(BufferConfig::unbounded()).with_logging_disabled(),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "suppress_until_window_closes");
}

#[test]
fn suppress_until_window_closes_logged_matches_jvm() {
    use crabka_client_streams::{
        BufferConfig, I64Serde, Materialized, Suppressed, TimeWindowedSerde, TimeWindows,
    };
    // Mirrors Capture.java `suppressUntilWindowClosesLogged()` — identical to the #13
    // app but with the suppress buffer's changelog ENABLED (the slice-D default). The
    // buffer's changelog topic now appears in the wire:
    // `app-KTABLE-SUPPRESS-STATE-STORE-0000000004-changelog` (a plain compacted KV
    // changelog). Pins the suppress store name (consecutive index after the processor)
    // + the changelog config.
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by(TimeWindows::of_size(millis(60_000)))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "suppress_until_window_closes_logged");
}

#[test]
fn count_matches_jvm() {
    use crabka_client_streams::{I64Serde, Materialized};
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .select_key(|k: &String, _v: &String| k.clone())
        .group_by_key()
        .count_explicit(Materialized::with(StringSerde, I64Serde)) // UNNAMED → store = KSTREAM-AGGREGATE-STATE-STORE-0000000002
        .to_stream()
        .to("out");
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "count");
}

#[test]
fn table_reuse_matches_jvm() {
    // The JVM `tableReuse()` app: `builder.table("in", "store")`
    // followed by a NON-materialized `mapValues`, then `.toStream().to("out")`.
    // Under `optimization=all` the REUSE_KTABLE_SOURCE_TOPICS pass makes the
    // table store's changelog the SOURCE topic ("in") instead of
    // "app-store-changelog", and the non-materialized mapValues adds no store —
    // so the single subtopology carries exactly one changelog topic named "in".
    let b = StreamsBuilder::new();
    b.table::<String, String>("in", "store")
        .map_values(|v: &String| v.clone()) // NON-materialized
        .to_stream()
        .to("out");
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
    let src = b.stream::<String, String>(["in"]);
    let split = src.split();
    let b1 = split.branch(|_k: &String, _v: &String| true);
    let b2 = split.branch(|_k: &String, _v: &String| false);
    b1.merge(&b2).to("out");
    drop(b1);
    drop(b2);
    drop(src);
    drop(split);
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "branch_merge");
}

#[test]
fn to_table_matches_jvm() {
    // Mirrors Capture.java `toTable()`:
    //   stream("in").toTable("store").toStream().to("out")
    //
    // The key is unchanged through the source, so `toTable` must NOT insert a
    // repartition. The materialized store ("store") gets an implicit
    // `app-store-changelog` (compact KV-store changelog). Wire result: ONE
    // subtopology "0", source_topics=["in"], one changelog "app-store-changelog".
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .to_table("store")
        .to_stream()
        .to("out");
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "to_table");
}

#[test]
fn process_matches_jvm() {
    use crabka_client_streams::{Processor, ProcessorContext, Record};
    // Mirrors a JVM `addStateStore("store") + process(supplier, "store") + to("out")`
    // app: ONE subtopology "0", source "in", and the connected store's compact
    // `app-store-changelog`. The Fwd processor just forwards each record unchanged;
    // the store connection (not its use) is what surfaces the changelog topic.
    struct Fwd;
    #[async_trait::async_trait]
    impl Processor<String, String, String, String> for Fwd {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(r);
        }
    }
    let b = StreamsBuilder::new();
    b.add_state_store("store", StringSerde, StringSerde);
    b.stream::<String, String>(["in"])
        .process(|| Fwd, ["store"])
        .to("out");
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "process");
}

#[test]
fn process_values_matches_jvm() {
    use crabka_client_streams::{FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyRecord};
    // Mirrors a JVM `addStateStore("store") + processValues(supplier, "store") +
    // to("out")` app: ONE subtopology "0", source "in", and the connected store's
    // compact `app-store-changelog`. processValues preserves the key, so the wire is
    // byte-identical to the `process` fixture (per the T1 capture). The FixedFwd
    // processor forwards each record unchanged; the store connection (not its use) is
    // what surfaces the changelog topic.
    struct FixedFwd;
    #[async_trait::async_trait]
    impl FixedKeyProcessor<String, String, String> for FixedFwd {
        async fn process(
            &mut self,
            ctx: &mut FixedKeyProcessorContext<'_, '_, '_, String, String>,
            r: FixedKeyRecord<String, String>,
        ) {
            ctx.forward(r);
        }
    }
    let b = StreamsBuilder::new();
    b.add_state_store("store", StringSerde, StringSerde);
    b.stream::<String, String>(["in"])
        .process_values(|| FixedFwd, ["store"])
        .to("out");
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "process_values");
}

#[test]
fn stream_table_join_matches_jvm() {
    // Mirrors Capture.java `streamTableJoin()`:
    //   stream("left").join(table("right", "store"), (v,vt)->v+vt).to("out")
    //
    // Both the stream source ("left") and the table source ("right") land in ONE
    // subtopology (the join's `connect_processor_store` unions the join with the
    // table store), with a copartition group binding "left" and "right" as the
    // int16 indices [0, 1] into the sorted source_topics. Under optimization=all
    // (`build_optimized`) REUSE_KTABLE_SOURCE_TOPICS makes the "store" changelog
    // the source topic "right" — matching the JVM ground truth.
    let b = StreamsBuilder::new();
    let table = b.table::<String, String>("right", "store");
    b.stream::<String, String>(["left"])
        .join_table(&table, |v: &String, vt: &String| format!("{v}{vt}"))
        .to("out");
    drop(table);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stream_table_join");
}

#[test]
fn ktable_ktable_join_matches_jvm() {
    // Mirrors Capture.java `ktableKtableJoin()`:
    //   table("a", "sa").join(table("b", "sb"), (va,vb)->va+vb).toStream().to("out")
    //
    // Both table sources ("a","b"), the two join processors (JOINTHIS reads "sb",
    // JOINOTHER reads "sa"), and the merge land in ONE subtopology: each join's
    // `connect_processor_store` unions it with the store it reads, and the merge's
    // predecessor edges union the rest. A copartition group binds "a" and "b" as the
    // int16 indices [0, 1] into the sorted source_topics. Under optimization=all
    // (`build_optimized`) REUSE_KTABLE_SOURCE_TOPICS makes each store's changelog its
    // own source topic ("a"/"b"). The join result is unmaterialized — no result
    // changelog.
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("a", "sa");
    let tb = b.table::<String, String>("b", "sb");
    ta.join(&tb, |va: &String, vb: &String| format!("{va}{vb}"))
        .to_stream()
        .to("out");
    drop(ta);
    drop(tb);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "ktable_ktable_join");
}

#[test]
fn repartition_merge_matches_jvm() {
    use crabka_client_streams::{I64Serde, Materialized};
    // The JVM `repartitionMerge()` app: one `selectKey` feeds TWO bare aggregations
    // (no toStream/to). Under `optimization=all` the two repartitions collapse into
    // a single repartition topic, named after the FIRST aggregation's store.
    let b = StreamsBuilder::new();
    let s = b
        .stream::<String, String>(["in"])
        .select_key(|k: &String, _v: &String| k.clone());
    // First aggregation: count → store KSTREAM-AGGREGATE-STATE-STORE-0000000002.
    drop(
        s.group_by_key()
            .count_explicit(Materialized::with(StringSerde, I64Serde)),
    );
    // Second aggregation: reduce → store KSTREAM-REDUCE-STATE-STORE-0000000007.
    drop(s.group_by_key().reduce_explicit(
        |a: &String, _b: &String| a.clone(),
        Materialized::with(StringSerde, StringSerde),
    ));
    drop(s); // release the shared Rc so build_optimized can unwrap it
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "repartition_merge");
}

#[test]
fn global_table_join_matches_jvm() {
    use crabka_client_streams::{GlobalKTable, Materialized};
    // Mirrors Capture.java `globalTableJoin()`: a KStream joined to a GlobalKTable
    // by a key-mapper. The global store/source/processor are INVISIBLE in the wire
    // but the global source consumes subtopology index 0, so the stream subtopology
    // emits as id "1". One subtopology, source_topics=["in"], everything else empty.
    let b = StreamsBuilder::new();
    let g: GlobalKTable<String, String> = b.global_table_explicit(
        "global",
        Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde),
    );
    b.stream::<String, String>(["in"])
        .join_global(
            &g,
            |_k: &String, v: &String| v.clone(),
            |sv: &String, gv: &String| format!("{sv}{gv}"),
        )
        .to("out");
    drop(g); // release the GlobalKTable's Rc clone so build can unwrap it
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "global_table_join");
}

#[test]
fn fk_join_inner_matches_jvm() {
    // Mirrors the JVM `--fkjoin` capture: two source tables `a`/`b` (stores sa/sb),
    // an inner foreign-key join with fk extractor = identity on the left value,
    // joiner va+vb, fk serde = String. Result `.toStream().to("out")`.
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("a", "sa");
    let tb = b.table::<String, String>("b", "sb");
    ta.join_on_foreign_key(
        &tb,
        |va: &String| va.clone(),
        |va: &String, vb: &String| format!("{va}{vb}"),
        StringSerde,
    )
    .to_stream()
    .to("out");
    drop(ta);
    drop(tb);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "fk_join_inner");
}

#[test]
fn sliding_window_count_matches_jvm() {
    use crabka_client_streams::{
        I64Serde, Materialized, Produced, SlidingWindows, TimeWindowedSerde,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(
            60_000,
        )))
        .count_explicit(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "sliding_window_count");
}

#[test]
fn sliding_window_aggregate_matches_jvm() {
    use crabka_client_streams::{
        I64Serde, Materialized, Produced, SlidingWindows, TimeWindowedSerde,
    };
    let b = StreamsBuilder::new();
    b.stream::<String, String>(["in"])
        .group_by_key()
        .windowed_by_sliding(SlidingWindows::of_time_difference_with_no_grace(millis(
            60_000,
        )))
        .aggregate_explicit(
            || 0i64,
            |_k: &String, _v: &String, a: i64| a + 1,
            Materialized::with(StringSerde, I64Serde),
        )
        .to_stream()
        .to_explicit(
            "out",
            Produced::with(
                TimeWindowedSerde::new(StringSerde, millis(60_000)),
                I64Serde,
            ),
        );
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "sliding_window_aggregate");
}

#[test]
fn fk_join_left_matches_jvm() {
    // Like the inner FK join but `left_join_on_foreign_key`. The wire topology is
    // byte-identical to the inner golden (the left/inner difference is runtime
    // instruction/joiner only, not graph shape).
    let b = StreamsBuilder::new();
    let ta = b.table::<String, String>("a", "sa");
    let tb = b.table::<String, String>("b", "sb");
    ta.left_join_on_foreign_key(
        &tb,
        |va: &String| va.clone(),
        |va: &String, vb: Option<&String>| format!("{va}{}", vb.map_or("_", |s| s.as_str())),
        StringSerde,
    )
    .to_stream()
    .to("out");
    drop(ta);
    drop(tb);
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "fk_join_left");
}

#[test]
fn versioned_table_matches_jvm() {
    use crabka_client_streams::{I64Serde, Materialized, StringSerde};
    // Mirrors Capture.java `versionedTable()`:
    //   table("in", Consumed, Materialized.as(persistentVersionedKeyValueStore("vt", 600s)))
    //     .toStream().to("out")
    // Under optimization=all the JVM reuses the source topic `in` as the store's
    // changelog (REUSE_KTABLE_SOURCE_TOPICS); the changelog config carries
    // cleanup.policy=compact, message.timestamp.type=CreateTime,
    // min.compaction.lag.ms = 600_000 + 86_400_000 = 87_000_000.
    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
    )
    .to_stream()
    .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "versioned_table");
}
