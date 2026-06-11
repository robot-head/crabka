package crabka.capture;

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Captures the byte-exact {@code StreamsGroupHeartbeatRequest.Topology} (KIP-1071)
 * that Apache Kafka Streams 4.x would send to a streams-group coordinator, for five
 * DSL topologies built with {@code optimization=all}.
 *
 * <p><b>Capture mechanism (A — Kafka's own conversion, no broker):</b> we drive the
 * exact same private code path the JVM client uses to build the heartbeat:
 * <ol>
 *   <li>{@code StreamsBuilder.build(props)} with {@code TOPOLOGY_OPTIMIZATION_CONFIG=OPTIMIZE}
 *       produces an optimized {@link Topology}.</li>
 *   <li>Reflect the package-private {@code Topology.internalTopologyBuilder} field.</li>
 *   <li>Reflect {@code StreamThread.initBrokerTopology(StreamsConfig, InternalTopologyBuilder)}
 *       → {@code Map<String, StreamsRebalanceData.Subtopology>} (the streams-internal
 *       per-subtopology topic sets, keyed by the integer node-group index rendered as a
 *       decimal string — exactly what {@code StreamsRebalanceData} feeds the heartbeat).</li>
 *   <li>Reflect {@code StreamsGroupHeartbeatRequestManager.HeartbeatState
 *       .fromStreamsToHeartbeatRequest(Map)} → {@code List<StreamsGroupHeartbeatRequestData
 *       .Subtopology>} — the EXACT wire subtopologies (sorted source/sink topics, sorted
 *       changelog/repartition TopicInfo, copartition groups encoded as int16 indices). This
 *       is the literal method {@code StreamsGroupHeartbeatRequestManager} calls when JOINING.</li>
 * </ol>
 * We then render each wire {@code Subtopology} into Crabka's snake_case wire JSON shape
 * (matching {@code testdata/golden/single_source_sink.topology.json}) and write one file
 * per topology.
 *
 * <p>The integer node-group index → decimal-string subtopology id, the topic sorting, and
 * the copartition int16 index encoding are all done by Kafka's own code, not by us, so the
 * fixtures are ground truth.
 */
public final class Capture {

    private static final String APP_ID = "app";

    public static void main(String[] args) throws Exception {
        Path outDir = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(outDir);

        write(outDir, "stateless_chain", statelessChain());
        write(outDir, "count", count());
        write(outDir, "repartition_merge", repartitionMerge());
        write(outDir, "table_reuse", tableReuse());
        write(outDir, "branch_merge", branchMerge());
        write(outDir, "to_table", toTable());
        write(outDir, "stream_table_join", streamTableJoin());
        write(outDir, "ktable_ktable_join", ktableKtableJoin());
        write(outDir, "windowed_count", windowedCount());
        write(outDir, "stream_stream_join", streamStreamJoin());
        write(outDir, "stream_stream_outer_join", streamStreamOuterJoin());
        write(outDir, "session_count", sessionCount());
        write(outDir, "suppress_until_window_closes", suppressUntilWindowCloses());
        write(outDir, "suppress_until_window_closes_logged", suppressUntilWindowClosesLogged());
        write(outDir, "global_table_join", globalTableJoin());
        write(outDir, "process", processTopology());
        write(outDir, "process_values", processValuesTopology());
        write(outDir, "fk_join_inner", fkJoinInner());
        write(outDir, "fk_join_left", fkJoinLeft());
        write(outDir, "sliding_window_count", slidingWindowCount());
        write(outDir, "sliding_window_aggregate", slidingWindowAggregate());
        write(outDir, "versioned_table", versionedTable());
        write(outDir, "cogroup", cogroup());
        write(outDir, "cogroup_time", cogroupTime());
        write(outDir, "cogroup_sliding", cogroupSliding());
        write(outDir, "cogroup_session", cogroupSession());
        write(outDir, "kgrouped_table", kgroupedTable());

        System.out.println("Capture complete. Wrote 27 fixtures to " + outDir.toAbsolutePath());
    }

    // ---- the 5 DSL topologies (all with optimization=all) -------------------

    /** 1. stateless_chain: stream -> mapValues -> filter -> to. */
    static Topology statelessChain() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .mapValues(v -> v)
            .filter((k, v) -> true)
            .to("out");
        return b.build(optimizedProps());
    }

    /**
     * 9. windowed_count: stream -> groupByKey -> windowedBy(TimeWindows 60s, no grace)
     * -> count -> toStream -> to. The aggregate store is a window store, so its
     * changelog gets cleanup.policy=compact,delete + retention.ms = size+grace+1day.
     * No selectKey (no key change) → no repartition. The `count` path burns one
     * store-name counter index for backward topology compatibility.
     */
    static Topology windowedCount() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /** Sliding-window (KIP-450) count: stream -> groupByKey -> windowedBy(SlidingWindows) -> count -> toStream -> to. */
    static Topology slidingWindowCount() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.SlidingWindows.ofTimeDifferenceWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /** Sliding-window aggregate (no count name-burn): groupByKey -> windowedBy(SlidingWindows) -> aggregate -> toStream -> to. */
    static Topology slidingWindowAggregate() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.SlidingWindows.ofTimeDifferenceWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .aggregate(() -> 0L, (k, v, a) -> a + 1, Materialized.with(Serdes.String(), Serdes.Long()))
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /** Non-windowed cogroup: in1 (len) + in2 (constant) → aggregate → toStream → to. */
    static Topology cogroup() {
        StreamsBuilder b = new StreamsBuilder();
        KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
        KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
        KTable<String, Long> t = g1
            .<Long>cogroup((k, v, agg) -> agg + v.length())
            .cogroup(g2, (k, v, agg) -> agg + 1)
            .aggregate(() -> 0L,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("cg-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        t.toStream().to("out", Produced.with(Serdes.String(), Serdes.Long()));
        return b.build(optimizedProps());
    }

    /** Time-windowed cogroup. */
    static Topology cogroupTime() {
        StreamsBuilder b = new StreamsBuilder();
        KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
        KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
        KTable<Windowed<String>, Long> t = g1
            .<Long>cogroup((k, v, agg) -> agg + v.length())
            .cogroup(g2, (k, v, agg) -> agg + 1)
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(100)))
            .aggregate(() -> 0L,
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("cg-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        t.toStream().to("out", Produced.with(
            WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L), Serdes.Long()));
        return b.build(optimizedProps());
    }

    /** Sliding-windowed cogroup. */
    static Topology cogroupSliding() {
        StreamsBuilder b = new StreamsBuilder();
        KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
        KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
        KTable<Windowed<String>, Long> t = g1
            .<Long>cogroup((k, v, agg) -> agg + v.length())
            .cogroup(g2, (k, v, agg) -> agg + 1)
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(100)))
            .aggregate(() -> 0L,
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("cg-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        t.toStream().to("out", Produced.with(
            WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L), Serdes.Long()));
        return b.build(optimizedProps());
    }

    /** Session-windowed cogroup (note the session merger). */
    static Topology cogroupSession() {
        StreamsBuilder b = new StreamsBuilder();
        KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
        KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
        KTable<Windowed<String>, Long> t = g1
            .<Long>cogroup((k, v, agg) -> agg + v.length())
            .cogroup(g2, (k, v, agg) -> agg + 1)
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(100)))
            .aggregate(() -> 0L, (k, a, bb) -> a + bb,
                Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("cg-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        t.toStream().to("out", Produced.with(
            WindowedSerdes.sessionWindowedSerdeFrom(String.class), Serdes.Long()));
        return b.build(optimizedProps());
    }

    /**
     * kgrouped_table: table("in") -> filter(v > 0) -> groupBy(even/odd) ->
     * count + reduce(add,sub) + aggregate(add,sub) -> three sinks.
     * Exercises KTable.groupBy repartition + three KGroupedTable aggregations.
     */
    static Topology kgroupedTable() {
        StreamsBuilder b = new StreamsBuilder();
        KTable<String, Long> src = b.table("in",
            Consumed.with(Serdes.String(), Serdes.Long()),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("src-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        KTable<String, Long> pos = src.filter((k, v) -> v > 0,
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("filter-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        KGroupedTable<String, Long> grouped = pos.groupBy(
            (k, v) -> KeyValue.pair(v % 2 == 0 ? "even" : "odd", v),
            Grouped.with(Serdes.String(), Serdes.Long()));
        grouped.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream().to("count-out", Produced.with(Serdes.String(), Serdes.Long()));
        grouped.reduce((a, v) -> a + v, (a, v) -> a - v,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("reduce-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream().to("reduce-out", Produced.with(Serdes.String(), Serdes.Long()));
        grouped.aggregate(() -> 0L, (k, v, a) -> a + v, (k, v, a) -> a - v,
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("agg-store")
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream().to("agg-out", Produced.with(Serdes.String(), Serdes.Long()));
        return b.build(optimizedProps());
    }

    /** 2. count: stream -> selectKey -> groupByKey -> count -> toStream -> to. */
    static Topology count() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .selectKey((k, v) -> k)
            .groupByKey()
            .count()
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /**
     * 3. repartition_merge: one selectKey feeding TWO aggregations. Under optimization the
     * two aggregations share a single repartition topic.
     */
    static Topology repartitionMerge() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> s = b.<String, String>stream("in").selectKey((k, v) -> k);
        s.groupByKey().count();
        s.groupByKey().reduce((a, c) -> a);
        return b.build(optimizedProps());
    }

    /**
     * 4. table_reuse: builder.table with a materialized store. Source-topic-reuse means the
     * store's changelog should be the source topic {@code in}, not {@code app-store-changelog}.
     */
    static Topology tableReuse() {
        StreamsBuilder b = new StreamsBuilder();
        b.table("in", Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("store"))
            .mapValues(v -> v)
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /**
     * 6. to_table: materialize a KStream into a KTable via {@code toTable}, then back to a
     * stream and out. The key is unchanged through the source, so {@code toTable} must NOT
     * insert a repartition; the materialized store gets an implicit
     * {@code app-<store>-changelog}.
     */
    static Topology toTable() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .toTable(Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("store"))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 7. stream_table_join: stream("left").join(table("right", store), joiner).to("out").
     * The stream side and the table side are copartitioned, so the JVM places both sources,
     * the join, and the table source in ONE subtopology with a copartition group binding
     * "left" and "right", and an implicit {@code app-store-changelog} for the table store.
     */
    /**
     * 15. global_table_join: a KStream joined to a GlobalKTable by a key-mapper.
     * The global table is fully replicated (no copartition / repartition); the key-mapper
     * maps the stream value to the global lookup key. Pins how the KIP-1071 wire encodes
     * a global store (no dedicated global field in the Topology — capture-first).
     */
    static Topology globalTableJoin() {
        StreamsBuilder b = new StreamsBuilder();
        GlobalKTable<String, String> g = b.globalTable(
            "global",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("global-store"));
        b.stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .join(g, (k, v) -> v, (sv, gv) -> sv + gv)
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    static Topology streamTableJoin() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> left = b.stream("left", Consumed.with(Serdes.String(), Serdes.String()));
        org.apache.kafka.streams.kstream.KTable<String, String> right = b.table(
            "right",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("store"));
        left.join(right, (v, vt) -> v + vt)
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 10. stream_stream_join: stream("left").join(stream("right"), joiner, JoinWindows 60s).to("out").
     * Two retainDuplicates window stores (one per side); their changelogs are cleanup.policy=delete
     * (NOT compact,delete) + retention.ms = before+after+grace+1day = 60s+60s+0+1day. Copartition
     * binds "left" and "right". No outer store (inner join).
     */
    static Topology streamStreamJoin() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> left = b.stream("left", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> right = b.stream("right", Consumed.with(Serdes.String(), Serdes.String()));
        left.join(
                right,
                (a, c) -> a + c,
                org.apache.kafka.streams.kstream.JoinWindows.ofTimeDifferenceWithNoGrace(java.time.Duration.ofSeconds(60)),
                org.apache.kafka.streams.kstream.StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 11. stream_stream_outer_join: stream("left").outerJoin(stream("right"), joiner, JoinWindows 60s).to("out").
     * Like the inner join (two retainDuplicates window stores, cleanup.policy=delete changelogs,
     * copartition), but KIP-633 left/outer adds a SHARED outer-join store (KSTREAM-OUTERSHARED-)
     * that buffers non-matched records until their window closes. Its name/index and changelog
     * config are JVM ground truth — this fixture pins them.
     */
    static Topology streamStreamOuterJoin() {
        StreamsBuilder b = new StreamsBuilder();
        KStream<String, String> left = b.stream("left", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> right = b.stream("right", Consumed.with(Serdes.String(), Serdes.String()));
        left.outerJoin(
                right,
                (a, c) -> (a == null ? "" : a) + (c == null ? "" : c),
                org.apache.kafka.streams.kstream.JoinWindows.ofTimeDifferenceWithNoGrace(java.time.Duration.ofSeconds(60)),
                org.apache.kafka.streams.kstream.StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 8. ktable_ktable_join: table("a", sa).join(table("b", sb), joiner).toStream().to("out").
     * Both tables are materialized and copartitioned, so the JVM unions both table sources,
     * the two join processors (JOINTHIS/JOINOTHER), and the merge into ONE subtopology with a
     * copartition group binding "a" and "b". Under optimization=all REUSE_KTABLE_SOURCE_TOPICS
     * makes each store's changelog its own source topic ("a"/"b"). The join result is NOT
     * materialized (no result changelog).
     */
    static Topology ktableKtableJoin() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> a = b.table(
            "a",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sa"));
        org.apache.kafka.streams.kstream.KTable<String, String> bt = b.table(
            "b",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sb"));
        a.join(bt, (va, vb) -> va + vb)
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 18. fk_join_inner: KIP-213 many-to-one foreign-key KTable↔KTable INNER join.
     * {@code a = table("a", "sa"); bt = table("b", "sb");
     *  a.join(bt, fkExtractor=(va)->va, joiner=(va,vb)->va+vb, Materialized.with(...))
     *   .toStream().to("out")}.
     *
     * <p>Unlike the equi-join (#8), the FK-join is NOT copartitioned: it inserts two
     * internal repartition topics (subscription-registration + subscription-response)
     * and a subscription state store with its own changelog, splitting the pipeline
     * across TWO subtopologies. This fixture is the wire ground truth for those topic
     * names + the subscription store changelog config. The result is materialized
     * (Materialized.with) so the FK overload is well-formed, but the result store's
     * changelog reuses no source topic — captured as-is.
     */
    static Topology fkJoinInner() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> a = b.table(
            "a",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sa"));
        org.apache.kafka.streams.kstream.KTable<String, String> bt = b.table(
            "b",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sb"));
        a.join(
                bt,
                (java.util.function.Function<String, String>) (String va) -> va,
                (va, vb) -> va + vb,
                Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * Unoptimized ({@code b.build()}) inner FK-join, for the behavioral oracle
     * ({@link ForeignKeyJoinBehavior}) which drives a TopologyTestDriver. Same builder
     * shape as {@link #fkJoinInner()} but built WITHOUT optimization.
     */
    static Topology fkJoinInnerUnoptimized() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> a = b.table(
            "a",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sa"));
        org.apache.kafka.streams.kstream.KTable<String, String> bt = b.table(
            "b",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sb"));
        a.join(
                bt,
                (java.util.function.Function<String, String>) (String va) -> va,
                (va, vb) -> va + vb,
                Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build();
    }

    /** Unoptimized ({@code b.build()}) left FK-join, for the behavioral oracle. */
    static Topology fkJoinLeftUnoptimized() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> a = b.table(
            "a",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sa"));
        org.apache.kafka.streams.kstream.KTable<String, String> bt = b.table(
            "b",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sb"));
        a.leftJoin(
                bt,
                (java.util.function.Function<String, String>) (String va) -> va,
                (va, vb) -> va + (vb == null ? "_" : vb),
                Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build();
    }

    /**
     * 19. fk_join_left: identical to {@link #fkJoinInner()} but the LEFT FK-join, so a
     * primary-table record with no matching foreign value still emits {@code va + "_"}.
     * Pins whether left vs inner perturbs the wire topology (node indices / topic names).
     */
    static Topology fkJoinLeft() {
        StreamsBuilder b = new StreamsBuilder();
        org.apache.kafka.streams.kstream.KTable<String, String> a = b.table(
            "a",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sa"));
        org.apache.kafka.streams.kstream.KTable<String, String> bt = b.table(
            "b",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, org.apache.kafka.streams.state.KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("sb"));
        a.leftJoin(
                bt,
                (java.util.function.Function<String, String>) (String va) -> va,
                (va, vb) -> va + (vb == null ? "_" : vb),
                Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 12. session_count: stream -> groupByKey -> windowedBy(SessionWindows gap 60s)
     * -> count -> toStream -> to. Session store; changelog cleanup.policy=compact,delete
     * + retention.ms = gap + grace + 1day. Pins the session store name + changelog config.
     */
    static Topology sessionCount() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.SessionWindows.ofInactivityGapWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /**
     * 13. suppress_until_window_closes: windowed count + suppress(untilWindowCloses,
     * logging disabled). With logging disabled the suppress buffer adds no changelog,
     * so the wire is expected byte-identical to windowed_count.
     */
    static Topology suppressUntilWindowCloses() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .suppress(org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses(
                org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded()
                    .withLoggingDisabled()))
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /**
     * 14. suppress_until_window_closes_logged: identical to #13 but with the suppress
     * buffer's changelog ENABLED (the default). The suppress buffer's changelog topic
     * now appears in the wire topology — pins its name + config.
     */
    static Topology suppressUntilWindowClosesLogged() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .suppress(org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses(
                org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded()))
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }

    /**
     * 15. process: addStateStore + process(supplier, "store") -> to("out"). A custom
     * Processor-API node with a connected KV store. The store's changelog topic appears
     * in the wire (compact); the processor node kind/name is not wire-visible.
     */
    static Topology processTopology() {
        StreamsBuilder b = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, String>> sb = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("store"), Serdes.String(), Serdes.String());
        b.addStateStore(sb);
        b.<String, String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                public void process(org.apache.kafka.streams.processor.api.Record<String, String> r) {
                    context().forward(r);
                }
            }, "store")
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /**
     * 16. process_values: addStateStore + processValues(supplier, "store") -> to("out").
     * The fixed-key variant. Expected byte-identical to process here (same source/sink/store;
     * the node kind is not wire-visible).
     */
    static Topology processValuesTopology() {
        StreamsBuilder b = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, String>> sb = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("store"), Serdes.String(), Serdes.String());
        b.addStateStore(sb);
        b.<String, String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .processValues(() -> new ContextualFixedKeyProcessor<String, String, String>() {
                public void process(org.apache.kafka.streams.processor.api.FixedKeyRecord<String, String> r) {
                    context().forward(r);
                }
            }, "store")
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        return b.build(optimizedProps());
    }

    /** 5. branch_merge: split into two branches, merge them, then .to("out"). */
    static Topology branchMerge() {
        StreamsBuilder b = new StreamsBuilder();
        List<KStream<String, String>> captured = new ArrayList<>();
        BranchedKStream<String, String> split = b.<String, String>stream("in").split();
        Consumer<KStream<String, String>> grab = captured::add;
        split.branch((k, v) -> true, Branched.withConsumer(grab));
        split.branch((k, v) -> false, Branched.withConsumer(grab));
        split.noDefaultBranch();
        captured.get(0).merge(captured.get(1)).to("out");
        return b.build(optimizedProps());
    }

    private static Properties optimizedProps() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // optimization=all
        p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        return p;
    }

    // ---- capture: optimized Topology -> wire Subtopology list ----------------

    /**
     * Run Kafka's own DSL→heartbeat conversion and return the wire subtopologies.
     * Mirrors exactly what {@code StreamsGroupHeartbeatRequestManager} sends on JOIN.
     */
    @SuppressWarnings("unchecked")
    static List<StreamsGroupHeartbeatRequestData.Subtopology> wireSubtopologies(Topology topology)
            throws Exception {
        InternalTopologyBuilder itb = internalTopologyBuilder(topology);

        StreamsConfig config = new StreamsConfig(optimizedProps());

        // StreamThread.initBrokerTopology(StreamsConfig, InternalTopologyBuilder)
        //   -> Map<String, StreamsRebalanceData.Subtopology>
        Class<?> streamThread = Class.forName(
            "org.apache.kafka.streams.processor.internals.StreamThread");
        Method initBrokerTopology = streamThread.getDeclaredMethod(
            "initBrokerTopology", StreamsConfig.class, InternalTopologyBuilder.class);
        initBrokerTopology.setAccessible(true);
        Map<String, StreamsRebalanceData.Subtopology> rebalance =
            (Map<String, StreamsRebalanceData.Subtopology>) initBrokerTopology.invoke(
                null, config, itb);

        // StreamsGroupHeartbeatRequestManager.HeartbeatState
        //   .fromStreamsToHeartbeatRequest(Map<String, StreamsRebalanceData.Subtopology>)
        //   -> List<StreamsGroupHeartbeatRequestData.Subtopology>
        Class<?> heartbeatState = Class.forName(
            "org.apache.kafka.clients.consumer.internals."
                + "StreamsGroupHeartbeatRequestManager$HeartbeatState");
        Method fromStreams = heartbeatState.getDeclaredMethod(
            "fromStreamsToHeartbeatRequest", Map.class);
        fromStreams.setAccessible(true);
        return (List<StreamsGroupHeartbeatRequestData.Subtopology>)
            fromStreams.invoke(null, rebalance);
    }

    @SuppressWarnings("unchecked")
    private static InternalTopologyBuilder internalTopologyBuilder(Topology topology)
            throws Exception {
        Field f = Topology.class.getDeclaredField("internalTopologyBuilder");
        f.setAccessible(true);
        InternalTopologyBuilder itb = (InternalTopologyBuilder) f.get(topology);
        StreamsConfig config = new StreamsConfig(optimizedProps());

        // rewriteTopology sets the application id and applies optimizer-dependent
        // finalization (source-topic reuse, repartition merge) the same way KafkaStreams
        // does before building the runtime. subtopologyToTopicsInfo() requires the app id.
        Method rewrite = InternalTopologyBuilder.class.getDeclaredMethod(
            "rewriteTopology", StreamsConfig.class);
        rewrite.setAccessible(true);
        rewrite.invoke(itb, config);

        // Build every subtopology — exactly what StreamThread/TaskManager does when it
        // materializes the ProcessorTopology. This is the step that populates
        // storeToChangelogTopic (via buildProcessorNode), so that subtopologyToTopicsInfo()
        // reports the implicit changelog topics (e.g. the count store's changelog). Without
        // this the changelog set is empty. Building instantiates the (RocksDB) stores, so
        // rocksdbjni must be on the classpath.
        Method nodeGroups = InternalTopologyBuilder.class.getDeclaredMethod("nodeGroups");
        nodeGroups.setAccessible(true);
        Map<Integer, ?> groups = (Map<Integer, ?>) nodeGroups.invoke(itb);
        Method buildSubtopology = InternalTopologyBuilder.class.getDeclaredMethod(
            "buildSubtopology", int.class);
        buildSubtopology.setAccessible(true);
        for (Integer gid : groups.keySet()) {
            buildSubtopology.invoke(itb, gid);
        }
        return itb;
    }

    // ---- render to Crabka wire JSON shape ------------------------------------

    private static void write(Path outDir, String name,
                              Topology topology) throws Exception {
        List<StreamsGroupHeartbeatRequestData.Subtopology> subs = wireSubtopologies(topology);
        String json = toCrabkaWireJson(subs);
        Path file = outDir.resolve(name + ".topology.json");
        Files.writeString(file, json, StandardCharsets.UTF_8);
        System.out.println("wrote " + file + "\n" + json);
    }

    /**
     * Render the wire subtopologies into Crabka's snake_case wire JSON shape — the exact
     * field set and ordering of {@code single_source_sink.topology.json}:
     * {@code epoch} + {@code subtopologies[]} with
     * {@code subtopology_id, source_topics, source_topic_regex, repartition_sink_topics,
     * repartition_source_topics, state_changelog_topics, copartition_groups}.
     *
     * <p>Topic arrays are already sorted by Kafka's converter; we do not re-sort.
     * Epoch is the topology epoch (0 for a freshly built topology).
     */
    static String toCrabkaWireJson(List<StreamsGroupHeartbeatRequestData.Subtopology> subs) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"epoch\": 0,\n");
        sb.append("  \"subtopologies\": [");
        for (int i = 0; i < subs.size(); i++) {
            sb.append(i == 0 ? "\n" : ",\n");
            renderSubtopology(sb, subs.get(i), "    ");
        }
        sb.append(subs.isEmpty() ? "]\n" : "\n  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    private static void renderSubtopology(StringBuilder sb,
                                          StreamsGroupHeartbeatRequestData.Subtopology s,
                                          String ind) {
        sb.append(ind).append("{\n");
        sb.append(ind).append("  \"subtopology_id\": ").append(jsonStr(s.subtopologyId())).append(",\n");
        sb.append(ind).append("  \"source_topics\": ").append(jsonStrArray(s.sourceTopics())).append(",\n");
        sb.append(ind).append("  \"source_topic_regex\": ").append(jsonStrArray(s.sourceTopicRegex())).append(",\n");
        sb.append(ind).append("  \"repartition_sink_topics\": ").append(jsonStrArray(s.repartitionSinkTopics())).append(",\n");
        sb.append(ind).append("  \"repartition_source_topics\": ").append(topicInfoArray(s.repartitionSourceTopics(), ind + "  ")).append(",\n");
        sb.append(ind).append("  \"state_changelog_topics\": ").append(topicInfoArray(s.stateChangelogTopics(), ind + "  ")).append(",\n");
        sb.append(ind).append("  \"copartition_groups\": ").append(copartitionArray(s.copartitionGroups(), ind + "  ")).append("\n");
        sb.append(ind).append("}");
    }

    private static String topicInfoArray(List<StreamsGroupHeartbeatRequestData.TopicInfo> infos,
                                         String ind) {
        if (infos.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < infos.size(); i++) {
            StreamsGroupHeartbeatRequestData.TopicInfo t = infos.get(i);
            sb.append(i == 0 ? "\n" : ",\n");
            sb.append(ind).append("  {\n");
            sb.append(ind).append("    \"name\": ").append(jsonStr(t.name())).append(",\n");
            sb.append(ind).append("    \"partitions\": ").append(t.partitions()).append(",\n");
            sb.append(ind).append("    \"replication_factor\": ").append(t.replicationFactor()).append(",\n");
            sb.append(ind).append("    \"topic_configs\": ").append(keyValueArray(t.topicConfigs(), ind + "    ")).append("\n");
            sb.append(ind).append("  }");
        }
        sb.append("\n").append(ind).append("]");
        return sb.toString();
    }

    private static String keyValueArray(List<StreamsGroupHeartbeatRequestData.KeyValue> kvs,
                                        String ind) {
        if (kvs.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < kvs.size(); i++) {
            StreamsGroupHeartbeatRequestData.KeyValue kv = kvs.get(i);
            sb.append(i == 0 ? "\n" : ",\n");
            sb.append(ind).append("  { \"key\": ").append(jsonStr(kv.key()))
                .append(", \"value\": ").append(jsonStr(kv.value())).append(" }");
        }
        sb.append("\n").append(ind).append("]");
        return sb.toString();
    }

    private static String copartitionArray(List<StreamsGroupHeartbeatRequestData.CopartitionGroup> groups,
                                           String ind) {
        if (groups.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < groups.size(); i++) {
            StreamsGroupHeartbeatRequestData.CopartitionGroup g = groups.get(i);
            sb.append(i == 0 ? "\n" : ",\n");
            sb.append(ind).append("  {\n");
            sb.append(ind).append("    \"source_topics\": ").append(shortArray(g.sourceTopics())).append(",\n");
            sb.append(ind).append("    \"source_topic_regex\": ").append(shortArray(g.sourceTopicRegex())).append(",\n");
            sb.append(ind).append("    \"repartition_source_topics\": ").append(shortArray(g.repartitionSourceTopics())).append("\n");
            sb.append(ind).append("  }");
        }
        sb.append("\n").append(ind).append("]");
        return sb.toString();
    }

    // ---- tiny JSON helpers (deterministic, no external deps) -----------------

    private static String jsonStrArray(List<String> xs) {
        if (xs.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(jsonStr(xs.get(i)));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String shortArray(List<Short> xs) {
        if (xs == null || xs.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(xs.get(i));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String jsonStr(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append("\"");
        return sb.toString();
    }

    /**
     * 22. versioned_table: table("in", Consumed, Materialized.as(persistentVersionedKeyValueStore("vt", 600s)))
     * .toStream().to("out"). The versioned store's changelog carries
     * min.compaction.lag.ms = history_retention_ms + 86_400_000 (24h grace).
     * Built with optimization=all.
     */
    static Topology versionedTable() {
        StreamsBuilder b = new StreamsBuilder();
        b.table("in",
                Consumed.with(Serdes.String(), Serdes.Long()),
                Materialized.<String, Long>as(
                        Stores.persistentVersionedKeyValueStore("vt", Duration.ofMillis(600_000)))
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.Long()));
        return b.build(optimizedProps());
    }

    /**
     * Unoptimized versioned table topology for the behavioral oracle
     * ({@link VersionedTableBehavior}).
     */
    static Topology versionedTableUnoptimized() {
        StreamsBuilder b = new StreamsBuilder();
        b.table("in",
                Consumed.with(Serdes.String(), Serdes.Long()),
                Materialized.<String, Long>as(
                        Stores.persistentVersionedKeyValueStore("vt", Duration.ofMillis(600_000)))
                    .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.Long()));
        return b.build();
    }

    private Capture() {
    }
}
