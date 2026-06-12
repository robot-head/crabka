package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Capture-first behavioral + store-name oracle for a KStream-to-VersionedKTable
 * join WITH a stream-side grace period (KIP-923 {@code Joined.withGracePeriod}).
 *
 * <p>A versioned table ({@code persistentVersionedKeyValueStore("vt", history=10min)})
 * is inner-joined by a {@link KStream}. The join is configured with
 * {@code Joined.with(...).withGracePeriod(Duration.ofMillis(GRACE))}, GRACE strictly
 * less than the table history retention. The grace period makes the join BUFFER each
 * incoming stream record until stream-time advances past {@code record.ts + GRACE},
 * which both reorders out-of-order stream records and lets the as-of table lookup see
 * any table updates that land within the grace window.
 *
 * <p>The harness captures, in addition to the ordered {@code out} records:
 * <ul>
 *   <li>the BUFFER STORE NAME the JVM mints (a {@code KSTREAM-BUFFER-*} store, pulled
 *       from the optimized {@code describe()} string), and
 *   <li>that buffer store's CHANGELOG TOPIC NAME + its {@code topicConfigs}
 *       (cleanup.policy, retention.ms, ...) as the real KIP-1071 wire conversion emits
 *       them — these are constants a later Rust task pins.
 * </ul>
 *
 * App id is {@code app}; the table changelog is {@code app-vt-changelog}.
 */
public final class StreamTableGraceBehavior {
    private static final long GRACE_MS = 60_000L;          // 1 min, < 10 min history
    private static final long HISTORY_MS = 600_000L;       // 10 min

    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        Topology topo = buildTopology();
        // describe() is taken from the UNOPTIMIZED build for stable store names; the
        // changelog wire config is taken from the OPTIMIZED build via Capture.wireSubtopologies.
        String describe = topo.describe().toString();

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StringBuilder outJson = new StringBuilder("[");

        try (TopologyTestDriver driver = new TopologyTestDriver(topo, p, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, Long> tableIn = driver.createInputTopic(
                "table", Serdes.String().serializer(), Serdes.Long().serializer());
            TestInputTopic<String, Long> streamIn = driver.createInputTopic(
                "stream", Serdes.String().serializer(), Serdes.Long().serializer());
            TestOutputTopic<String, Long> outTopic = driver.createOutputTopic(
                "out", Serdes.String().deserializer(), Serdes.Long().deserializer());

            // Two table versions.
            tableIn.pipeInput("a", 10L, Instant.ofEpochMilli(100));
            tableIn.pipeInput("a", 20L, Instant.ofEpochMilli(200));

            // OUT-OF-ORDER stream records + one already-late record. The grace buffer
            // holds each record until stream-time advances GRACE past its ts, so the
            // emission order is reordered relative to arrival order. Arrival order:
            //   (a,1)@300, (a,1)@250, (a,1)@150, then a stream-time advance @100000.
            long[][] streamRecords = {
                {300L, 1L}, // arrives first, ts highest
                {250L, 1L}, // out of order (older than prior arrival)
                {150L, 1L}, // out of order again (as-of-150 -> table=10)
            };
            for (long[] rec : streamRecords) {
                streamIn.pipeInput("a", rec[1], Instant.ofEpochMilli(rec[0]));
                drain(outTopic, outJson);
            }
            // Advance stream-time well past every buffered record + GRACE to flush.
            streamIn.pipeInput("a", 9L, Instant.ofEpochMilli(1_000_000));
            drain(outTopic, outJson);
        }
        outJson.append("\n  ]");

        // Buffer store name from describe(); changelog config from the wire conversion.
        // The grace buffer is minted as a "<join-processor>-Buffer" store (e.g.
        // KSTREAM-JOIN-0000000003-Buffer), listed in the join processor's stores: [...].
        String bufferStore = extractBufferStore(describe);
        StringBuilder bufferChangelog = new StringBuilder("null");
        StringBuilder bufferConfigs = new StringBuilder("null");
        try {
            Topology optimized = buildTopology();
            List<org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Subtopology> subs =
                Capture.wireSubtopologies(optimized);
            for (var s : subs) {
                for (var ti : s.stateChangelogTopics()) {
                    if (ti.name().contains("BUFFER") || (bufferStore != null && ti.name().contains(bufferStore))) {
                        bufferChangelog.setLength(0);
                        bufferChangelog.append(quote(ti.name()));
                        bufferConfigs.setLength(0);
                        bufferConfigs.append("{");
                        boolean firstCfg = true;
                        for (var kv : ti.topicConfigs()) {
                            if (!firstCfg) bufferConfigs.append(", ");
                            firstCfg = false;
                            bufferConfigs.append(quote(kv.key())).append(": ").append(quote(kv.value()));
                        }
                        bufferConfigs.append("}");
                    }
                }
            }
        } catch (Throwable t) {
            bufferChangelog.setLength(0);
            bufferChangelog.append(quote("ERROR: " + t));
        }

        StringBuilder doc = new StringBuilder();
        doc.append("{\n");
        doc.append("  \"scenario\": \"stream_table_grace\",\n");
        doc.append("  \"grace_ms\": ").append(GRACE_MS).append(",\n");
        doc.append("  \"history_retention_ms\": ").append(HISTORY_MS).append(",\n");
        doc.append("  \"buffer_store_name\": ").append(quote(bufferStore)).append(",\n");
        doc.append("  \"buffer_changelog_topic\": ").append(bufferChangelog).append(",\n");
        doc.append("  \"buffer_changelog_configs\": ").append(bufferConfigs).append(",\n");
        doc.append("  \"out\": ").append(outJson).append(",\n");
        doc.append("  \"describe\": ").append(quote(describe)).append("\n");
        doc.append("}\n");

        Files.writeString(out.resolve("grace.json"), doc.toString());
        System.out.println("grace out:\n" + outJson);
        System.out.println("grace buffer_store_name: " + bufferStore);
        System.out.println("grace buffer_changelog: " + bufferChangelog);
        System.out.println("grace buffer_configs: " + bufferConfigs);
        System.out.println("grace describe:\n" + describe);
    }

    private static Topology buildTopology() {
        StreamsBuilder b = new StreamsBuilder();
        KTable<String, Long> table = b.table(
            "table",
            Consumed.with(Serdes.String(), Serdes.Long()),
            Materialized.<String, Long>as(
                    Stores.persistentVersionedKeyValueStore("vt", Duration.ofMillis(HISTORY_MS)))
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        KStream<String, Long> stream = b.stream(
            "stream", Consumed.with(Serdes.String(), Serdes.Long()));
        stream.join(
                table,
                (sv, tv) -> tv + sv,
                Joined.with(Serdes.String(), Serdes.Long(), Serdes.Long())
                    .withGracePeriod(Duration.ofMillis(GRACE_MS)))
            .to("out", Produced.with(Serdes.String(), Serdes.Long()));
        return b.build();
    }

    private static void drain(TestOutputTopic<String, Long> outTopic, StringBuilder outJson) {
        for (TestRecord<String, Long> r : outTopic.readRecordsToList()) {
            if (outJson.length() > 1) outJson.append(",");
            outJson.append("\n    { \"key\": ").append(quote(r.key()))
                .append(", \"value\": ")
                .append(r.value() == null ? "null" : r.value().toString())
                .append(", \"ts\": ").append(r.timestamp())
                .append(" }");
        }
    }

    /**
     * Pull the grace buffer store name (a {@code <join>-Buffer} store) out of the
     * describe() string. Finds the {@code -Buffer} marker and walks backward/forward
     * over the store-name characters to recover the full token.
     */
    private static String extractBufferStore(String s) {
        int marker = s.indexOf("-Buffer");
        if (marker < 0) return null;
        int end = marker + "-Buffer".length();
        int start = marker;
        while (start > 0) {
            char c = s.charAt(start - 1);
            if (Character.isLetterOrDigit(c) || c == '-' || c == '_') {
                start--;
            } else {
                break;
            }
        }
        return s.substring(start, end);
    }

    private static String quote(String s) {
        if (s == null) return "null";
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

    private StreamTableGraceBehavior() {
    }
}
