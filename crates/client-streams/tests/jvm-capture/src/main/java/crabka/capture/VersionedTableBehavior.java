package crabka.capture;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Capture-first behavioral + changelog oracle for KIP-1071 streams-client
 * versioned KTables (KIP-889 / 914).
 *
 * <p>Drives a {@link TopologyTestDriver} over the UNOPTIMIZED versioned-table
 * topology ({@code table("in", Materialized.as(persistentVersionedKeyValueStore))
 * .toStream().to("out")}) feeding a fixed battery that includes an out-of-order
 * record and a tombstone, then dumps:
 *
 * <ul>
 *   <li>{@code behavioral/versioned_table.json} — the ordered records emitted on
 *       {@code out} ({@code toStream} drops tombstones), as
 *       {@code {key, value, ts}}. The Rust runtime must reproduce this sequence.
 *   <li>{@code behavioral/versioned_changelog.json} — the changelog records the
 *       driver produced for {@code app-vt-changelog}, as
 *       {@code {keyHex, valueHex, ts}}. This pins the JVM's exact changelog byte
 *       format (whether the version timestamp rides in the value header or only
 *       in the record timestamp field).
 * </ul>
 *
 * App id is {@code app}; the changelog topic is therefore {@code app-vt-changelog}.
 */
public final class VersionedTableBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Path behavioral = out.resolve("behavioral");
        Files.createDirectories(behavioral);

        // (key, value, ts). Tombstone (null-value) source records are a
        // pre-existing DSL-wide limitation in Crabka (the KV KTable source path
        // also takes a non-null value), so this oracle exercises in-order +
        // out-of-order PUTs only. Values are Long to match the Rust I64Serde.
        Object[][] battery = {
            {"k", 10L, 100L},
            {"k", 20L, 200L},
            {"k", 15L, 150L}, // out-of-order
            {"k", 30L, 300L},
            {"j", 5L, 120L},
        };

        Topology topo = Capture.versionedTableUnoptimized();
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StringBuilder outJson = new StringBuilder("[");
        StringBuilder clJson = new StringBuilder("[");
        boolean firstOut = true;
        boolean firstCl = true;

        try (TopologyTestDriver driver = new TopologyTestDriver(topo, p, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, Long> in = driver.createInputTopic(
                "in", Serdes.String().serializer(), Serdes.Long().serializer());
            TestOutputTopic<String, Long> outTopic = driver.createOutputTopic(
                "out", Serdes.String().deserializer(), new LongDeserializer());
            TestOutputTopic<byte[], byte[]> clTopic = driver.createOutputTopic(
                "app-vt-changelog", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            for (Object[] row : battery) {
                String key = (String) row[0];
                Long value = (Long) row[1];
                long ts = (Long) row[2];
                in.pipeInput(key, value, Instant.ofEpochMilli(ts));

                for (TestRecord<String, Long> r : outTopic.readRecordsToList()) {
                    if (!firstOut) outJson.append(",");
                    firstOut = false;
                    outJson.append("\n    { \"key\": ").append(quote(r.key()))
                        .append(", \"value\": ")
                        .append(r.value() == null ? "null" : r.value().toString())
                        .append(", \"ts\": ").append(r.timestamp())
                        .append(" }");
                }
                for (TestRecord<byte[], byte[]> r : clTopic.readRecordsToList()) {
                    if (!firstCl) clJson.append(",");
                    firstCl = false;
                    clJson.append("\n    { \"keyHex\": ")
                        .append(r.key() == null ? "null" : quote(hex(r.key())))
                        .append(", \"valueHex\": ")
                        .append(r.value() == null ? "null" : quote(hex(r.value())))
                        .append(", \"ts\": ").append(r.timestamp())
                        .append(" }");
                }
            }
        }
        outJson.append("\n  ]\n");
        clJson.append("\n  ]\n");

        Files.writeString(behavioral.resolve("versioned_table.json"), outJson.toString());
        Files.writeString(behavioral.resolve("versioned_changelog.json"), clJson.toString());
        System.out.println("versioned_table out:\n" + outJson);
        System.out.println("versioned_table changelog:\n" + clJson);
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
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

    private VersionedTableBehavior() {
    }
}
