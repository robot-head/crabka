package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
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
import java.util.Properties;

/**
 * Capture-first behavioral oracle for an inner join between TWO versioned KTables
 * (KIP-889 / KIP-914).
 *
 * <p>Both sides are {@code persistentVersionedKeyValueStore(history=10min)}. With
 * versioned tables, a KTable-KTable join SUPPRESSES updates whose record timestamp is
 * older than the latest version already in the store for that key (an out-of-order
 * update does not become the "current" value, so it produces NO new join result).
 * In-order updates produce a join result against the other side's current value.
 *
 * <p>Battery:
 * <ul>
 *   <li>{@code a:(k,1)@100}, {@code b:(k,2)@100} → in-order, both current → join {@code 1|2};
 *   <li>{@code a:(k,3)@200} → in-order update of a → join {@code 3|2};
 *   <li>{@code a:(k,9)@150} → OUT-OF-ORDER (150 < latest validFrom 200) → NO new join result.
 * </ul>
 *
 * <p>Output value = {@code aValue + "|" + bValue} (Strings). Serializes the optimized
 * topology {@code describe()} plus the ordered {@code out} records {@code {key, value, ts}}.
 */
public final class TableTableVersionedBehavior {
    private static final long HISTORY_MS = 600_000L;

    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        StreamsBuilder b = new StreamsBuilder();
        KTable<String, String> a = b.table(
            "a",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(
                    Stores.persistentVersionedKeyValueStore("va", Duration.ofMillis(HISTORY_MS)))
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        KTable<String, String> bt = b.table(
            "b",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String>as(
                    Stores.persistentVersionedKeyValueStore("vb", Duration.ofMillis(HISTORY_MS)))
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.String()));
        a.join(bt, (va, vb) -> va + "|" + vb)
            .toStream()
            .to("out", Produced.with(Serdes.String(), Serdes.String()));
        Topology topo = b.build();
        String describe = topo.describe().toString();

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StringBuilder outJson = new StringBuilder("[");

        try (TopologyTestDriver driver = new TopologyTestDriver(topo, p, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> aIn = driver.createInputTopic(
                "a", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> bIn = driver.createInputTopic(
                "b", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outTopic = driver.createOutputTopic(
                "out", Serdes.String().deserializer(), Serdes.String().deserializer());

            aIn.pipeInput("k", "1", Instant.ofEpochMilli(100));   // a current = 1
            drain(outTopic, outJson);
            bIn.pipeInput("k", "2", Instant.ofEpochMilli(100));   // in-order -> join 1|2
            drain(outTopic, outJson);
            aIn.pipeInput("k", "3", Instant.ofEpochMilli(200));   // in-order update -> join 3|2
            drain(outTopic, outJson);
            aIn.pipeInput("k", "9", Instant.ofEpochMilli(150));   // out-of-order -> NO new join result
            drain(outTopic, outJson);
        }
        outJson.append("\n  ]");

        StringBuilder doc = new StringBuilder();
        doc.append("{\n");
        doc.append("  \"scenario\": \"table_table_versioned\",\n");
        doc.append("  \"history_retention_ms\": ").append(HISTORY_MS).append(",\n");
        doc.append("  \"out\": ").append(outJson).append(",\n");
        doc.append("  \"describe\": ").append(quote(describe)).append("\n");
        doc.append("}\n");

        Files.writeString(out.resolve("tabletable.json"), doc.toString());
        System.out.println("tabletable out:\n" + outJson);
        System.out.println("tabletable describe:\n" + describe);
    }

    private static void drain(TestOutputTopic<String, String> outTopic, StringBuilder outJson) {
        for (TestRecord<String, String> r : outTopic.readRecordsToList()) {
            if (outJson.length() > 1) outJson.append(",");
            outJson.append("\n    { \"key\": ").append(quote(r.key()))
                .append(", \"value\": ").append(quote(r.value()))
                .append(", \"ts\": ").append(r.timestamp())
                .append(" }");
        }
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

    private TableTableVersionedBehavior() {
    }
}
