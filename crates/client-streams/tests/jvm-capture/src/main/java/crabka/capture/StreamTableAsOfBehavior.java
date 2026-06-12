package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
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
import java.util.Properties;

/**
 * Capture-first behavioral oracle for a KStream-to-VersionedKTable "as-of" join
 * (KIP-889 / KIP-914).
 *
 * <p>A versioned table ({@code persistentVersionedKeyValueStore("vt", history=10min)})
 * is joined (inner) by a {@link KStream}. Because the table is versioned, the join
 * looks up the table value <em>as of the stream record's timestamp</em> rather than
 * "latest". The battery exercises the three diagnostic cases:
 *
 * <ul>
 *   <li>table {@code (a,10)@ts=100}, {@code (a,20)@ts=200} establish two versions;
 *   <li>stream {@code (a,1)@150} → as-of-150 the table value is 10 → output {@code 11};
 *   <li>stream {@code (a,1)@250} → as-of-250 the table value is 20 → output {@code 21};
 *   <li>stream {@code (a,1)@50}  → as-of-50 PREDATES the first version → inner join
 *       produces NO output.
 * </ul>
 *
 * <p>Output value = tableValue + streamValue (both Long). Serializes the optimized
 * topology {@code describe()} string plus the ordered {@code out} records
 * {@code {key, value, ts}}.
 *
 * App id is {@code app}; the versioned store changelog is {@code app-vt-changelog}.
 */
public final class StreamTableAsOfBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        StreamsBuilder b = new StreamsBuilder();
        KTable<String, Long> table = b.table(
            "table",
            Consumed.with(Serdes.String(), Serdes.Long()),
            Materialized.<String, Long>as(
                    Stores.persistentVersionedKeyValueStore("vt", Duration.ofMillis(600_000)))
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        KStream<String, Long> stream = b.stream(
            "stream", Consumed.with(Serdes.String(), Serdes.Long()));
        stream.join(table, (sv, tv) -> tv + sv)
            .to("out", Produced.with(Serdes.String(), Serdes.Long()));
        Topology topo = b.build();
        String describe = topo.describe().toString();

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StringBuilder outJson = new StringBuilder("[");
        boolean first = true;

        try (TopologyTestDriver driver = new TopologyTestDriver(topo, p, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, Long> tableIn = driver.createInputTopic(
                "table", Serdes.String().serializer(), Serdes.Long().serializer());
            TestInputTopic<String, Long> streamIn = driver.createInputTopic(
                "stream", Serdes.String().serializer(), Serdes.Long().serializer());
            TestOutputTopic<String, Long> outTopic = driver.createOutputTopic(
                "out", Serdes.String().deserializer(), Serdes.Long().deserializer());

            // Establish two table versions FIRST so the as-of lookups have history.
            tableIn.pipeInput("a", 10L, Instant.ofEpochMilli(100));
            tableIn.pipeInput("a", 20L, Instant.ofEpochMilli(200));

            // Stream probes (each annotated with its expectation).
            long[][] streamRecords = {
                {150L, 1L}, // as-of 150 -> table=10 -> expect 11
                {250L, 1L}, // as-of 250 -> table=20 -> expect 21
                {50L, 1L},  // as-of 50  -> predates first version -> expect NO output
            };
            for (long[] rec : streamRecords) {
                long ts = rec[0];
                long val = rec[1];
                streamIn.pipeInput("a", val, Instant.ofEpochMilli(ts));
                for (TestRecord<String, Long> r : outTopic.readRecordsToList()) {
                    if (!first) outJson.append(",");
                    first = false;
                    outJson.append("\n    { \"key\": ").append(quote(r.key()))
                        .append(", \"value\": ")
                        .append(r.value() == null ? "null" : r.value().toString())
                        .append(", \"ts\": ").append(r.timestamp())
                        .append(" }");
                }
            }
        }
        outJson.append("\n  ]");

        StringBuilder doc = new StringBuilder();
        doc.append("{\n");
        doc.append("  \"scenario\": \"stream_table_asof\",\n");
        doc.append("  \"history_retention_ms\": 600000,\n");
        doc.append("  \"out\": ").append(outJson).append(",\n");
        doc.append("  \"describe\": ").append(quote(describe)).append("\n");
        doc.append("}\n");

        Files.writeString(out.resolve("asof.json"), doc.toString());
        System.out.println("asof out:\n" + outJson);
        System.out.println("asof describe:\n" + describe);
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

    private StreamTableAsOfBehavior() {
    }
}
