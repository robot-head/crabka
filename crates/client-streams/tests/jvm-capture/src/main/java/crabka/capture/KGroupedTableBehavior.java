package crabka.capture;

import java.nio.file.*;
import java.util.*;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.ChangedSerializer;

/**
 * Behavioral + ChangedSerializer byte golden for KTable.groupBy / KGroupedTable.
 * Topology: table("in") -> filter(v > 0) -> groupBy(key = v % 2, value = v)
 *   -> count / reduce(sum, diff) / aggregate(0; +v; -v).
 * The filter exercises the downstream-tombstone subtract path (a row whose value
 * drops to <= 0 emits Change{new:null}).
 */
public final class KGroupedTableBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "../testdata/kgrouped_table");
        Files.createDirectories(out);

        StreamsBuilder b = new StreamsBuilder();
        KTable<String, Long> src = b.table("in",
            Consumed.with(Serdes.String(), Serdes.Long()),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("src-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
        KTable<String, Long> pos = src.filter((k, v) -> v > 0);

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

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        try (TopologyTestDriver d = new TopologyTestDriver(b.build(), props, java.time.Instant.ofEpochMilli(0))) {
            TestInputTopic<String, Long> in = d.createInputTopic(
                "in", Serdes.String().serializer(), Serdes.Long().serializer());
            TestOutputTopic<String, Long> countOut = d.createOutputTopic(
                "count-out", Serdes.String().deserializer(), Serdes.Long().deserializer());
            TestOutputTopic<String, Long> reduceOut = d.createOutputTopic(
                "reduce-out", Serdes.String().deserializer(), Serdes.Long().deserializer());
            TestOutputTopic<String, Long> aggOut = d.createOutputTopic(
                "agg-out", Serdes.String().deserializer(), Serdes.Long().deserializer());

            in.pipeInput("a", 2L, java.time.Instant.ofEpochMilli(0));
            in.pipeInput("b", 4L, java.time.Instant.ofEpochMilli(1));
            in.pipeInput("a", 6L, java.time.Instant.ofEpochMilli(2));
            in.pipeInput("c", 3L, java.time.Instant.ofEpochMilli(3));
            in.pipeInput("b", 5L, java.time.Instant.ofEpochMilli(4));
            in.pipeInput("a", -1L, java.time.Instant.ofEpochMilli(5));

            StringBuilder sb = new StringBuilder("{\n");
            sb.append("  \"count\": ").append(dump(countOut)).append(",\n");
            sb.append("  \"reduce\": ").append(dump(reduceOut)).append(",\n");
            sb.append("  \"aggregate\": ").append(dump(aggOut)).append("\n}\n");
            Files.writeString(out.resolve("behavior.json"), sb.toString());
        }

        ChangedSerializer<Long> cs = new ChangedSerializer<>(Serdes.Long().serializer());
        Map<String, Change<Long>> samples = new LinkedHashMap<>();
        samples.put("both", new Change<>(6L, 2L));
        samples.put("new_only", new Change<>(5L, null));
        samples.put("old_only", new Change<>(null, 4L));
        StringBuilder hb = new StringBuilder("{\n");
        int i = 0;
        for (Map.Entry<String, Change<Long>> e : samples.entrySet()) {
            byte[] bytes = cs.serialize("topic", e.getValue());
            hb.append("  \"").append(e.getKey()).append("\": \"").append(hex(bytes)).append("\"");
            hb.append(++i < samples.size() ? ",\n" : "\n");
        }
        hb.append("}\n");
        Files.writeString(out.resolve("changed_bytes.json"), hb.toString());
    }

    private static String dump(TestOutputTopic<String, Long> t) {
        StringBuilder sb = new StringBuilder("[");
        List<KeyValue<String, Long>> recs = t.readKeyValuesToList();
        for (int i = 0; i < recs.size(); i++) {
            KeyValue<String, Long> kv = recs.get(i);
            if (kv.value == null) {
                sb.append("{\"key\": \"").append(kv.key).append("\", \"value\": null}");
            } else {
                sb.append("{\"key\": \"").append(kv.key).append("\", \"value\": ").append(kv.value).append("}");
            }
            if (i + 1 < recs.size()) sb.append(", ");
        }
        return sb.append("]").toString();
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte x : b) sb.append(String.format("%02x", x));
        return sb.toString();
    }
}
