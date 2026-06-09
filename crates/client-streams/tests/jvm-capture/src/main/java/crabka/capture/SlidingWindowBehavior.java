package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Capture-first behavior pin for KIP-450 sliding-window count and reduce: runs
 * the JVM {@link TopologyTestDriver} with two sliding-window topologies, drives
 * a fixed out-of-order script, and writes the exact emission sequences to
 * {@code behavior.json} (count) and {@code behavior_reduce.json} (reduce). This
 * pins the JVM KStreamSlidingWindowAggregate/Reduce algorithm
 * (processInOrder + out-of-order path) as ground truth for the Rust port.
 */
public final class SlidingWindowBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        // ── Count topology ──────────────────────────────────────────────────
        {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-behavior");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder b = new StreamsBuilder();
            b.<String, String>stream("in")
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(10)))
                .count()
                .toStream()
                .to("out", Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L),
                    Serdes.Long()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), props, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in = driver.createInputTopic(
                        "in", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<Windowed<String>, Long> outTopic = driver.createOutputTopic(
                        "out",
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L).deserializer(),
                        Serdes.Long().deserializer());

                // Drive fixed out-of-order script: (key, timestamp_ms), value always "v"
                Object[][] script = {
                    {"a", 0L},
                    {"a", 5L},
                    {"a", 12L},
                    {"a", 3L},
                    {"b", 7L},
                    {"a", 30L},
                };
                for (Object[] row : script) {
                    in.pipeInput((String) row[0], "v", Instant.ofEpochMilli((Long) row[1]));
                }

                List<org.apache.kafka.streams.KeyValue<Windowed<String>, Long>> records =
                    outTopic.readKeyValuesToList();

                StringBuilder sb = new StringBuilder("[\n");
                for (int i = 0; i < records.size(); i++) {
                    org.apache.kafka.streams.KeyValue<Windowed<String>, Long> kv = records.get(i);
                    long ws = kv.key.window().start();
                    long we = kv.key.window().end();
                    String key = kv.key.key();
                    long value = kv.value;
                    sb.append("  {\"key\": \"").append(key).append("\", ")
                      .append("\"windowStart\": ").append(ws).append(", ")
                      .append("\"windowEnd\": ").append(we).append(", ")
                      .append("\"value\": ").append(value).append("}");
                    sb.append(i + 1 < records.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior.json"), sb.toString());
                System.out.println("sliding window count behavior:\n" + sb);
            }
        }

        // ── Reduce topology ─────────────────────────────────────────────────
        {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-reduce-behavior");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder b = new StreamsBuilder();
            b.<String, String>stream("in")
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(10)))
                .reduce((a, v) -> a + "|" + v)
                .toStream()
                .to("out", Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L),
                    Serdes.String()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), props, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in = driver.createInputTopic(
                        "in", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<Windowed<String>, String> outTopic = driver.createOutputTopic(
                        "out",
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L).deserializer(),
                        Serdes.String().deserializer());

                // Same input script as count: (key, timestamp_ms), value always "v"
                Object[][] script = {
                    {"a", 0L},
                    {"a", 5L},
                    {"a", 12L},
                    {"a", 3L},
                    {"b", 7L},
                    {"a", 30L},
                };
                for (Object[] row : script) {
                    in.pipeInput((String) row[0], "v", Instant.ofEpochMilli((Long) row[1]));
                }

                List<org.apache.kafka.streams.KeyValue<Windowed<String>, String>> records =
                    outTopic.readKeyValuesToList();

                StringBuilder sb = new StringBuilder("[\n");
                for (int i = 0; i < records.size(); i++) {
                    org.apache.kafka.streams.KeyValue<Windowed<String>, String> kv = records.get(i);
                    long ws = kv.key.window().start();
                    long we = kv.key.window().end();
                    String key = kv.key.key();
                    String value = kv.value;
                    sb.append("  {\"key\": \"").append(key).append("\", ")
                      .append("\"windowStart\": ").append(ws).append(", ")
                      .append("\"windowEnd\": ").append(we).append(", ")
                      .append("\"value\": \"").append(value).append("\"}");
                    sb.append(i + 1 < records.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior_reduce.json"), sb.toString());
                System.out.println("sliding window reduce behavior:\n" + sb);
            }
        }
    }
}
