package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Capture-first behavior pin for KIP-150 cogroup: drives a
 * {@link TopologyTestDriver} over each of the four cogroup topologies
 * (non-windowed, time-windowed, sliding-windowed, session-windowed) with a
 * fixed two-topic input script and writes the emission sequence to JSON.
 */
public final class CogroupBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogroup-behavior");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // ── Non-windowed cogroup ───────────────────────────────────────────
        {
            StreamsBuilder b = new StreamsBuilder();
            KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
            KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
            g1.<Long>cogroup((k, v, agg) -> agg + v.length())
              .cogroup(g2, (k, v, agg) -> agg + 1)
              .aggregate(() -> 0L,
                  Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("cg-store")
                      .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
              .toStream().to("out", Produced.with(Serdes.String(), Serdes.Long()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), props, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in1 = driver.createInputTopic(
                    "in1", Serdes.String().serializer(), Serdes.String().serializer());
                TestInputTopic<String, String> in2 = driver.createInputTopic(
                    "in2", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<String, Long> outT = driver.createOutputTopic(
                    "out", Serdes.String().deserializer(), Serdes.Long().deserializer());

                // interleaved: (topic, key, value, ts)
                in1.pipeInput("a", "xx", Instant.ofEpochMilli(0));   // +2
                in2.pipeInput("a", "z", Instant.ofEpochMilli(1));    // +1
                in1.pipeInput("a", "y", Instant.ofEpochMilli(2));    // +1
                in1.pipeInput("b", "qqqq", Instant.ofEpochMilli(3)); // +4
                in2.pipeInput("b", "z", Instant.ofEpochMilli(4));    // +1

                StringBuilder sb = new StringBuilder("[\n");
                List<KeyValue<String, Long>> recs = outT.readKeyValuesToList();
                for (int i = 0; i < recs.size(); i++) {
                    KeyValue<String, Long> kv = recs.get(i);
                    sb.append("  {\"key\": \"").append(kv.key).append("\", \"value\": ").append(kv.value).append("}");
                    sb.append(i + 1 < recs.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior.json"), sb.toString());
                System.out.println("cogroup non-windowed behavior:\n" + sb);
            }
        }

        // ── Time-windowed cogroup ──────────────────────────────────────────
        {
            Properties tProps = new Properties();
            tProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogroup-time-behavior");
            tProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            tProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            tProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder b = new StreamsBuilder();
            KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
            KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
            g1.<Long>cogroup((k, v, agg) -> agg + v.length())
              .cogroup(g2, (k, v, agg) -> agg + 1)
              .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(100)))
              .aggregate(() -> 0L,
                  Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("cg-store")
                      .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
              .toStream().to("out", Produced.with(
                  WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L), Serdes.Long()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), tProps, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in1 = driver.createInputTopic(
                    "in1", Serdes.String().serializer(), Serdes.String().serializer());
                TestInputTopic<String, String> in2 = driver.createInputTopic(
                    "in2", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<Windowed<String>, Long> outT = driver.createOutputTopic(
                    "out",
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L).deserializer(),
                    Serdes.Long().deserializer());

                in1.pipeInput("a", "xx", Instant.ofEpochMilli(0));
                in2.pipeInput("a", "z", Instant.ofEpochMilli(1));
                in1.pipeInput("a", "y", Instant.ofEpochMilli(2));
                in1.pipeInput("b", "qqqq", Instant.ofEpochMilli(3));
                in2.pipeInput("b", "z", Instant.ofEpochMilli(4));

                StringBuilder sb = new StringBuilder("[\n");
                List<KeyValue<Windowed<String>, Long>> recs = outT.readKeyValuesToList();
                for (int i = 0; i < recs.size(); i++) {
                    KeyValue<Windowed<String>, Long> kv = recs.get(i);
                    long ws = kv.key.window().start();
                    long we = kv.key.window().end();
                    sb.append("  {\"key\": \"").append(kv.key.key()).append("\", ")
                      .append("\"windowStart\": ").append(ws).append(", ")
                      .append("\"windowEnd\": ").append(we).append(", ")
                      .append("\"value\": ").append(kv.value).append("}");
                    sb.append(i + 1 < recs.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior_time.json"), sb.toString());
                System.out.println("cogroup time-windowed behavior:\n" + sb);
            }
        }

        // ── Sliding-windowed cogroup ───────────────────────────────────────
        {
            Properties sProps = new Properties();
            sProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogroup-sliding-behavior");
            sProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            sProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            sProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder b = new StreamsBuilder();
            KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
            KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
            g1.<Long>cogroup((k, v, agg) -> agg + v.length())
              .cogroup(g2, (k, v, agg) -> agg + 1)
              .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(100)))
              .aggregate(() -> 0L,
                  Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("cg-store")
                      .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
              .toStream().to("out", Produced.with(
                  WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L), Serdes.Long()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), sProps, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in1 = driver.createInputTopic(
                    "in1", Serdes.String().serializer(), Serdes.String().serializer());
                TestInputTopic<String, String> in2 = driver.createInputTopic(
                    "in2", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<Windowed<String>, Long> outT = driver.createOutputTopic(
                    "out",
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 100L).deserializer(),
                    Serdes.Long().deserializer());

                in1.pipeInput("a", "xx", Instant.ofEpochMilli(0));
                in2.pipeInput("a", "z", Instant.ofEpochMilli(1));
                in1.pipeInput("a", "y", Instant.ofEpochMilli(2));
                in1.pipeInput("b", "qqqq", Instant.ofEpochMilli(3));
                in2.pipeInput("b", "z", Instant.ofEpochMilli(4));

                StringBuilder sb = new StringBuilder("[\n");
                List<KeyValue<Windowed<String>, Long>> recs = outT.readKeyValuesToList();
                for (int i = 0; i < recs.size(); i++) {
                    KeyValue<Windowed<String>, Long> kv = recs.get(i);
                    long ws = kv.key.window().start();
                    long we = kv.key.window().end();
                    sb.append("  {\"key\": \"").append(kv.key.key()).append("\", ")
                      .append("\"windowStart\": ").append(ws).append(", ")
                      .append("\"windowEnd\": ").append(we).append(", ")
                      .append("\"value\": ").append(kv.value).append("}");
                    sb.append(i + 1 < recs.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior_sliding.json"), sb.toString());
                System.out.println("cogroup sliding-windowed behavior:\n" + sb);
            }
        }

        // ── Session-windowed cogroup ───────────────────────────────────────
        {
            Properties sessProps = new Properties();
            sessProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogroup-session-behavior");
            sessProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
            sessProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            sessProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder b = new StreamsBuilder();
            KGroupedStream<String, String> g1 = b.<String, String>stream("in1").groupByKey();
            KGroupedStream<String, String> g2 = b.<String, String>stream("in2").groupByKey();
            g1.<Long>cogroup((k, v, agg) -> agg + v.length())
              .cogroup(g2, (k, v, agg) -> agg + 1)
              .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(100)))
              .aggregate(() -> 0L, (k, a, bb) -> a + bb,
                  Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("cg-store")
                      .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
              .toStream().to("out", Produced.with(
                  WindowedSerdes.sessionWindowedSerdeFrom(String.class), Serdes.Long()));

            try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), sessProps, Instant.ofEpochMilli(0))) {
                TestInputTopic<String, String> in1 = driver.createInputTopic(
                    "in1", Serdes.String().serializer(), Serdes.String().serializer());
                TestInputTopic<String, String> in2 = driver.createInputTopic(
                    "in2", Serdes.String().serializer(), Serdes.String().serializer());
                TestOutputTopic<Windowed<String>, Long> outT = driver.createOutputTopic(
                    "out",
                    WindowedSerdes.sessionWindowedSerdeFrom(String.class).deserializer(),
                    Serdes.Long().deserializer());

                in1.pipeInput("a", "xx", Instant.ofEpochMilli(0));
                in2.pipeInput("a", "z", Instant.ofEpochMilli(1));
                in1.pipeInput("a", "y", Instant.ofEpochMilli(2));
                in1.pipeInput("b", "qqqq", Instant.ofEpochMilli(3));
                in2.pipeInput("b", "z", Instant.ofEpochMilli(4));

                StringBuilder sb = new StringBuilder("[\n");
                List<KeyValue<Windowed<String>, Long>> recs = outT.readKeyValuesToList();
                for (int i = 0; i < recs.size(); i++) {
                    KeyValue<Windowed<String>, Long> kv = recs.get(i);
                    long ws = kv.key.window().start();
                    long we = kv.key.window().end();
                    sb.append("  {\"key\": \"").append(kv.key.key()).append("\", ")
                      .append("\"windowStart\": ").append(ws).append(", ")
                      .append("\"windowEnd\": ").append(we).append(", ")
                      .append("\"value\": ").append(kv.value).append("}");
                    sb.append(i + 1 < recs.size() ? ",\n" : "\n");
                }
                sb.append("]\n");
                Files.writeString(out.resolve("behavior_session.json"), sb.toString());
                System.out.println("cogroup session-windowed behavior:\n" + sb);
            }
        }
    }
}
