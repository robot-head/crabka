package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
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
 * Capture-first behavior pin for KIP-825 native emit-final
 * ({@code EmitStrategy.onWindowClose()}) across the three windowed aggregations:
 * time (tumbling), sliding (KIP-450), and session. Runs the JVM
 * {@link TopologyTestDriver} with a fixed input script per window type and
 * writes the exact emission sequence to {@code time.json} / {@code sliding.json}
 * / {@code session.json}. This is the ground truth for the Rust emit-final port:
 * it pins WHICH windows emit, WHEN (the close boundary), with WHAT final value,
 * and in WHAT order. The {@code .toStream()} sink drops the internal
 * {@code Change} wrapper, so the old/new shape is not observable here (a separate
 * changelog-bytes golden would pin that); the emission sequence + close boundary
 * are what this fixture validates.
 */
public final class EmitFinalBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        // Shared discriminating script. Each later record advances stream-time,
        // closing earlier windows; the final window never closes (no record
        // after it), so it must NOT appear in the output.
        long[] scriptTs = {1L, 5L, 11L, 21L, 40L};

        captureTime(out, scriptTs);
        captureSliding(out, scriptTs);
        captureSession(out);
    }

    /** Tumbling time windows, size 10, no grace, emit-on-close, count. */
    private static void captureTime(Path out, long[] scriptTs) throws Exception {
        Properties props = baseProps("emit-final-time");
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(10)))
            .emitStrategy(EmitStrategy.onWindowClose())
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
            for (long ts : scriptTs) {
                in.pipeInput("a", "v", Instant.ofEpochMilli(ts));
            }
            writeLongWindows(out.resolve("time.json"), outTopic.readKeyValuesToList(), "emit-final time");
        }
    }

    /** Sliding windows, time-difference 10, no grace, emit-on-close, count. */
    private static void captureSliding(Path out, long[] scriptTs) throws Exception {
        Properties props = baseProps("emit-final-sliding");
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(10)))
            .emitStrategy(EmitStrategy.onWindowClose())
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
            for (long ts : scriptTs) {
                in.pipeInput("a", "v", Instant.ofEpochMilli(ts));
            }
            writeLongWindows(out.resolve("sliding.json"), outTopic.readKeyValuesToList(), "emit-final sliding");
        }
    }

    /** Session windows, inactivity gap 10, no grace, emit-on-close, count. */
    private static void captureSession(Path out) throws Exception {
        Properties props = baseProps("emit-final-session");
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(10)))
            .emitStrategy(EmitStrategy.onWindowClose())
            .count()
            .toStream()
            .to("out", Produced.with(
                WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                Serdes.Long()));

        // a@0,a@4 merge into session [0,4]; a@20 opens a new session [20,20]
        // (gap 16 > 10); a@100 closes the earlier sessions.
        long[] sessionTs = {0L, 4L, 20L, 100L};
        try (TopologyTestDriver driver = new TopologyTestDriver(b.build(), props, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> in = driver.createInputTopic(
                    "in", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<Windowed<String>, Long> outTopic = driver.createOutputTopic(
                    "out",
                    WindowedSerdes.sessionWindowedSerdeFrom(String.class).deserializer(),
                    Serdes.Long().deserializer());
            for (long ts : sessionTs) {
                in.pipeInput("a", "v", Instant.ofEpochMilli(ts));
            }
            writeLongWindows(out.resolve("session.json"), outTopic.readKeyValuesToList(), "emit-final session");
        }
    }

    private static Properties baseProps(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static void writeLongWindows(
            Path file, List<KeyValue<Windowed<String>, Long>> records, String label) throws Exception {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < records.size(); i++) {
            KeyValue<Windowed<String>, Long> kv = records.get(i);
            sb.append("  {\"key\": \"").append(kv.key.key()).append("\", ")
              .append("\"windowStart\": ").append(kv.key.window().start()).append(", ")
              .append("\"windowEnd\": ").append(kv.key.window().end()).append(", ")
              .append("\"value\": ").append(kv.value).append("}");
            sb.append(i + 1 < records.size() ? ",\n" : "\n");
        }
        sb.append("]\n");
        Files.writeString(file, sb.toString());
        System.out.println(label + " behavior:\n" + sb);
    }

    private EmitFinalBehavior() {}
}
