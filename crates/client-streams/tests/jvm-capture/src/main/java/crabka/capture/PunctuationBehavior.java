package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Capture-first behavior pin for KIP-1071 streams-client punctuation: runs the JVM
 * {@link TopologyTestDriver} with a processor that schedules one STREAM_TIME and one
 * WALL_CLOCK_TIME punctuator (interval 10ms each), drives a fixed script, and writes
 * the exact sequence of fired timestamps to {@code behavior.json}. This is NOT a wire
 * golden — punctuation is invisible in the topology; it pins the firing SEMANTICS
 * (first-fire offset, catch-up count, the timestamp each PunctuationType passes).
 */
public final class PunctuationBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);
        List<String> fired = new ArrayList<>();

        Topology topo = new Topology();
        topo.addSource("src", "in");
        topo.addProcessor("proc", () -> new Processor<String, String, String, String>() {
            private ProcessorContext<String, String> ctx;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.ctx = context;
                context.schedule(Duration.ofMillis(10), PunctuationType.STREAM_TIME,
                        ts -> fired.add("stream:" + ts));
                context.schedule(Duration.ofMillis(10), PunctuationType.WALL_CLOCK_TIME,
                        ts -> fired.add("wall:" + ts));
            }

            @Override
            public void process(Record<String, String> r) {
                ctx.forward(r);
            }
        }, "src");
        topo.addSink("snk", "out", "proc");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "punct");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Mock wall clock starts at epoch 0. Markers after each driving action let us
        // correlate which pipe/advance produced which fire (disambiguates interval
        // gating, boundary-vs-current value, and catch-up).
        try (TopologyTestDriver driver = new TopologyTestDriver(topo, props, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> in = driver.createInputTopic(
                    "in", Serdes.String().serializer(), Serdes.String().serializer());
            // Stream-time script: sub-interval steps, an exact boundary, then a big jump.
            for (long ts : new long[] {0, 5, 9, 10, 11, 100}) {
                in.pipeInput("k", "v", Instant.ofEpochMilli(ts));
                fired.add("|pipe@" + ts);
            }
            fired.add("=== wall ===");
            // Wall-clock script: sub-interval advances reaching a boundary, then a big jump.
            for (long step : new long[] {3, 3, 4, 100}) {
                driver.advanceWallClockTime(Duration.ofMillis(step));
                fired.add("|adv+" + step);
            }
        }

        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < fired.size(); i++) {
            sb.append("  \"").append(fired.get(i)).append("\"");
            sb.append(i + 1 < fired.size() ? ",\n" : "\n");
        }
        sb.append("]\n");
        Files.writeString(out.resolve("behavior.json"), sb.toString());
        System.out.println("punctuation behavior:\n" + sb);
    }
}
