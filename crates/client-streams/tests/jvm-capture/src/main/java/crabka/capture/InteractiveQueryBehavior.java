package crabka.capture;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Capture-first behavior pin for KIP-1071 streams-client Interactive Queries.
 * Runs three JVM {@link TopologyTestDriver} topologies (KV count, windowed count,
 * session count), feeds a fixed, deterministic input script, then reads each
 * materialized store back through the JVM IQ store APIs
 * ({@code getKeyValueStore} / {@code getWindowStore} / {@code getSessionStore})
 * and dumps the observed reads to {@code behavior.json}.
 *
 * This is NOT a wire golden — it pins the read SEMANTICS the Rust IQ byte layer
 * must reproduce: point get / inclusive range / all / approximateNumEntries for
 * KV; point + range fetch for windows; per-key session fetch with (start,end).
 * The {@code records} arrays carry the exact (value, timestamp) pairs fed, so the
 * Rust side can replay identically and assert parity.
 */
public final class InteractiveQueryBehavior {
    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"kv\": ").append(captureKv()).append(",\n");
        json.append("  \"window\": ").append(captureWindow()).append(",\n");
        json.append("  \"session\": ").append(captureSession()).append("\n");
        json.append("}\n");

        Files.writeString(out.resolve("behavior.json"), json.toString());
        System.out.println("iq behavior:\n" + json);
    }

    private static Properties props(String appId) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return p;
    }

    /**
     * KV: stream("in").groupByKey().count(as "counts"). Feed a,a,b → counts a=2,b=1.
     * Read get("a"), get("z")=null, range("a","b") inclusive, all(), approxCount.
     */
    private static String captureKv() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as("counts"));
        Topology topo = b.build();

        // (value, timestamp) script. The key equals the value so each distinct
        // value lands under its own count key.
        String[] vals = {"a", "a", "b"};
        long[] ts = {0, 0, 0};

        StringBuilder rec = new StringBuilder("[");
        try (TopologyTestDriver driver =
                 new TopologyTestDriver(topo, props("iq-kv"), Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> in = driver.createInputTopic(
                "in", Serdes.String().serializer(), Serdes.String().serializer());
            for (int i = 0; i < vals.length; i++) {
                in.pipeInput(vals[i], vals[i], Instant.ofEpochMilli(ts[i]));
                if (i > 0) rec.append(",");
                rec.append("[").append(quote(vals[i])).append(",").append(ts[i]).append("]");
            }
            rec.append("]");

            KeyValueStore<String, Long> kv = driver.getKeyValueStore("counts");
            Long getA = kv.get("a");
            Long getZ = kv.get("z");

            StringBuilder range = new StringBuilder("[");
            try (KeyValueIterator<String, Long> it = kv.range("a", "b")) {
                boolean first = true;
                while (it.hasNext()) {
                    KeyValue<String, Long> e = it.next();
                    if (!first) range.append(",");
                    range.append("[").append(quote(e.key)).append(",").append(e.value).append("]");
                    first = false;
                }
            }
            range.append("]");

            StringBuilder all = new StringBuilder("[");
            try (KeyValueIterator<String, Long> it = kv.all()) {
                boolean first = true;
                while (it.hasNext()) {
                    KeyValue<String, Long> e = it.next();
                    if (!first) all.append(",");
                    all.append("[").append(quote(e.key)).append(",").append(e.value).append("]");
                    first = false;
                }
            }
            all.append("]");

            long count = kv.approximateNumEntries();

            return "{ \"records\": " + rec
                + ", \"get_a\": " + getA
                + ", \"get_z\": " + (getZ == null ? "null" : getZ)
                + ", \"range_a_b\": " + range
                + ", \"all\": " + all
                + ", \"count\": " + count + " }";
        }
    }

    /**
     * Window: stream("in").groupByKey().windowedBy(TimeWindows size 1000, no grace)
     * .count(as "wc"). Feed ("k",0) and ("k",1000) → windows [0,1000) and [1000,2000)
     * each count 1. Read fetch("k", 0) point, fetch("k", 0, 1000) range.
     */
    private static String captureWindow() {
        long sizeMs = 1000;
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(sizeMs)))
            .count(Materialized.as("wc"));
        Topology topo = b.build();

        String[] vals = {"v", "v"};
        long[] ts = {0, 1000};

        StringBuilder rec = new StringBuilder("[");
        try (TopologyTestDriver driver =
                 new TopologyTestDriver(topo, props("iq-window"), Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> in = driver.createInputTopic(
                "in", Serdes.String().serializer(), Serdes.String().serializer());
            for (int i = 0; i < vals.length; i++) {
                // Key is always "k" so both records aggregate under the same key.
                in.pipeInput("k", vals[i], Instant.ofEpochMilli(ts[i]));
                if (i > 0) rec.append(",");
                rec.append("[").append(quote("k")).append(",").append(ts[i]).append("]");
            }
            rec.append("]");

            WindowStore<String, Long> ws = driver.getWindowStore("wc");
            // Point fetch: value of the window of "k" that starts at ts=0.
            Long fetchSingle = ws.fetch("k", 0L);

            // Range fetch over windows whose start is in [0, 1000]; each entry is
            // (windowStart, count).
            StringBuilder fetchRange = new StringBuilder("[");
            try (WindowStoreIterator<Long> it = ws.fetch("k", Instant.ofEpochMilli(0),
                                                          Instant.ofEpochMilli(1000))) {
                boolean first = true;
                while (it.hasNext()) {
                    KeyValue<Long, Long> e = it.next();
                    if (!first) fetchRange.append(",");
                    fetchRange.append("[").append(e.key).append(",").append(e.value).append("]");
                    first = false;
                }
            }
            fetchRange.append("]");

            return "{ \"records\": " + rec
                + ", \"size_ms\": " + sizeMs
                + ", \"fetch_single_k_0\": " + (fetchSingle == null ? "null" : fetchSingle)
                + ", \"fetch_k_0_1000\": " + fetchRange + " }";
        }
    }

    /**
     * Session: stream("in").groupByKey().windowedBy(SessionWindows gap 100, no grace)
     * .count(as "sc"). Feed ("k",0),("k",10),("k",500) → sessions [0,10] count 2 and
     * [500,500] count 1. Read fetch("k") → list of ((start,end), count).
     */
    private static String captureSession() {
        long gapMs = 100;
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMillis(gapMs)))
            .count(Materialized.as("sc"));
        Topology topo = b.build();

        long[] ts = {0, 10, 500};

        StringBuilder rec = new StringBuilder("[");
        try (TopologyTestDriver driver =
                 new TopologyTestDriver(topo, props("iq-session"), Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> in = driver.createInputTopic(
                "in", Serdes.String().serializer(), Serdes.String().serializer());
            for (int i = 0; i < ts.length; i++) {
                in.pipeInput("k", "x", Instant.ofEpochMilli(ts[i]));
                if (i > 0) rec.append(",");
                rec.append("[").append(quote("k")).append(",").append(ts[i]).append("]");
            }
            rec.append("]");

            SessionStore<String, Long> ss = driver.getSessionStore("sc");
            List<String> sessions = new ArrayList<>();
            try (KeyValueIterator<Windowed<String>, Long> it = ss.fetch("k")) {
                while (it.hasNext()) {
                    KeyValue<Windowed<String>, Long> e = it.next();
                    long start = e.key.window().start();
                    long end = e.key.window().end();
                    sessions.add("[[" + start + "," + end + "]," + e.value + "]");
                }
            }

            return "{ \"records\": " + rec
                + ", \"gap_ms\": " + gapMs
                + ", \"fetch_k\": [" + String.join(",", sessions) + "] }";
        }
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
