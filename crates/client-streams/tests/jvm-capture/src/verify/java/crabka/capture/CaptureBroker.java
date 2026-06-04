package crabka.capture;

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cross-check harness (mechanism B): runs the {@code count} DSL topology against a REAL
 * Apache Kafka 4.1 broker with {@code group.protocol=streams}, lets {@link KafkaStreams}
 * join the streams group (which builds and sends the apiKey-88
 * {@code StreamsGroupHeartbeatRequest.Topology}), then reflects the LIVE
 * {@link StreamsRebalanceData} the running client actually computed and prints its
 * subtopologies. This is the topology the client really sent to the broker — used to
 * confirm the no-broker mechanism-A capture in {@link Capture} is byte-identical.
 *
 * <p>Run with {@code BOOTSTRAP} env var pointing at the broker (e.g. {@code crabka-broker:9092}).
 */
public final class CaptureBroker {

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092");

        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .selectKey((k, v) -> k)
            .groupByKey()
            .count()
            .toStream()
            .to("out");

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        p.put("group.protocol", "streams");
        p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        Topology topology = b.build(p);
        KafkaStreams streams = new KafkaStreams(topology, p);

        AtomicReference<String> dump = new AtomicReference<>(null);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING || newState == KafkaStreams.State.REBALANCING) {
                try {
                    String s = reflectRebalanceData(streams);
                    if (s != null) {
                        dump.compareAndSet(null, s);
                    }
                } catch (Exception ignore) {
                    // keep polling on next transition
                }
            }
        });

        streams.start();

        // give the client time to join the streams group and build StreamsRebalanceData
        for (int i = 0; i < 60 && dump.get() == null; i++) {
            Thread.sleep(1000);
            try {
                String s = reflectRebalanceData(streams);
                if (s != null) {
                    dump.compareAndSet(null, s);
                }
            } catch (Exception ignore) {
                // not yet available
            }
        }

        System.out.println("=== LIVE StreamsRebalanceData.subtopologies (real client) ===");
        System.out.println(dump.get() == null ? "<<NOT CAPTURED>>" : dump.get());

        streams.close();
        System.exit(0);
    }

    /** Reflect the live {@code StreamThread.streamsRebalanceData} and dump its subtopologies. */
    @SuppressWarnings("unchecked")
    private static String reflectRebalanceData(KafkaStreams streams) throws Exception {
        Field threadsField = KafkaStreams.class.getDeclaredField("threads");
        threadsField.setAccessible(true);
        Object threads = threadsField.get(streams);
        java.util.List<?> threadList = (java.util.List<?>) threads;
        if (threadList.isEmpty()) {
            return null;
        }
        Object thread = threadList.get(0);
        Field rdField = thread.getClass().getDeclaredField("streamsRebalanceData");
        rdField.setAccessible(true);
        java.util.Optional<StreamsRebalanceData> opt =
            (java.util.Optional<StreamsRebalanceData>) rdField.get(thread);
        if (opt.isEmpty()) {
            return null;
        }
        StreamsRebalanceData rd = opt.get();
        Map<String, StreamsRebalanceData.Subtopology> subs = rd.subtopologies();
        if (subs.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        subs.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> {
                StreamsRebalanceData.Subtopology s = e.getValue();
                sb.append("subtopology ").append(e.getKey()).append(":\n")
                    .append("  sourceTopics=").append(sorted(s.sourceTopics())).append("\n")
                    .append("  repartitionSinkTopics=").append(sorted(s.repartitionSinkTopics())).append("\n")
                    .append("  repartitionSourceTopics=").append(sorted(s.repartitionSourceTopics().keySet())).append("\n")
                    .append("  stateChangelogTopics=").append(sorted(s.stateChangelogTopics().keySet())).append("\n")
                    .append("  copartitionGroups=").append(s.copartitionGroups()).append("\n");
            });
        return sb.toString();
    }

    private static java.util.List<String> sorted(java.util.Collection<String> c) {
        return c.stream().sorted().collect(java.util.stream.Collectors.toList());
    }

    private CaptureBroker() {
    }
}
