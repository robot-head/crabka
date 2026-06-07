package crabka.capture;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.KeyValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * KIP-213 foreign-key-join byte + semantic ORACLE for the Crabka Rust FK-join codecs
 * and processors. Reflects the JVM's own internal FK-join classes (CombinedKeySchema,
 * SubscriptionWrapperSerde, SubscriptionResponseWrapperSerde, Murmur3) and dumps their
 * exact serialized bytes as hex, plus drives a {@link TopologyTestDriver} over the
 * inner/left FK-join topologies and records every emitted output. Writes ONE
 * {@code behavior.json} to {@code arg[0]}.
 *
 * <p>This is the ground truth that every later FK-join task validates against — it is a
 * real JVM 4.1 capture, never hand-authored.
 *
 * <p><b>Pinned byte layouts (from the JVM 4.1 sources):</b>
 * <ul>
 *   <li>{@code CombinedKeySchema.toBytes(fk, pk)} =
 *       {@code {Integer.BYTES fkLen BE}{fk bytes}{pk bytes}};
 *       {@code prefixBytes(fk)} = {@code {Integer.BYTES fkLen BE}{fk bytes}}.</li>
 *   <li>{@code SubscriptionWrapper} CURRENT_VERSION = 1. Serializer:
 *       {@code {1-bit isHashNull | 7-bit version}{1-byte instruction}}
 *       {@code {opt 16-byte hash = putLong(h0) ‖ putLong(h1), each BE}}
 *       {@code {PK bytes}{4-byte primaryPartition BE (v1 only, at END)}}.
 *       When hash==null the high bit of the first byte is set (version | 0x80) and the
 *       16-byte hash is omitted.</li>
 *   <li>{@code SubscriptionResponseWrapper} CURRENT_VERSION = 0. Serializer:
 *       {@code {1-bit isHashNull | 7-bit version}{opt 16-byte hash}{foreign-value bytes}}.
 *       primaryPartition is a non-serializing field (NOT on the wire).</li>
 *   <li>Murmur3 staleness hash: {@code Murmur3.hash128(byte[])} → {@code long[2]};
 *       stored as two BE longs h0 then h1 = 16 bytes (the exact order the wrappers write).</li>
 * </ul>
 */
public final class ForeignKeyJoinBehavior {

    private static final String FK_PKG =
        "org.apache.kafka.streams.kstream.internals.foreignkeyjoin.";
    private static final String DUMMY_TOPIC = "fk-topic";

    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"subscription_wrapper_version\": ")
            .append(subscriptionWrapperCurrentVersion()).append(",\n");
        json.append("  \"subscription_response_wrapper_version\": ")
            .append(subscriptionResponseWrapperCurrentVersion()).append(",\n");
        json.append("  \"combined_key\": ").append(captureCombinedKey()).append(",\n");
        json.append("  \"murmur3\": ").append(captureMurmur3()).append(",\n");
        json.append("  \"instruction_ordinals\": ").append(captureInstructionOrdinals()).append(",\n");
        json.append("  \"subscription_wrapper\": ").append(captureSubscriptionWrapper()).append(",\n");
        json.append("  \"subscription_response_wrapper\": ")
            .append(captureSubscriptionResponseWrapper()).append(",\n");
        json.append("  \"inner_sequence\": ").append(captureSequence(false)).append(",\n");
        json.append("  \"left_sequence\": ").append(captureSequence(true)).append(",\n");
        json.append("  \"names\": ").append(captureNames()).append("\n");
        json.append("}\n");

        Files.writeString(out.resolve("behavior.json"), json.toString(), StandardCharsets.UTF_8);
        System.out.println("fk-join behavior:\n" + json);
    }

    // ---- static version scalars ----------------------------------------------

    private static int subscriptionWrapperCurrentVersion() throws Exception {
        Class<?> sw = Class.forName(FK_PKG + "SubscriptionWrapper");
        Field f = sw.getDeclaredField("CURRENT_VERSION");
        f.setAccessible(true);
        return f.getByte(null) & 0xFF;
    }

    private static int subscriptionResponseWrapperCurrentVersion() throws Exception {
        Class<?> srw = Class.forName(FK_PKG + "SubscriptionResponseWrapper");
        Field f = srw.getDeclaredField("CURRENT_VERSION");
        f.setAccessible(true);
        return f.getByte(null) & 0xFF;
    }

    // ---- 1. CombinedKeySchema.toBytes + prefixBytes --------------------------

    /**
     * Construct a {@code CombinedKeySchema<KRight=String fk, KLeft=String pk>} with String
     * serdes via its public ctor, set the (otherwise init-time) serde-topic fields by
     * reflection, then invoke the package-private {@code toBytes(fk, pk)} / {@code prefixBytes(fk)}.
     */
    @SuppressWarnings("unchecked")
    private static String captureCombinedKey() throws Exception {
        Class<?> cks = Class.forName(FK_PKG + "CombinedKeySchema");
        // ctor: (Supplier<String> fkTopicSupplier, Serde<KRight> fkSerde,
        //        Supplier<String> pkTopicSupplier, Serde<KLeft> pkSerde)
        Constructor<?> ctor = cks.getDeclaredConstructor(
            Supplier.class, Serde.class, Supplier.class, Serde.class);
        ctor.setAccessible(true);
        Supplier<String> topicSupplier = () -> DUMMY_TOPIC;
        Object schema = ctor.newInstance(
            topicSupplier, Serdes.String(), topicSupplier, Serdes.String());

        // The ctor sets the serializers; init() (which needs a ProcessorContext) only sets
        // the serde-topic strings. Set them directly to the dummy topic.
        setField(schema, "primaryKeySerdeTopic", DUMMY_TOPIC);
        setField(schema, "foreignKeySerdeTopic", DUMMY_TOPIC);

        Method toBytes = cks.getDeclaredMethod("toBytes", Object.class, Object.class);
        toBytes.setAccessible(true);
        Method prefixBytes = cks.getDeclaredMethod("prefixBytes", Object.class);
        prefixBytes.setAccessible(true);

        String[][] pairs = {
            {"", ""}, {"A", "k1"}, {"fk", "pk"}, {"hello", "world"}, {"A", "k2"},
        };
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < pairs.length; i++) {
            String fk = pairs[i][0];
            String pk = pairs[i][1];
            Bytes full = (Bytes) toBytes.invoke(schema, fk, pk);
            Bytes prefix = (Bytes) prefixBytes.invoke(schema, fk);
            if (i > 0) sb.append(",");
            sb.append("\n    { \"fk\": ").append(quote(fk))
                .append(", \"pk\": ").append(quote(pk))
                .append(", \"bytes_hex\": ").append(quote(hex(full.get())))
                .append(", \"prefix_hex\": ").append(quote(hex(prefix.get())))
                .append(" }");
        }
        sb.append("\n  ]");
        return sb.toString();
    }

    // ---- 2. Murmur3 16-byte hash ---------------------------------------------

    /**
     * {@code Murmur3.hash128(byte[])} → {@code long[2]}, serialized the way the FK wrappers
     * store it: two BE longs, h0 then h1 (16 bytes). We replicate the wrapper's exact
     * write (ByteBuffer.putLong twice) so the Rust hash codec can match.
     */
    private static String captureMurmur3() throws Exception {
        Class<?> murmur = Class.forName("org.apache.kafka.streams.state.internals.Murmur3");
        Method hash128 = murmur.getDeclaredMethod("hash128", byte[].class);
        hash128.setAccessible(true);

        byte[] twenty = new byte[20];
        for (int i = 0; i < 20; i++) twenty[i] = (byte) i;
        byte[][] inputs = {
            new byte[0],
            "A".getBytes(StandardCharsets.UTF_8),
            "hello".getBytes(StandardCharsets.UTF_8),
            twenty,
        };

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < inputs.length; i++) {
            long[] h = (long[]) hash128.invoke(null, (Object) inputs[i]);
            byte[] hashBytes = new byte[16];
            putLongBE(hashBytes, 0, h[0]);
            putLongBE(hashBytes, 8, h[1]);
            if (i > 0) sb.append(",");
            sb.append("\n    { \"input_hex\": ").append(quote(hex(inputs[i])))
                .append(", \"h0\": ").append(h[0])
                .append(", \"h1\": ").append(h[1])
                .append(", \"hash_hex\": ").append(quote(hex(hashBytes)))
                .append(" }");
        }
        sb.append("\n  ]");
        return sb.toString();
    }

    // ---- 3. Instruction ordinals/bytes + version -----------------------------

    private static String captureInstructionOrdinals() throws Exception {
        Class<?> sw = Class.forName(FK_PKG + "SubscriptionWrapper");
        Class<?> inst = Class.forName(FK_PKG + "SubscriptionWrapper$Instruction");
        Object[] values = (Object[]) inst.getMethod("values").invoke(null);
        Method valueM = inst.getMethod("value");

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            String name = ((Enum<?>) values[i]).name();
            byte b = (byte) valueM.invoke(values[i]);
            if (i > 0) sb.append(",");
            sb.append("\n    { \"name\": ").append(quote(name))
                .append(", \"byte\": ").append(b & 0xFF)
                .append(", \"ordinal\": ").append(((Enum<?>) values[i]).ordinal())
                .append(" }");
        }
        sb.append("\n  ]");
        return sb.toString();
    }

    // ---- 4. SubscriptionWrapper serializer output ----------------------------

    /**
     * For each Instruction, serialize a representative {@code SubscriptionWrapper} via the
     * JVM {@code SubscriptionWrapperSerde} serializer. PROPAGATE_* carry a non-null hash;
     * DELETE_* carry a null hash. primaryPartition is fixed at 0 and (v1) is serialized as
     * the trailing 4-byte BE int. We RECORD that primaryPartition is present + its position.
     */
    @SuppressWarnings("unchecked")
    private static String captureSubscriptionWrapper() throws Exception {
        Class<?> sw = Class.forName(FK_PKG + "SubscriptionWrapper");
        Class<?> inst = Class.forName(FK_PKG + "SubscriptionWrapper$Instruction");
        // public ctor (long[] hash, Instruction, KLeft primaryKey, Integer primaryPartition)
        Constructor<?> swCtor = sw.getConstructor(
            long[].class, inst, Object.class, Integer.class);

        // public SubscriptionWrapperSerde(Supplier<String> pkPseudoTopicSupplier, Serde<KLeft> pkSerde)
        Class<?> serdeClass = Class.forName(FK_PKG + "SubscriptionWrapperSerde");
        Constructor<?> serdeCtor = serdeClass.getConstructor(Supplier.class, Serde.class);
        Supplier<String> topicSupplier = () -> DUMMY_TOPIC;
        Object serde = serdeCtor.newInstance(topicSupplier, Serdes.String());
        Serializer<Object> ser =
            (Serializer<Object>) serdeClass.getMethod("serializer").invoke(serde);

        // A deterministic 16-byte hash (two longs) for the PROPAGATE_* cases.
        long[] hash = {0x0102030405060708L, 0x1112131415161718L};
        byte[] hashBytes = new byte[16];
        putLongBE(hashBytes, 0, hash[0]);
        putLongBE(hashBytes, 8, hash[1]);
        Integer primaryPartition = 0;
        String pk = "pk";

        Object[] instructions = (Object[]) inst.getMethod("values").invoke(null);
        Method valueM = inst.getMethod("value");

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < instructions.length; i++) {
            String name = ((Enum<?>) instructions[i]).name();
            byte instByte = (byte) valueM.invoke(instructions[i]);
            // PROPAGATE_* (0x02/0x03) carry a hash; DELETE_* (0x00/0x01) carry null.
            boolean hasHash = name.startsWith("PROPAGATE");
            long[] h = hasHash ? hash : null;
            Object wrapper = swCtor.newInstance(h, instructions[i], pk, primaryPartition);
            byte[] bytes = ser.serialize(DUMMY_TOPIC, wrapper);
            if (i > 0) sb.append(",");
            sb.append("\n    { \"instruction\": ").append(quote(name))
                .append(", \"instruction_byte\": ").append(instByte & 0xFF)
                .append(", \"hash_hex\": ")
                .append(hasHash ? quote(hex(hashBytes)) : "null")
                .append(", \"pk\": ").append(quote(pk))
                .append(", \"primary_partition\": ").append(primaryPartition)
                .append(", \"bytes_hex\": ").append(quote(hex(bytes)))
                .append(" }");
        }
        sb.append("\n  ]");
        return sb.toString();
    }

    // ---- 5. SubscriptionResponseWrapper serializer output --------------------

    @SuppressWarnings("unchecked")
    private static String captureSubscriptionResponseWrapper() throws Exception {
        Class<?> srw = Class.forName(FK_PKG + "SubscriptionResponseWrapper");
        // public ctor (long[] originalValueHash, VRight foreignValue, Integer primaryPartition)
        Constructor<?> srwCtor = srw.getConstructor(
            long[].class, Object.class, Integer.class);

        // public SubscriptionResponseWrapperSerde(Serde<VRight> foreignValueSerde)
        Class<?> serdeClass = Class.forName(FK_PKG + "SubscriptionResponseWrapperSerde");
        Constructor<?> serdeCtor = serdeClass.getConstructor(Serde.class);
        Object serde = serdeCtor.newInstance(Serdes.String());
        Serializer<Object> ser =
            (Serializer<Object>) serdeClass.getMethod("serializer").invoke(serde);

        long[] hash = {0x2122232425262728L, 0x3132333435363738L};
        byte[] hashBytes = new byte[16];
        putLongBE(hashBytes, 0, hash[0]);
        putLongBE(hashBytes, 8, hash[1]);
        Integer primaryPartition = 0;

        // Entry A: hash present + foreign value present.
        Object withVal = srwCtor.newInstance(hash, "vfk", primaryPartition);
        byte[] bytesWithVal = ser.serialize(DUMMY_TOPIC, withVal);

        // Entry B: hash present + null foreign value.
        Object nullVal = srwCtor.newInstance(hash, null, primaryPartition);
        byte[] bytesNullVal = ser.serialize(DUMMY_TOPIC, nullVal);

        // Entry C: null hash + foreign value present.
        Object nullHash = srwCtor.newInstance(null, "vfk", primaryPartition);
        byte[] bytesNullHash = ser.serialize(DUMMY_TOPIC, nullHash);

        String fvHex = hex("vfk".getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder("[");
        sb.append("\n    { \"hash_hex\": ").append(quote(hex(hashBytes)))
            .append(", \"foreign_value\": ").append(quote("vfk"))
            .append(", \"foreign_value_hex\": ").append(quote(fvHex))
            .append(", \"bytes_hex\": ").append(quote(hex(bytesWithVal))).append(" },");
        sb.append("\n    { \"hash_hex\": ").append(quote(hex(hashBytes)))
            .append(", \"foreign_value\": null")
            .append(", \"foreign_value_hex\": null")
            .append(", \"bytes_hex\": ").append(quote(hex(bytesNullVal))).append(" },");
        sb.append("\n    { \"hash_hex\": null")
            .append(", \"foreign_value\": ").append(quote("vfk"))
            .append(", \"foreign_value_hex\": ").append(quote(fvHex))
            .append(", \"bytes_hex\": ").append(quote(hex(bytesNullHash))).append(" }");
        sb.append("\n  ]");
        return sb.toString();
    }

    // ---- 6. inner/left behavioral sequence -----------------------------------

    /**
     * Drive a {@link TopologyTestDriver} over the inner or left FK-join (built with
     * {@code b.build()}, NOT optimized) and record EVERY output on topic {@code out} after
     * each input pipe. One input can yield 0..N outputs, hence an array per input.
     */
    private static String captureSequence(boolean left) {
        // Built with b.build() (NOT optimized), per the FK-join behavioral-oracle spec.
        Topology topo = left ? Capture.fkJoinLeftUnoptimized() : Capture.fkJoinInnerUnoptimized();
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, left ? "fk-left" : "fk-inner");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Input script: (side, key, value, ts). value==null means a tombstone.
        Object[][] script = {
            {"a", "k1", "A", 0L},
            {"b", "A", "X", 1L},
            {"a", "k1", "A2", 2L},
            {"a", "k2", "A", 3L},
            {"b", "A", "Y", 4L},
            {"a", "k1", "B", 5L},
            {"a", "k1", null, 6L},
        };

        StringBuilder sb = new StringBuilder("[");
        try (TopologyTestDriver driver = new TopologyTestDriver(topo, p, Instant.ofEpochMilli(0))) {
            TestInputTopic<String, String> inA = driver.createInputTopic(
                "a", Serdes.String().serializer(), Serdes.String().serializer());
            TestInputTopic<String, String> inB = driver.createInputTopic(
                "b", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outT = driver.createOutputTopic(
                "out", Serdes.String().deserializer(), Serdes.String().deserializer());

            for (int i = 0; i < script.length; i++) {
                String side = (String) script[i][0];
                String key = (String) script[i][1];
                String value = (String) script[i][2];
                long ts = (Long) script[i][3];
                TestInputTopic<String, String> in = side.equals("a") ? inA : inB;
                in.pipeInput(key, value, Instant.ofEpochMilli(ts));

                List<KeyValue<String, String>> outs = outT.readKeyValuesToList();
                if (i > 0) sb.append(",");
                sb.append("\n    { \"in\": ")
                    .append(quote(side + ":" + key + "=" + (value == null ? "null" : value)
                        + "@" + ts))
                    .append(", \"out\": [");
                for (int j = 0; j < outs.size(); j++) {
                    KeyValue<String, String> kv = outs.get(j);
                    if (j > 0) sb.append(", ");
                    sb.append("{ \"key\": ").append(quote(kv.key))
                        .append(", \"value\": ")
                        .append(kv.value == null ? "null" : quote(kv.value))
                        .append(" }");
                }
                sb.append("] }");
            }
        }
        sb.append("\n  ]");
        return sb.toString();
    }

    // ---- 7. names ------------------------------------------------------------

    /**
     * Record the FK processor node-name prefixes, the subscription store name, both
     * internal repartition topic names, and the subscription changelog topic name, by
     * walking the optimized topology's describe() string and the wire subtopologies.
     * Also includes the raw describe() string so any name is recoverable.
     */
    private static String captureNames() throws Exception {
        Topology inner = Capture.fkJoinInner();
        String describe = inner.describe().toString();

        // The wire subtopologies carry the app-prefixed repartition + changelog topic names.
        List<org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.Subtopology> subs =
            Capture.wireSubtopologies(inner);
        List<String> repartitionTopics = new ArrayList<>();
        List<String> changelogTopics = new ArrayList<>();
        for (var s : subs) {
            for (var ti : s.repartitionSourceTopics()) repartitionTopics.add(ti.name());
            for (var ti : s.stateChangelogTopics()) changelogTopics.add(ti.name());
        }

        // Pull specific names out of the describe() string.
        String registrationRepartition = firstContaining(repartitionTopics,
            "SUBSCRIPTION-REGISTRATION");
        String responseRepartition = firstContaining(repartitionTopics,
            "SUBSCRIPTION-RESPONSE");
        String subscriptionChangelog = firstContaining(changelogTopics,
            "SUBSCRIPTION-STATE-STORE");
        String subscriptionStore = extractToken(describe,
            "KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-");

        StringBuilder sb = new StringBuilder("{");
        sb.append("\n    \"node_prefixes\": {")
            .append("\n      \"subscription_registration\": \"KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-\",")
            .append("\n      \"subscription_processor\": \"KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-\",")
            .append("\n      \"subscription_response_resolver\": \"KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-\",")
            .append("\n      \"fk_join_output\": \"KTABLE-FK-JOIN-OUTPUT-\",")
            .append("\n      \"subscription_state_store\": \"KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-\"")
            .append("\n    },");
        sb.append("\n    \"subscription_store_name\": ").append(quote(subscriptionStore)).append(",");
        sb.append("\n    \"subscription_registration_repartition_topic\": ")
            .append(quote(registrationRepartition)).append(",");
        sb.append("\n    \"subscription_response_repartition_topic\": ")
            .append(quote(responseRepartition)).append(",");
        sb.append("\n    \"subscription_changelog_topic\": ")
            .append(quote(subscriptionChangelog)).append(",");
        sb.append("\n    \"all_repartition_topics\": ").append(jsonStrList(repartitionTopics)).append(",");
        sb.append("\n    \"all_changelog_topics\": ").append(jsonStrList(changelogTopics)).append(",");
        sb.append("\n    \"describe_inner\": ").append(quote(describe));
        sb.append("\n  }");
        return sb.toString();
    }

    // ---- tiny helpers --------------------------------------------------------

    private static String firstContaining(List<String> xs, String needle) {
        for (String x : xs) {
            if (x.contains(needle)) return x;
        }
        return null;
    }

    /** Extract the full token (whitespace/bracket-delimited) starting with {@code prefix}. */
    private static String extractToken(String haystack, String prefix) {
        int idx = haystack.indexOf(prefix);
        if (idx < 0) return null;
        int end = idx;
        while (end < haystack.length()) {
            char c = haystack.charAt(end);
            if (c == ' ' || c == ']' || c == ')' || c == ',' || c == '\n' || c == '\r') break;
            end++;
        }
        return haystack.substring(idx, end);
    }

    private static void setField(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static void putLongBE(byte[] dst, int off, long v) {
        for (int i = 0; i < 8; i++) {
            dst[off + i] = (byte) (v >>> (8 * (7 - i)));
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static String jsonStrList(List<String> xs) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(quote(xs.get(i)));
        }
        sb.append("]");
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

    private ForeignKeyJoinBehavior() {
    }
}
