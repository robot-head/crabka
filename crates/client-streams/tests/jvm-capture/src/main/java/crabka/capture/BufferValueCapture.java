package crabka.capture;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

/**
 * Dumps the JVM `InMemoryTimeOrderedKeyValueChangeBuffer` changelog VALUE bytes
 * for known inputs, as hex, so the Rust `suppress_bufval` codec can match them
 * byte-for-byte. The changelog value = `BufferValue.serialize(8) ‖ bufferTime:8BE`
 * (the JVM `logValue` calls `serialize(Long.BYTES)` then `putLong(time)`).
 *
 * `BufferValue` is an internal class (package-private ctor + serialize), so we use
 * reflection. Values are serialized as 8-byte BE longs (the count aggregate).
 */
public final class BufferValueCapture {

    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : "out");
        Files.createDirectories(out);

        Class<?> bvClass =
            Class.forName("org.apache.kafka.streams.state.internals.BufferValue");
        Constructor<?> ctor = bvClass.getDeclaredConstructor(
            byte[].class, byte[].class, byte[].class, ProcessorRecordContext.class);
        ctor.setAccessible(true);
        Method serialize = bvClass.getDeclaredMethod("serialize", int.class);
        serialize.setAccessible(true);

        final byte[] count1 = ByteBuffer.allocate(8).putLong(1L).array();
        final byte[] count2 = ByteBuffer.allocate(8).putLong(2L).array();

        // wc_first: a window's first value — prior=null, old=null, new=count(1).
        ProcessorRecordContext ctx10 =
            new ProcessorRecordContext(10L, 0L, 0, "in", new RecordHeaders());
        dump(out, "wc_first", serialize, ctor.newInstance(null, null, count1, ctx10), 10L);

        // wc_change: the same window changes 1->2 — prior=count1, old=count1, new=count2
        // (exercises the "old == prior" sentinel).
        ProcessorRecordContext ctx12 =
            new ProcessorRecordContext(12L, 0L, 0, "in", new RecordHeaders());
        dump(out, "wc_change", serialize, ctor.newInstance(count1, count1, count2, ctx12), 12L);

        // tombstone: a deletion — new=null.
        ProcessorRecordContext ctx20 =
            new ProcessorRecordContext(20L, 0L, 0, "in", new RecordHeaders());
        dump(out, "tombstone", serialize, ctor.newInstance(count1, count1, null, ctx20), 20L);

        System.out.println("BufferValue capture complete -> " + out.toAbsolutePath());
    }

    static void dump(Path out, String name, Method serialize, Object bufferValue, long bufferTime)
            throws Exception {
        // serialize(endPadding = 8) leaves 8 trailing bytes; the JVM logValue writes
        // the buffer time there.
        ByteBuffer buf = (ByteBuffer) serialize.invoke(bufferValue, 8);
        buf.putLong(bufferTime);
        byte[] bytes = buf.array();
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        Files.writeString(out.resolve(name + ".hex"), sb.toString(), StandardCharsets.UTF_8);
    }
}
