// Declared in the MM2 package so it can call the package-private
// recordKey()/recordValue() methods on Heartbeat/Checkpoint/OffsetSync.
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.TopicPartition;

/**
 * Captures golden byte vectors from the real JVM MirrorMaker-2 record codecs
 * (mirror.gcr.io/apache/kafka:4.0.0) for Crabka's MM2 byte-exactness proof. Prints one
 * `name=<lowercase-hex>` line per key/value.
 *
 * The three record classes live in connect-mirror-4.0.0.jar:
 *   org.apache.kafka.connect.mirror.{Heartbeat,Checkpoint,OffsetSync}
 * Verified constructor + accessor signatures (javap):
 *   Heartbeat(String sourceClusterAlias, String targetClusterAlias, long ts)
 *   Checkpoint(String consumerGroupId, TopicPartition tp, long upstream,
 *              long downstream, String metadata)
 *   OffsetSync(TopicPartition tp, long upstream, long downstream)
 * All three expose package-private `byte[] recordKey()` / `byte[] recordValue()`,
 * hence this class is declared in the same package.
 *
 * FIXED constants (must match crates/replicator/tests/mm2_golden_jvm.rs):
 *   source     = "us-east"
 *   target     = "eu-west"
 *   timestamp  = 100
 *   group      = "analytics"
 *   topic      = "orders"
 *   partition  = 7
 *   upstream   = 1000
 *   downstream = 742
 *   metadata   = "" (empty string)
 */
public final class Capture {
    public static void main(String[] args) {
        final String source = "us-east";
        final String target = "eu-west";
        final long timestamp = 100L;
        final String group = "analytics";
        final String topic = "orders";
        final int partition = 7;
        final long upstream = 1000L;
        final long downstream = 742L;
        final String metadata = "";

        final TopicPartition tp = new TopicPartition(topic, partition);

        final Heartbeat heartbeat = new Heartbeat(source, target, timestamp);
        emit("heartbeat_key", heartbeat.recordKey());
        emit("heartbeat_value", heartbeat.recordValue());

        final Checkpoint checkpoint =
                new Checkpoint(group, tp, upstream, downstream, metadata);
        emit("checkpoint_key", checkpoint.recordKey());
        emit("checkpoint_value", checkpoint.recordValue());

        final OffsetSync offsetSync = new OffsetSync(tp, upstream, downstream);
        emit("offset_sync_key", offsetSync.recordKey());
        emit("offset_sync_value", offsetSync.recordValue());
    }

    private static void emit(String name, byte[] bytes) {
        StringBuilder sb = new StringBuilder(name).append('=');
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        System.out.println(sb);
    }
}
