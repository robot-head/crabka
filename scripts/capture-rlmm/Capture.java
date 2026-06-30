import java.util.Optional;
import java.util.TreeMap;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;

/**
 * Captures golden byte vectors from the real JVM RemoteLogMetadataSerde
 * (mirror.gcr.io/apache/kafka:4.0.0) for the five RLMM event cases used in Crabka's
 * byte-exactness proof. Prints one `name=<lowercase-hex>` line per case.
 *
 * FIXED constants (must match crates/remote-storage-topic/tests/jvm_serde_golden.rs):
 *   topicId   = new Uuid(0L, 0xCAL)  (128-bit value 0xCA)
 *   topic     = "orders"
 *   partition = 7
 *   segmentId = new Uuid(0L, 0xFEL)  (128-bit value 0xFE)
 *   startOffset=0 endOffset=99 brokerId=42 maxTimestampMs=100
 *   eventTimestampMs=123 (add cases) segmentSizeInBytes=4096
 *   segmentLeaderEpochs = {0->0, 1->50} (TreeMap, sorted ascending)
 *   customMetadata (with-custom case) = [1,2,3,4]
 */
public final class Capture {
    public static void main(String[] args) {
        final Uuid topicId = new Uuid(0L, 0xCAL);
        final String topic = "orders";
        final int partition = 7;
        final Uuid segmentUuid = new Uuid(0L, 0xFEL);

        final TopicIdPartition tp =
                new TopicIdPartition(topicId, new TopicPartition(topic, partition));
        final RemoteLogSegmentId segId = new RemoteLogSegmentId(tp, segmentUuid);

        final long startOffset = 0L;
        final long endOffset = 99L;
        final int brokerId = 42;
        final long maxTimestampMs = 100L;
        final long eventTimestampMs = 123L;
        final int segmentSizeInBytes = 4096;

        final TreeMap<Integer, Long> epochs = new TreeMap<>();
        epochs.put(0, 0L);
        epochs.put(1, 50L);

        final CustomMetadata custom = new CustomMetadata(new byte[] {1, 2, 3, 4});

        final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

        // add_with_custom: COPY_SEGMENT_STARTED, customMetadata present, txnIdxEmpty=false
        RemoteLogSegmentMetadata addWithCustom = new RemoteLogSegmentMetadata(
                segId, startOffset, endOffset, maxTimestampMs, brokerId, eventTimestampMs,
                segmentSizeInBytes, Optional.of(custom),
                RemoteLogSegmentState.COPY_SEGMENT_STARTED, epochs, false);
        emit("add_with_custom", serde.serialize(addWithCustom));

        // add_no_custom: same, Optional.empty(), txnIdxEmpty=false
        RemoteLogSegmentMetadata addNoCustom = new RemoteLogSegmentMetadata(
                segId, startOffset, endOffset, maxTimestampMs, brokerId, eventTimestampMs,
                segmentSizeInBytes, Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_STARTED, epochs, false);
        emit("add_no_custom", serde.serialize(addNoCustom));

        // add_txn_empty: same as add_no_custom but txnIdxEmpty=true
        RemoteLogSegmentMetadata addTxnEmpty = new RemoteLogSegmentMetadata(
                segId, startOffset, endOffset, maxTimestampMs, brokerId, eventTimestampMs,
                segmentSizeInBytes, Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_STARTED, epochs, true);
        emit("add_txn_empty", serde.serialize(addTxnEmpty));

        // update_finish: COPY_SEGMENT_FINISHED, brokerId 42, eventTimestampMs 456, no custom
        RemoteLogSegmentMetadataUpdate update = new RemoteLogSegmentMetadataUpdate(
                segId, 456L, Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        emit("update_finish", serde.serialize(update));

        // partition_delete_marked: DELETE_PARTITION_MARKED, brokerId 42, eventTimestampMs 789
        RemotePartitionDeleteMetadata delete = new RemotePartitionDeleteMetadata(
                tp, RemotePartitionDeleteState.DELETE_PARTITION_MARKED, 789L, brokerId);
        emit("partition_delete_marked", serde.serialize(delete));
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
