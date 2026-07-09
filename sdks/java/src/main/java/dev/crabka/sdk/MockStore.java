package dev.crabka.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class MockStore {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final URI endpoint;
    private final List<MockMessage> messages = new ArrayList<>();
    private int nextQueueSessionId = 1;

    MockStore(URI endpoint) {
        this.endpoint = endpoint;
    }

    synchronized RecordResult publish(Record record) {
        if ("unreachable".equals(endpoint.getScheme())) {
            throw new TransportException("endpoint unreachable");
        }
        long offset = messages.stream().filter(message -> message.record().topic().equals(record.topic())).count();
        messages.add(new MockMessage(record, 0, offset));
        return new RecordResult(0, offset, false);
    }

    synchronized MessageStream subscribe(List<String> topics, Optional<Filter> filter) {
        return new MessageStream(new MockSubscription(this, List.copyOf(topics), filter));
    }

    synchronized Optional<MockDelivery> nextFrom(int index, List<String> topics, Optional<Filter> filter) {
        for (int nextIndex = index; nextIndex < messages.size(); nextIndex++) {
            MockMessage message = messages.get(nextIndex);
            if (!topics.contains(message.record().topic())) {
                continue;
            }
            if (!matches(filter, message.record().value())) {
                continue;
            }
            Inbound inbound = new Inbound(
                    message.record().topic(),
                    message.partition(),
                    message.offset(),
                    message.record().value(),
                    message.record().headers());
            return Optional.of(new MockDelivery(nextIndex + 1, inbound));
        }
        return Optional.empty();
    }

    synchronized int size() {
        return messages.size();
    }

    synchronized QueueAcquireResult acquireQueueMessages(String topic, int maxMessages) {
        String sessionId = "queue-session-" + nextQueueSessionId;
        nextQueueSessionId += 1;

        List<QueueMessage> acquiredMessages = messages.stream()
                .filter(message -> message.record().topic().equals(topic))
                .filter(message -> message.queueState() == QueueState.AVAILABLE)
                .limit(maxMessages)
                .map(this::acquireQueueMessage)
                .toList();
        return new QueueAcquireResult(sessionId, acquiredMessages);
    }

    synchronized QueueBatchResult acknowledgeQueueMessages(List<QueueAckEntry> entries) {
        List<QueueResult> results = entries.stream().map(this::acknowledgeQueueMessage).toList();
        return new QueueBatchResult(results);
    }

    synchronized QueueBatchResult renewQueueMessages(List<QueueRenewEntry> entries) {
        List<QueueResult> results = entries.stream().map(this::renewQueueMessage).toList();
        return new QueueBatchResult(results);
    }

    private static boolean matches(Optional<Filter> filter, byte[] value) {
        if (filter.isEmpty()) {
            return true;
        }
        Filter parsedFilter = filter.get();
        if (parsedFilter.op() != FilterOp.EQUALS || !parsedFilter.path().startsWith("$.")) {
            return false;
        }
        try {
            JsonNode decoded = JSON.readTree(value);
            return decoded.path(parsedFilter.path().substring(2)).equals(parsedFilter.value());
        } catch (Exception ignored) {
            return false;
        }
    }

    private QueueMessage acquireQueueMessage(MockMessage message) {
        message.acquire();
        return new QueueMessage(
                messageId(message),
                message.record().topic(),
                message.partition(),
                message.offset(),
                message.record().value(),
                message.record().headers(),
                message.deliveryCount());
    }

    private QueueResult acknowledgeQueueMessage(QueueAckEntry entry) {
        Optional<MockMessage> message = acquiredMessage(entry.messageId());
        if (message.isEmpty()) {
            return QueueResult.notAcquired(entry.messageId());
        }
        message.get().acknowledge(entry.ackType());
        return QueueResult.success(entry.messageId());
    }

    private QueueResult renewQueueMessage(QueueRenewEntry entry) {
        if (acquiredMessage(entry.messageId()).isEmpty()) {
            return QueueResult.notAcquired(entry.messageId());
        }
        return QueueResult.success(entry.messageId());
    }

    private Optional<MockMessage> acquiredMessage(String messageId) {
        return messages.stream()
                .filter(message -> message.queueState() == QueueState.ACQUIRED)
                .filter(message -> messageId(message).equals(messageId))
                .findFirst();
    }

    private static String messageId(MockMessage message) {
        return message.record().topic() + ":" + message.partition() + ":" + message.offset();
    }

    record MockDelivery(int nextIndex, Inbound inbound) {}

    private static final class MockMessage {
        private final Record record;
        private final int partition;
        private final long offset;
        private QueueState queueState = QueueState.AVAILABLE;
        private int deliveryCount;

        private MockMessage(Record record, int partition, long offset) {
            this.record = record;
            this.partition = partition;
            this.offset = offset;
        }

        private Record record() {
            return record;
        }

        private int partition() {
            return partition;
        }

        private long offset() {
            return offset;
        }

        private QueueState queueState() {
            return queueState;
        }

        private int deliveryCount() {
            return deliveryCount;
        }

        private void acquire() {
            queueState = QueueState.ACQUIRED;
            deliveryCount += 1;
        }

        private void acknowledge(QueueAckType ackType) {
            queueState = switch (ackType) {
                case ACCEPT -> QueueState.ACCEPTED;
                case RELEASE -> QueueState.AVAILABLE;
                case REJECT -> QueueState.REJECTED;
            };
        }
    }

    private enum QueueState {
        AVAILABLE,
        ACQUIRED,
        ACCEPTED,
        REJECTED
    }
}
