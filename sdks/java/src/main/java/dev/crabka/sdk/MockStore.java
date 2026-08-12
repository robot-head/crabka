package dev.crabka.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class MockStore {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final URI endpoint;
    private final List<MockMessage> messages = new ArrayList<>();
    private final Map<String, MockQueueSession> queueSessions = new HashMap<>();
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

    synchronized QueueAcquireResult acquireQueueMessages(String topic, String group, int maxMessages, String sessionId) {
        int effectiveMax = Math.min(Math.max(maxMessages, 1), 500);
        MockQueueSession session;
        if (sessionId.isBlank()) {
            sessionId = "queue-session-" + nextQueueSessionId;
            nextQueueSessionId += 1;
            session = new MockQueueSession(topic, group, effectiveMax);
            queueSessions.put(sessionId, session);
        } else {
            session = requireQueueSession(sessionId);
            if (!session.topic().equals(topic) || !session.group().equals(group)) {
                throw new InvalidArgumentException("group_id and topics are fixed when a queue session is created");
            }
            if (maxMessages != 0 && effectiveMax != session.maxMessages()) {
                throw new InvalidArgumentException("max_messages is fixed when a queue session is created");
            }
        }

        String acquiredBy = sessionId;
        String acquiredGroup = session.group();
        List<QueueMessage> acquiredMessages = messages.stream()
                .filter(message -> message.record().topic().equals(topic))
                .filter(message -> message.queueDelivery(acquiredGroup).queueState() == QueueState.AVAILABLE)
                .limit(effectiveMax)
                .map(message -> acquireQueueMessage(message, acquiredGroup, acquiredBy))
                .toList();
        return new QueueAcquireResult(sessionId, acquiredMessages);
    }

    synchronized QueueBatchResult acknowledgeQueueMessages(String sessionId, List<QueueAckEntry> entries) {
        String group = requireQueueSession(sessionId).group();
        List<QueueResult> results = entries.stream()
                .map(entry -> acknowledgeQueueMessage(group, sessionId, entry))
                .toList();
        return new QueueBatchResult(results);
    }

    synchronized QueueBatchResult renewQueueMessages(String sessionId, List<QueueRenewEntry> entries) {
        String group = requireQueueSession(sessionId).group();
        List<QueueResult> results = entries.stream()
                .map(entry -> renewQueueMessage(group, sessionId, entry))
                .toList();
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

    private QueueMessage acquireQueueMessage(MockMessage message, String group, String sessionId) {
        MockQueueDelivery delivery = message.queueDelivery(group);
        delivery.acquire(sessionId);
        return new QueueMessage(
                messageId(message),
                message.record().topic(),
                message.partition(),
                message.offset(),
                message.record().value(),
                message.record().headers(),
                delivery.deliveryCount());
    }

    private QueueResult acknowledgeQueueMessage(String group, String sessionId, QueueAckEntry entry) {
        Optional<MockQueueDelivery> delivery = acquiredDelivery(group, sessionId, entry.messageId());
        if (delivery.isEmpty()) {
            return QueueResult.notAcquired(entry.messageId());
        }
        delivery.get().acknowledge(entry.ackType());
        return QueueResult.success(entry.messageId());
    }

    private QueueResult renewQueueMessage(String group, String sessionId, QueueRenewEntry entry) {
        if (acquiredDelivery(group, sessionId, entry.messageId()).isEmpty()) {
            return QueueResult.notAcquired(entry.messageId());
        }
        return QueueResult.success(entry.messageId());
    }

    private Optional<MockQueueDelivery> acquiredDelivery(String group, String sessionId, String messageId) {
        return messages.stream()
                .filter(message -> messageId(message).equals(messageId))
                .map(message -> message.queueDelivery(group))
                .filter(delivery -> delivery.queueState() == QueueState.ACQUIRED)
                .filter(delivery -> sessionId.equals(delivery.queueSessionId()))
                .findFirst();
    }

    private MockQueueSession requireQueueSession(String sessionId) {
        MockQueueSession session = queueSessions.get(sessionId);
        if (session == null) {
            throw new InvalidArgumentException("queue session expired; re-acquire");
        }
        return session;
    }

    private static String messageId(MockMessage message) {
        return message.record().topic() + ":" + message.partition() + ":" + message.offset();
    }

    record MockDelivery(int nextIndex, Inbound inbound) {}

    private record MockQueueSession(String topic, String group, int maxMessages) {}

    private static final class MockMessage {
        private final Record record;
        private final int partition;
        private final long offset;
        private final Map<String, MockQueueDelivery> queueDeliveries = new HashMap<>();

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

        private MockQueueDelivery queueDelivery(String group) {
            return queueDeliveries.computeIfAbsent(group, ignored -> new MockQueueDelivery());
        }
    }

    private static final class MockQueueDelivery {
        private QueueState queueState = QueueState.AVAILABLE;
        private String queueSessionId;
        private int deliveryCount;

        private QueueState queueState() {
            return queueState;
        }

        private int deliveryCount() {
            return deliveryCount;
        }

        private String queueSessionId() {
            return queueSessionId;
        }

        private void acquire(String sessionId) {
            queueState = QueueState.ACQUIRED;
            queueSessionId = sessionId;
            deliveryCount += 1;
        }

        private void acknowledge(QueueAckType ackType) {
            queueState = switch (ackType) {
                case ACCEPT -> QueueState.ACCEPTED;
                case RELEASE -> QueueState.AVAILABLE;
                case REJECT -> QueueState.REJECTED;
            };
            queueSessionId = null;
        }
    }

    private enum QueueState {
        AVAILABLE,
        ACQUIRED,
        ACCEPTED,
        REJECTED
    }
}
