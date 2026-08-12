package dev.crabka.sdk;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class Queues {
    private static final long DEFAULT_LOCK_DURATION_MS = 30_000;

    private final CrabkaClient client;

    Queues(CrabkaClient client) {
        this.client = client;
    }

    public CompletableFuture<QueueAcquireResult> acquire(String topic, String group, int max, Duration lockDuration) {
        return acquireWithSession(topic, group, max, lockDuration, "");
    }

    public CompletableFuture<QueueAcquireResult> acquireWithSession(
            String topic, String group, int max, Duration lockDuration, String sessionId) {
        return CompletableFuture.supplyAsync(() -> acquireSync(topic, group, max, lockDuration, sessionId));
    }

    public CompletableFuture<QueueBatchResult> acknowledge(String sessionId, List<QueueAckEntry> entries) {
        return CompletableFuture.supplyAsync(() -> acknowledgeSync(sessionId, entries));
    }

    public CompletableFuture<QueueBatchResult> renew(String sessionId, List<QueueRenewEntry> entries) {
        return CompletableFuture.supplyAsync(() -> renewSync(sessionId, entries));
    }

    public CompletableFuture<Void> ack(String messageId) {
        return CompletableFuture.failedFuture(new UnimplementedException("queues", "gateway-sharegroup-rpc"));
    }

    private QueueAcquireResult acquireSync(
            String topic, String group, int max, Duration lockDuration, String sessionId) {
        Objects.requireNonNull(topic, "topic");
        if (topic.isBlank()) {
            throw new InvalidArgumentException("queue topic is required");
        }
        assertSupportedAcquireOptions(group, lockDuration);
        if (client.usesMockTransport()) {
            return client.mockStore().acquireQueueMessages(topic, group, max, sessionId);
        }
        return client.liveTransport().queueAcquire(topic, group, max, DEFAULT_LOCK_DURATION_MS, sessionId);
    }

    private QueueBatchResult acknowledgeSync(String sessionId, List<QueueAckEntry> entries) {
        assertSessionId(sessionId);
        List<QueueAckEntry> copiedEntries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (client.usesMockTransport()) {
            return client.mockStore().acknowledgeQueueMessages(sessionId, copiedEntries);
        }
        return client.liveTransport().queueAcknowledge(sessionId, copiedEntries);
    }

    private QueueBatchResult renewSync(String sessionId, List<QueueRenewEntry> entries) {
        assertSessionId(sessionId);
        List<QueueRenewEntry> copiedEntries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (client.usesMockTransport()) {
            return client.mockStore().renewQueueMessages(sessionId, copiedEntries);
        }
        return client.liveTransport().queueRenew(sessionId, copiedEntries);
    }

    private static void assertSupportedAcquireOptions(String group, Duration lockDuration) {
        Objects.requireNonNull(lockDuration, "lockDuration");
        if (group == null || group.isBlank()) {
            throw new InvalidArgumentException("queue group is required");
        }
        if (lockDuration.toMillis() != DEFAULT_LOCK_DURATION_MS) {
            throw new InvalidArgumentException("queue lock_duration_ms must be 30000; per-acquire lock durations are not supported");
        }
    }

    private static void assertSessionId(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new InvalidArgumentException("queue session_id is required");
        }
    }
}
