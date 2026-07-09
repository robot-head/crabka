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
        try {
            return CompletableFuture.completedFuture(acquireSync(topic, group, max, lockDuration));
        } catch (CrabkaException error) {
            return failedFuture(error);
        }
    }

    public CompletableFuture<QueueBatchResult> acknowledge(String sessionId, List<QueueAckEntry> entries) {
        try {
            return CompletableFuture.completedFuture(acknowledgeSync(sessionId, entries));
        } catch (CrabkaException error) {
            return failedFuture(error);
        }
    }

    public CompletableFuture<QueueBatchResult> renew(String sessionId, List<QueueRenewEntry> entries) {
        try {
            return CompletableFuture.completedFuture(renewSync(sessionId, entries));
        } catch (CrabkaException error) {
            return failedFuture(error);
        }
    }

    public CompletableFuture<Void> ack(String messageId) {
        return failedFuture(new UnimplementedException("queues", "gateway-sharegroup-rpc"));
    }

    private QueueAcquireResult acquireSync(String topic, String group, int max, Duration lockDuration) {
        Objects.requireNonNull(topic, "topic");
        assertSupportedAcquireOptions(group, lockDuration);
        if (client.usesMockTransport()) {
            return client.mockStore().acquireQueueMessages(topic, max);
        }
        return client.liveTransport().queueAcquire(topic, group, max, DEFAULT_LOCK_DURATION_MS);
    }

    private QueueBatchResult acknowledgeSync(String sessionId, List<QueueAckEntry> entries) {
        assertSessionId(sessionId);
        List<QueueAckEntry> copiedEntries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (client.usesMockTransport()) {
            return client.mockStore().acknowledgeQueueMessages(copiedEntries);
        }
        return client.liveTransport().queueAcknowledge(sessionId, copiedEntries);
    }

    private QueueBatchResult renewSync(String sessionId, List<QueueRenewEntry> entries) {
        assertSessionId(sessionId);
        List<QueueRenewEntry> copiedEntries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (client.usesMockTransport()) {
            return client.mockStore().renewQueueMessages(copiedEntries);
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

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }
}
