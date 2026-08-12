package dev.crabka.sdk;

import java.util.Objects;

public record QueueResult(String messageId, QueueOperationError error) {
    private static final String QUEUE_MESSAGE_NOT_ACQUIRED = "queue message is not acquired";

    public QueueResult {
        Objects.requireNonNull(messageId, "messageId");
    }

    static QueueResult success(String messageId) {
        return new QueueResult(messageId, null);
    }

    static QueueResult notAcquired(String messageId) {
        return new QueueResult(messageId, new QueueOperationError("invalid_argument", QUEUE_MESSAGE_NOT_ACQUIRED));
    }
}
