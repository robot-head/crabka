package dev.crabka.sdk;

import java.util.Objects;

public record QueueOperationError(String kind, String message, boolean retriable) {
    public QueueOperationError(String kind, String message) {
        this(kind, message, false);
    }

    public QueueOperationError {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(message, "message");
    }
}
