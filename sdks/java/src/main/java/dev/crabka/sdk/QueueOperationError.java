package dev.crabka.sdk;

import java.util.Objects;

public record QueueOperationError(String kind, String message) {
    public QueueOperationError {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(message, "message");
    }
}
