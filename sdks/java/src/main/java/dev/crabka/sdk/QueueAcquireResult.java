package dev.crabka.sdk;

import java.util.List;
import java.util.Objects;

public record QueueAcquireResult(String sessionId, List<QueueMessage> messages) {
    public QueueAcquireResult {
        Objects.requireNonNull(sessionId, "sessionId");
        messages = List.copyOf(Objects.requireNonNull(messages, "messages"));
    }
}
