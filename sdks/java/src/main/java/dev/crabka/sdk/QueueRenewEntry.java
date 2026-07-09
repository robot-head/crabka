package dev.crabka.sdk;

import java.util.Objects;

public record QueueRenewEntry(String messageId) {
    public QueueRenewEntry {
        Objects.requireNonNull(messageId, "messageId");
    }
}
