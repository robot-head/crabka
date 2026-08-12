package dev.crabka.sdk;

import java.util.Objects;

public record QueueAckEntry(String messageId, QueueAckType ackType) {
    public QueueAckEntry {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(ackType, "ackType");
    }
}
