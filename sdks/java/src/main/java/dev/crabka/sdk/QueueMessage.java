package dev.crabka.sdk;

import java.util.List;
import java.util.Objects;

public record QueueMessage(String messageId, String topic, int partition, long offset, byte[] value, List<Header> headers, int deliveryCount) {
    public QueueMessage {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(topic, "topic");
        value = value == null ? null : value.clone();
        headers = List.copyOf(Objects.requireNonNull(headers, "headers"));
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }
}
