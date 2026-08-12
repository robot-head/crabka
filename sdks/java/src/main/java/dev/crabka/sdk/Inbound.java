package dev.crabka.sdk;

import java.util.List;
import java.util.Objects;

public record Inbound(String topic, int partition, long offset, byte[] value, List<Header> headers) {
    public Inbound {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(value, "value");
        value = value.clone();
        headers = List.copyOf(Objects.requireNonNull(headers, "headers"));
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
