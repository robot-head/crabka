package dev.crabka.sdk;

import java.util.List;
import java.util.Objects;

public record Record(String topic, byte[] value, List<Header> headers) {
    public Record {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(value, "value");
        value = value.clone();
        headers = List.copyOf(Objects.requireNonNull(headers, "headers"));
    }

    public static Record of(String topic, byte[] value) {
        return new Record(topic, value, List.of());
    }

    @Override
    public byte[] value() {
        return value.clone();
    }
}
