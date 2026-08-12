package dev.crabka.sdk;

import java.util.Arrays;
import java.util.Objects;

public record Header(String name, byte[] value) {
    public Header {
        Objects.requireNonNull(name, "name");
        value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] value() {
        if (value == null) {
            return null;
        }
        return Arrays.copyOf(value, value.length);
    }
}
