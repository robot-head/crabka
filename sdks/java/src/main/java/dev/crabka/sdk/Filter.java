package dev.crabka.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

public record Filter(String path, FilterOp op, JsonNode value) {
    public Filter {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(value, "value");
    }
}
