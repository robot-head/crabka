package dev.crabka.sdk;

import java.util.List;
import java.util.Objects;

public record QueueBatchResult(List<QueueResult> results) {
    public QueueBatchResult {
        results = List.copyOf(Objects.requireNonNull(results, "results"));
    }
}
