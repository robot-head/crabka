package dev.crabka.sdk;

public record RecordResult(int partition, long offset, boolean deduplicated) {}
