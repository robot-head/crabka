package dev.crabka.sdk;

import java.util.concurrent.CompletableFuture;

public final class Database {
    public CompletableFuture<Void> connect(String name) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new UnimplementedException("database", "chapter-f-control-plane"));
        return future;
    }
}
