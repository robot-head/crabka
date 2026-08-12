package dev.crabka.sdk;

import java.util.concurrent.CompletableFuture;

public final class Blob {
    public CompletableFuture<Void> put(String key, byte[] value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new UnimplementedException("blob", "chapter-b-blob-api"));
        return future;
    }

    public CompletableFuture<byte[]> get(String key) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        future.completeExceptionally(new UnimplementedException("blob", "chapter-b-blob-api"));
        return future;
    }
}
