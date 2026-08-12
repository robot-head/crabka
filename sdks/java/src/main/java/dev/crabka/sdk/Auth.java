package dev.crabka.sdk;

import java.util.concurrent.CompletableFuture;

public final class Auth {
    private final CrabkaClient client;

    Auth(CrabkaClient client) {
        this.client = client;
    }

    public String bearerToken() {
        return client.bearerToken();
    }

    public CompletableFuture<Void> signIn(String username, String password) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new UnauthenticatedException("identity APIs are not part of contract v1"));
        return future;
    }
}
