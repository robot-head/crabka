package dev.crabka.sdk;

public final class ServerException extends CrabkaException {
    public ServerException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "server_error";
    }
}
