package dev.crabka.sdk;

public final class UnauthenticatedException extends CrabkaException {
    public UnauthenticatedException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "unauthenticated";
    }
}
