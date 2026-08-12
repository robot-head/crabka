package dev.crabka.sdk;

public final class InvalidArgumentException extends CrabkaException {
    public InvalidArgumentException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "invalid_argument";
    }
}
