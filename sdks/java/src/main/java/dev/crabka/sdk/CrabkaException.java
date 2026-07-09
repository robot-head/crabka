package dev.crabka.sdk;

public abstract class CrabkaException extends RuntimeException {
    protected CrabkaException(String message) {
        super(message);
    }

    protected CrabkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public abstract String kind();
}
