package dev.crabka.sdk;

public final class NotFoundException extends CrabkaException {
    public NotFoundException(String message) {
        super(message);
    }

    @Override
    public String kind() {
        return "not_found";
    }
}
