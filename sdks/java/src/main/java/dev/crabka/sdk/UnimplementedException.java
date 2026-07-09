package dev.crabka.sdk;

import java.util.Objects;

public final class UnimplementedException extends CrabkaException {
    private final String module;
    private final String gatedOn;

    public UnimplementedException(String module, String gatedOn) {
        super(module + " is gated on " + gatedOn);
        this.module = Objects.requireNonNull(module, "module");
        this.gatedOn = Objects.requireNonNull(gatedOn, "gatedOn");
    }

    public String module() {
        return module;
    }

    public String gatedOn() {
        return gatedOn;
    }

    @Override
    public String kind() {
        return "unimplemented";
    }
}
