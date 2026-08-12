package dev.crabka.sdk;

import java.util.Objects;
import java.util.Optional;

public record CloudEvent(
        String id,
        String source,
        String type,
        String specversion,
        Optional<String> datacontenttype,
        byte[] data) {
    public CloudEvent {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(specversion, "specversion");
        datacontenttype = Objects.requireNonNull(datacontenttype, "datacontenttype");
        Objects.requireNonNull(data, "data");
        data = data.clone();
    }

    public static CloudEvent of(String id, String source, String type, String specversion, byte[] data) {
        return new CloudEvent(id, source, type, specversion, Optional.empty(), data);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
