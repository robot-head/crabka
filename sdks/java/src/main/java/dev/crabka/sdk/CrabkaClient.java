package dev.crabka.sdk;

import java.net.URI;
import java.util.Objects;

public final class CrabkaClient {
    private final URI endpoint;
    private final String bearerToken;
    private final MockStore mockStore;
    private final LiveGatewayTransport liveTransport;

    private CrabkaClient(Builder builder) {
        endpoint = URI.create(builder.endpoint);
        bearerToken = builder.bearerToken;
        mockStore = usesMockTransport(endpoint) ? new MockStore(endpoint) : null;
        liveTransport = mockStore == null ? new LiveGatewayTransport(endpoint, bearerToken) : null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String bearerToken() {
        return bearerToken;
    }

    public Messaging messaging() {
        return new Messaging(this);
    }

    public Queues queues() {
        return new Queues(this);
    }

    public Database database() {
        return new Database();
    }

    public Auth auth() {
        return new Auth(this);
    }

    public Blob blob() {
        return new Blob();
    }

    MockStore mockStore() {
        if (mockStore != null) {
            return mockStore;
        }
        throw new TransportException("live Java transport is not wired in this SDK slice");
    }

    boolean usesMockTransport() {
        return mockStore != null;
    }

    LiveGatewayTransport liveTransport() {
        if (liveTransport != null) {
            return liveTransport;
        }
        throw new TransportException("mock endpoint does not have a live transport");
    }

    private static boolean usesMockTransport(URI endpoint) {
        String scheme = endpoint.getScheme();
        return "mock".equals(scheme) || "unreachable".equals(scheme);
    }

    public static final class Builder {
        private String endpoint = "mock://gateway";
        private String bearerToken = "";

        public Builder endpoint(String endpoint) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new InvalidArgumentException("endpoint is required");
            }
            this.endpoint = endpoint;
            return this;
        }

        public Builder bearerToken(String bearerToken) {
            this.bearerToken = Objects.requireNonNullElse(bearerToken, "");
            return this;
        }

        public CrabkaClient build() {
            return new CrabkaClient(this);
        }
    }
}
