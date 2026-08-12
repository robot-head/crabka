package dev.crabka.sdk;

import java.util.List;
import java.util.Optional;

final class MockSubscription {
    private final MockStore store;
    private final List<String> topics;
    private final Optional<Filter> filter;
    private int nextIndex;

    MockSubscription(MockStore store, List<String> topics, Optional<Filter> filter) {
        this.store = store;
        this.topics = topics;
        this.filter = filter;
    }

    Inbound nextOrNull() {
        Optional<MockStore.MockDelivery> next = store.nextFrom(nextIndex, topics, filter);
        if (next.isEmpty()) {
            nextIndex = store.size();
            return null;
        }
        nextIndex = next.get().nextIndex();
        return next.get().inbound();
    }
}
