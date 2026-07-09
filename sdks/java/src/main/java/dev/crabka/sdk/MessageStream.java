package dev.crabka.sdk;

import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class MessageStream implements Iterator<Inbound>, AutoCloseable {
    private final MockSubscription subscription;
    private final LiveSubscription liveSubscription;
    private boolean closed;
    private Inbound buffered;

    MessageStream(MockSubscription subscription) {
        this.subscription = subscription;
        liveSubscription = null;
    }

    MessageStream(LiveSubscription liveSubscription) {
        subscription = null;
        this.liveSubscription = liveSubscription;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (buffered != null) {
            return true;
        }
        buffered = nextOrNull();
        return buffered != null;
    }

    @Override
    public Inbound next() {
        if (!hasNext()) {
            throw new NoSuchElementException("no message available");
        }
        Inbound next = buffered;
        buffered = null;
        return next;
    }

    @Override
    public void close() {
        closed = true;
        buffered = null;
        if (liveSubscription != null) {
            liveSubscription.close();
        }
    }

    Inbound nextWithin(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new InvalidArgumentException("timeout must be non-negative");
        }
        if (closed) {
            return null;
        }
        if (buffered == null) {
            buffered = nextOrNull(timeout);
        }
        if (buffered == null) {
            return null;
        }
        Inbound next = buffered;
        buffered = null;
        return next;
    }

    private Inbound nextOrNull() {
        if (subscription != null) {
            return subscription.nextOrNull();
        }
        return liveSubscription.nextOrNull();
    }

    private Inbound nextOrNull(Duration timeout) {
        if (subscription != null) {
            return subscription.nextOrNull();
        }
        return liveSubscription.nextOrNull(timeout);
    }
}
