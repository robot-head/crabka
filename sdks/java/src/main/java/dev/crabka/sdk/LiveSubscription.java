package dev.crabka.sdk;

import java.time.Duration;

interface LiveSubscription {
    Inbound nextOrNull();

    Inbound nextOrNull(Duration timeout);

    void close();
}
