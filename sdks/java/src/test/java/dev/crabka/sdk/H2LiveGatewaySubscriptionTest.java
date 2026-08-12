package dev.crabka.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class H2LiveGatewaySubscriptionTest {
    private static final URI SUBSCRIBE_URI = URI.create("http://gateway.test/crabka.gateway.v1.Gateway/Subscribe");
    private static final byte[] START_FRAME = new byte[] {0, 0, 0, 0, 0};

    @Test
    void closeDuringSetupClosesResourceRegisteredAfterClose() throws Exception {
        CountDownLatch openerStarted = new CountDownLatch(1);
        CountDownLatch allowResourceRegistration = new CountDownLatch(1);
        CountDownLatch resourceClosed = new CountDownLatch(1);
        H2LiveGatewaySubscription.StreamOpener opener = (uri, bearerToken, resources) -> {
            openerStarted.countDown();
            awaitLatch(allowResourceRegistration);
            resources.register(resourceClosed::countDown);
            return new IdleGatewayStream();
        };

        H2LiveGatewaySubscription subscription = new H2LiveGatewaySubscription(SUBSCRIBE_URI, "", START_FRAME, opener);
        assertTrue(openerStarted.await(1, TimeUnit.SECONDS));

        subscription.close();
        allowResourceRegistration.countDown();

        assertTrue(resourceClosed.await(1, TimeUnit.SECONDS));
        assertNull(subscription.nextOrNull(Duration.ofMillis(100)));
    }

    @Test
    void closeDuringHeaderWaitClosesRegisteredStream() throws Exception {
        CountDownLatch takingHeaders = new CountDownLatch(1);
        CountDownLatch streamClosed = new CountDownLatch(1);
        H2LiveGatewaySubscription.StreamOpener opener = (uri, bearerToken, resources) -> {
            resources.register(streamClosed::countDown);
            return new IdleGatewayStream() {
                @Override
                public void writeStartFrame(byte[] startFrame) {
                    assertArrayEquals(START_FRAME, startFrame);
                }

                @Override
                public String takeStatus() throws IOException {
                    takingHeaders.countDown();
                    awaitLatch(streamClosed);
                    throw new EOFException("closed by test");
                }
            };
        };
        H2LiveGatewaySubscription subscription = new H2LiveGatewaySubscription(SUBSCRIBE_URI, "", START_FRAME, opener);
        assertTrue(takingHeaders.await(1, TimeUnit.SECONDS));

        subscription.close();

        assertTrue(streamClosed.await(1, TimeUnit.SECONDS));
        assertNull(subscription.nextOrNull(Duration.ofMillis(100)));
    }

    @Test
    void closeDuringMessageReadClosesRegisteredStream() throws Exception {
        CountDownLatch readingMessage = new CountDownLatch(1);
        CountDownLatch streamClosed = new CountDownLatch(1);
        H2LiveGatewaySubscription.StreamOpener opener = (uri, bearerToken, resources) -> {
            resources.register(streamClosed::countDown);
            return new IdleGatewayStream() {
                @Override
                public String takeStatus() {
                    return "200";
                }

                @Override
                public Inbound readNextInbound() throws IOException {
                    readingMessage.countDown();
                    awaitLatch(streamClosed);
                    throw new EOFException("closed by test");
                }
            };
        };
        H2LiveGatewaySubscription subscription = new H2LiveGatewaySubscription(SUBSCRIBE_URI, "", START_FRAME, opener);
        assertTrue(readingMessage.await(1, TimeUnit.SECONDS));

        subscription.close();

        assertTrue(streamClosed.await(1, TimeUnit.SECONDS));
        assertNull(subscription.nextOrNull(Duration.ofMillis(100)));
    }

    @Test
    void slowSubscriberBackpressuresTheGatewayReader() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        CountDownLatch inboxFull = new CountDownLatch(1);
        CountDownLatch resumed = new CountDownLatch(1);
        H2LiveGatewaySubscription.StreamOpener opener = (uri, bearerToken, resources) -> new IdleGatewayStream() {
            @Override
            public String takeStatus() {
                return "200";
            }

            @Override
            public Inbound readNextInbound() {
                int read = reads.incrementAndGet();
                if (read == H2LiveGatewaySubscription.INBOX_CAPACITY + 1) {
                    inboxFull.countDown();
                }
                if (read == H2LiveGatewaySubscription.INBOX_CAPACITY + 2) {
                    resumed.countDown();
                }
                return new Inbound("topic", 0, read, new byte[0], List.of());
            }
        };
        H2LiveGatewaySubscription subscription = new H2LiveGatewaySubscription(SUBSCRIBE_URI, "", START_FRAME, opener);

        assertTrue(inboxFull.await(1, TimeUnit.SECONDS));
        assertEquals(H2LiveGatewaySubscription.INBOX_CAPACITY + 1, reads.get());
        subscription.nextOrNull(Duration.ofSeconds(1));
        assertTrue(resumed.await(1, TimeUnit.SECONDS));

        subscription.close();
    }

    @Test
    void unexpectedEofIsATransportError() {
        H2LiveGatewaySubscription.StreamOpener opener = (uri, bearerToken, resources) -> new IdleGatewayStream() {
            @Override
            public String takeStatus() {
                return "200";
            }

            @Override
            public Inbound readNextInbound() throws IOException {
                throw new EOFException("stream ended mid-frame");
            }
        };
        H2LiveGatewaySubscription subscription = new H2LiveGatewaySubscription(SUBSCRIBE_URI, "", START_FRAME, opener);

        TransportException error = assertThrows(
                TransportException.class, () -> subscription.nextOrNull(Duration.ofSeconds(1)));
        assertTrue(error.getMessage().contains("stream ended mid-frame"));
    }

    @Test
    void explicitEndStreamCompletesCleanly() {
        H2LiveGatewaySubscription.StreamOpener opener = (uri, bearerToken, resources) -> new IdleGatewayStream() {
            @Override
            public String takeStatus() {
                return "200";
            }

            @Override
            public Inbound readNextInbound() throws IOException {
                throw LiveGatewaySubscription.endStream(new byte[0]);
            }
        };
        H2LiveGatewaySubscription subscription = new H2LiveGatewaySubscription(SUBSCRIBE_URI, "", START_FRAME, opener);

        assertNull(subscription.nextOrNull(Duration.ofSeconds(1)));
    }

    private static void awaitLatch(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new IOException("timed out waiting for test latch");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for test latch", error);
        }
    }

    private static class IdleGatewayStream implements H2LiveGatewaySubscription.GatewayStream {
        @Override
        public void writeStartFrame(byte[] startFrame) {}

        @Override
        public String takeStatus() throws IOException {
            throw new EOFException("closed by test");
        }

        @Override
        public Inbound readNextInbound() throws IOException {
            throw new EOFException("closed by test");
        }
    }
}
