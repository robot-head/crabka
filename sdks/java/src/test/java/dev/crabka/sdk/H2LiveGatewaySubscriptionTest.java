package dev.crabka.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
