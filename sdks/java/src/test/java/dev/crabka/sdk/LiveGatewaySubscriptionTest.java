package dev.crabka.sdk;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import org.junit.jupiter.api.Test;

final class LiveGatewaySubscriptionTest {
    @Test
    void truncatedFramesRemainUnexpectedEof() {
        for (byte[] frame : new byte[][] {
                {0, 0},
                {0, 0, 0, 0, 2, 1},
        }) {
            EOFException error = assertThrows(EOFException.class, () -> LiveGatewaySubscription.readNextInbound(
                    new DataInputStream(new ByteArrayInputStream(frame))));

            assertFalse(error instanceof LiveGatewaySubscription.EndStreamException);
        }
    }

    @Test
    void explicitEndStreamUsesTheCleanCompletionMarker() {
        byte[] frame = {2, 0, 0, 0, 0};

        assertThrows(
                LiveGatewaySubscription.EndStreamException.class,
                () -> LiveGatewaySubscription.readNextInbound(
                        new DataInputStream(new ByteArrayInputStream(frame))));
    }
}
