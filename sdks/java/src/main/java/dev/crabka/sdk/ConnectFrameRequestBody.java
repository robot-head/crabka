package dev.crabka.sdk;

import com.google.protobuf.Message;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

final class ConnectFrameRequestBody extends RequestBody implements AutoCloseable {
    private static final MediaType CONNECT_STREAM_PROTO = MediaType.get("application/connect+proto");

    private final Message message;
    private final CountDownLatch closed = new CountDownLatch(1);

    ConnectFrameRequestBody(Message message) {
        this.message = message;
    }

    @Override
    public MediaType contentType() {
        return CONNECT_STREAM_PROTO;
    }

    @Override
    public boolean isDuplex() {
        return true;
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        sink.write(encodedFrame());
        sink.flush();
        waitUntilClosed();
    }

    @Override
    public void close() {
        closed.countDown();
    }

    byte[] encodedFrame() {
        byte[] payload = message.toByteArray();
        byte[] frame = new byte[5 + payload.length];
        frame[0] = 0;
        frame[1] = (byte) (payload.length >>> 24);
        frame[2] = (byte) (payload.length >>> 16);
        frame[3] = (byte) (payload.length >>> 8);
        frame[4] = (byte) payload.length;
        System.arraycopy(payload, 0, frame, 5, payload.length);
        return frame;
    }

    private void waitUntilClosed() throws IOException {
        try {
            closed.await();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while holding Subscribe request stream open", error);
        }
    }
}
