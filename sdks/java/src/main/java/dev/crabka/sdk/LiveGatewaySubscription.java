package dev.crabka.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import crabka.gateway.v1.GatewayOuterClass;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;
import okhttp3.ResponseBody;

final class LiveGatewaySubscription implements LiveSubscription {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final StreamItem END = new StreamItem(null, null, true);

    private final Call call;
    private final ConnectFrameRequestBody requestBody;
    private final BlockingQueue<StreamItem> inbox = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    LiveGatewaySubscription(Call call, ConnectFrameRequestBody requestBody) {
        this.call = call;
        this.requestBody = requestBody;
        start();
    }

    @Override
    public Inbound nextOrNull() {
        return awaitNext(0, null);
    }

    @Override
    public Inbound nextOrNull(Duration timeout) {
        return awaitNext(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        requestBody.close();
        call.cancel();
        inbox.offer(END);
    }

    private void start() {
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException error) {
                requestBody.close();
                if (closed.get()) {
                    sendEnd();
                    return;
                }
                sendError(new TransportException("Subscribe request failed", error));
            }

            @Override
            public void onResponse(Call call, Response response) {
                try (response) {
                    readResponse(response);
                    sendEnd();
                } catch (EndStreamException done) {
                    sendEnd();
                } catch (CrabkaException error) {
                    sendError(error);
                } catch (IOException error) {
                    sendError(new TransportException("Subscribe stream read failed: " + error.getMessage(), error));
                } finally {
                    requestBody.close();
                }
            }
        });
    }

    private Inbound awaitNext(long timeout, TimeUnit unit) {
        if (closed.get() && inbox.isEmpty()) {
            return null;
        }
        try {
            StreamItem item = unit == null ? inbox.take() : inbox.poll(timeout, unit);
            if (item == null || item.end()) {
                return null;
            }
            if (item.error() != null) {
                throw item.error();
            }
            return item.inbound();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new TransportException("interrupted while waiting for Subscribe message", error);
        }
    }

    private void readResponse(Response response) throws IOException {
        if (!response.isSuccessful()) {
            throw LiveGatewayTransport.errorForResponse(response);
        }
        ResponseBody body = response.body();
        if (body == null) {
            throw new TransportException("Subscribe response did not include a body");
        }
        readInboundFrames(body.byteStream());
    }

    private void readInboundFrames(InputStream input) throws IOException {
        DataInputStream frames = new DataInputStream(input);
        while (!closed.get()) {
            sendInbound(readNextInbound(frames));
        }
    }

    private void sendInbound(Inbound inbound) {
        if (!closed.get()) {
            inbox.offer(new StreamItem(inbound, null, false));
        }
    }

    private void sendError(CrabkaException error) {
        if (closed.compareAndSet(false, true)) {
            inbox.offer(new StreamItem(null, error, false));
        }
    }

    private void sendEnd() {
        if (closed.compareAndSet(false, true)) {
            inbox.offer(END);
        }
    }

    static Inbound readNextInbound(DataInputStream input) throws IOException {
        while (true) {
            int flags = input.readUnsignedByte();
            int length = input.readInt();
            if ((flags & 0x01) != 0) {
                throw new TransportException("Subscribe stream returned a compressed frame");
            }
            if (length < 0) {
                throw new TransportException("Subscribe stream returned a negative frame length");
            }
            byte[] payload = input.readNBytes(length);
            if (payload.length != length) {
                throw new EOFException("Subscribe stream ended mid-frame");
            }
            if (flags == 0x00) {
                return fromGatewayInbound(GatewayOuterClass.Inbound.parseFrom(payload));
            }
            if (flags == 0x02) {
                throw endStream(payload);
            }
            throw new TransportException("Subscribe stream returned unknown frame flags " + flags);
        }
    }

    static EndStreamException endStream(byte[] payload) throws IOException {
        if (payload.length == 0) {
            return new EndStreamException();
        }
        JsonNode error = JSON.readTree(payload).path("error");
        if (!error.isMissingNode()) {
            throw new TransportException(error.path("message").asText("Subscribe stream ended with an error"));
        }
        return new EndStreamException();
    }

    static Inbound fromGatewayInbound(GatewayOuterClass.Inbound inbound) {
        return new Inbound(
                inbound.getTopic(),
                inbound.getPartition(),
                inbound.getOffset(),
                inbound.getValue().toByteArray(),
                inbound.getHeadersList().stream().map(LiveGatewaySubscription::fromGatewayHeader).toList());
    }

    private static Header fromGatewayHeader(GatewayOuterClass.Header header) {
        byte[] value = header.hasValue() ? header.getValue().toByteArray() : null;
        return new Header(header.getKey(), value);
    }

    private record StreamItem(Inbound inbound, CrabkaException error, boolean end) {}

    static final class EndStreamException extends EOFException {
        private EndStreamException() {
            super("Subscribe stream ended");
        }
    }
}
