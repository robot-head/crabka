package dev.crabka.sdk;

import crabka.gateway.v1.GatewayOuterClass;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.Headers;
import okhttp3.internal.concurrent.TaskRunner;
import okhttp3.internal.http2.ErrorCode;
import okhttp3.internal.http2.Http2Connection;
import okhttp3.internal.http2.Http2Stream;
import okio.Buffer;
import okio.BufferedSource;
import okio.Okio;

final class H2LiveGatewaySubscription implements LiveSubscription {
    private static final StreamItem END = new StreamItem(null, null, true);
    private static final int CONNECT_TIMEOUT_MILLIS = 10_000;

    private final URI uri;
    private final String bearerToken;
    private final byte[] startFrame;
    private final StreamOpener streamOpener;
    private final BlockingQueue<StreamItem> inbox = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object closeLock = new Object();
    private final List<ResourceCloser> resourceClosers = new ArrayList<>();
    private final ResourceRegistry resources = new ResourceRegistry() {
        @Override
        public void register(ResourceCloser closeResource) throws IOException {
            registerResource(closeResource);
        }
    };

    H2LiveGatewaySubscription(URI uri, String bearerToken, byte[] startFrame) {
        this(uri, bearerToken, startFrame, new Http2StreamOpener());
    }

    H2LiveGatewaySubscription(URI uri, String bearerToken, byte[] startFrame, StreamOpener streamOpener) {
        this.uri = uri;
        this.bearerToken = bearerToken;
        this.startFrame = startFrame.clone();
        this.streamOpener = Objects.requireNonNull(streamOpener, "streamOpener");
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
        closeResources();
        inbox.offer(END);
    }

    private void start() {
        Thread reader = new Thread(this::readFromGateway, "crabka-live-subscribe");
        reader.setDaemon(true);
        reader.start();
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

    private void readFromGateway() {
        if (closed.get()) {
            return;
        }
        try {
            GatewayStream openedStream = streamOpener.open(uri, bearerToken, resources);
            throwIfClosed();
            openedStream.writeStartFrame(startFrame);
            throwIfClosed();
            ensureSuccessfulStatus(openedStream.takeStatus());
            throwIfClosed();
            readInboundFrames(openedStream);
            sendEnd();
        } catch (EOFException done) {
            sendEnd();
        } catch (CrabkaException error) {
            sendError(error);
        } catch (IOException error) {
            if (!closed.get()) {
                sendError(new TransportException("Subscribe stream read failed: " + error.getMessage(), error));
            }
        } finally {
            closeResources();
        }
    }

    private void throwIfClosed() throws EOFException {
        if (closed.get()) {
            throw new EOFException("Subscribe stream closed");
        }
    }

    private void registerResource(ResourceCloser closeResource) throws IOException {
        Objects.requireNonNull(closeResource, "closeResource");
        synchronized (closeLock) {
            if (!closed.get()) {
                resourceClosers.add(closeResource);
                return;
            }
        }

        EOFException closedDuringStartup = new EOFException("Subscribe stream closed during startup");
        try {
            closeResource.close();
        } catch (IOException error) {
            closedDuringStartup.addSuppressed(error);
        }
        throw closedDuringStartup;
    }

    private void closeResources() {
        List<ResourceCloser> closers;
        synchronized (closeLock) {
            if (resourceClosers.isEmpty()) {
                return;
            }
            closers = new ArrayList<>(resourceClosers);
            resourceClosers.clear();
        }
        for (int index = closers.size() - 1; index >= 0; index--) {
            try {
                closers.get(index).close();
            } catch (IOException ignored) {
            }
        }
    }

    private static GatewayStream openHttp2Stream(URI uri, String bearerToken, ResourceRegistry resources)
            throws IOException {
        if (!"http".equals(uri.getScheme())) {
            throw new TransportException("live h2c subscribe requires an http endpoint");
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new TransportException("live h2c subscribe endpoint is missing a host");
        }
        int port = uri.getPort() == -1 ? 80 : uri.getPort();
        String authority = port == 80 ? host : host + ":" + port;

        Socket openedSocket = new Socket();
        openedSocket.setTcpNoDelay(true);
        resources.register(openedSocket::close);
        openedSocket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);

        Http2Connection openedConnection = new Http2Connection.Builder(true, TaskRunner.INSTANCE)
                .socket(openedSocket, authority)
                .listener(Http2Connection.Listener.REFUSE_INCOMING_STREAMS)
                .build();
        resources.register(openedConnection::close);
        openedConnection.start();

        Http2Stream openedStream = openedConnection.newStream(requestHeaders(uri, bearerToken, authority), true);
        resources.register(() -> openedStream.close(ErrorCode.CANCEL, null));
        return new Http2GatewayStream(openedStream);
    }

    private static List<okhttp3.internal.http2.Header> requestHeaders(URI uri, String bearerToken, String authority) {
        List<okhttp3.internal.http2.Header> headers = new ArrayList<>();
        headers.add(new okhttp3.internal.http2.Header(okhttp3.internal.http2.Header.TARGET_METHOD, "POST"));
        headers.add(new okhttp3.internal.http2.Header(okhttp3.internal.http2.Header.TARGET_SCHEME, "http"));
        headers.add(new okhttp3.internal.http2.Header(okhttp3.internal.http2.Header.TARGET_AUTHORITY, authority));
        headers.add(new okhttp3.internal.http2.Header(okhttp3.internal.http2.Header.TARGET_PATH, requestPath(uri)));
        headers.add(new okhttp3.internal.http2.Header("content-type", "application/connect+proto"));
        headers.add(new okhttp3.internal.http2.Header("accept", "application/connect+proto"));
        headers.add(new okhttp3.internal.http2.Header("connect-protocol-version", "1"));
        if (!bearerToken.isBlank()) {
            headers.add(new okhttp3.internal.http2.Header("authorization", "Bearer " + bearerToken));
        }
        return List.copyOf(headers);
    }

    private static String requestPath(URI uri) {
        String rawPath = uri.getRawPath();
        String path = rawPath == null || rawPath.isBlank() ? "/" : rawPath;
        String query = uri.getRawQuery();
        if (query == null || query.isBlank()) {
            return path;
        }
        return path + "?" + query;
    }

    private void ensureSuccessfulStatus(String statusText) {
        if (statusText == null) {
            throw new TransportException("Subscribe response did not include an HTTP status");
        }
        int status = parseStatus(statusText);
        if (status < 200 || status >= 300) {
            throw LiveGatewayTransport.errorForStatus(status, "HTTP " + status);
        }
    }

    private static int parseStatus(String statusText) {
        try {
            return Integer.parseInt(statusText);
        } catch (NumberFormatException error) {
            throw new TransportException("Subscribe response returned invalid HTTP status " + statusText, error);
        }
    }

    private void readInboundFrames(GatewayStream openedStream) throws IOException {
        while (!closed.get()) {
            sendInbound(openedStream.readNextInbound());
        }
    }

    private static Inbound readNextInboundFrame(BufferedSource input) throws IOException {
        while (true) {
            int flags = input.readByte() & 0xff;
            int length = input.readInt();
            if ((flags & 0x01) != 0) {
                throw new TransportException("Subscribe stream returned a compressed frame");
            }
            if (length < 0) {
                throw new TransportException("Subscribe stream returned a negative frame length");
            }
            byte[] payload = input.readByteArray(length);
            if (flags == 0x00) {
                return LiveGatewaySubscription.fromGatewayInbound(GatewayOuterClass.Inbound.parseFrom(payload));
            }
            if (flags == 0x02) {
                throw LiveGatewaySubscription.endStream(payload);
            }
            throw new TransportException("Subscribe stream returned unknown frame flags " + flags);
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

    interface StreamOpener {
        GatewayStream open(URI uri, String bearerToken, ResourceRegistry resources) throws IOException;
    }

    interface ResourceRegistry {
        void register(ResourceCloser closeResource) throws IOException;
    }

    @FunctionalInterface
    interface ResourceCloser {
        void close() throws IOException;
    }

    interface GatewayStream {
        void writeStartFrame(byte[] startFrame) throws IOException;

        String takeStatus() throws IOException;

        Inbound readNextInbound() throws IOException;
    }

    private static final class Http2StreamOpener implements StreamOpener {
        @Override
        public GatewayStream open(URI uri, String bearerToken, ResourceRegistry resources) throws IOException {
            return openHttp2Stream(uri, bearerToken, resources);
        }
    }

    private static final class Http2GatewayStream implements GatewayStream {
        private final Http2Stream stream;
        private final BufferedSource input;

        private Http2GatewayStream(Http2Stream stream) {
            this.stream = stream;
            input = Okio.buffer(stream.getSource());
        }

        @Override
        public void writeStartFrame(byte[] startFrame) throws IOException {
            Buffer frame = new Buffer().write(startFrame);
            stream.getConnection().writeData(stream.getId(), false, frame, startFrame.length);
            stream.getConnection().flush();
        }

        @Override
        public String takeStatus() throws IOException {
            Headers responseHeaders = stream.takeHeaders(false);
            return responseHeaders.get(okhttp3.internal.http2.Header.RESPONSE_STATUS_UTF8);
        }

        @Override
        public Inbound readNextInbound() throws IOException {
            return readNextInboundFrame(input);
        }
    }

    private record StreamItem(Inbound inbound, CrabkaException error, boolean end) {}
}
