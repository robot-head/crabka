package dev.crabka.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import crabka.gateway.v1.GatewayOuterClass;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.junit.jupiter.api.Test;

final class MessagingTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final MediaType CONNECT_PROTO = MediaType.get("application/proto");
    private static final MediaType CONNECT_STREAM_PROTO = MediaType.get("application/connect+proto");

    @Test
    void cloudEventHeadersUseBinaryModeNames() {
        CloudEvent event = new CloudEvent(
                "evt-1",
                "/orders",
                "order.created",
                "1.0",
                Optional.of("application/json"),
                "{}".getBytes(StandardCharsets.UTF_8));

        List<Header> headers = Messaging.cloudEventHeaders(event);

        assertEquals(List.of("ce_id", "ce_source", "ce_type", "ce_specversion", "content-type"),
                headers.stream().map(Header::name).toList());
        assertFalse(headers.stream().anyMatch(header -> header.name().equals("ce_datacontenttype")));
        assertArrayEquals("application/json".getBytes(StandardCharsets.UTF_8), headers.get(4).value());
    }

    @Test
    void mockPublishSubscribeRoundTrips() {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();

        RecordResult result = client.messaging().publish(Record.of("roundtrip", "hello".getBytes(StandardCharsets.UTF_8))).join();
        MessageStream stream = client.messaging().subscribe(List.of("roundtrip"), "group", Optional.empty());

        assertEquals(new RecordResult(0, 0, false), result);
        assertTrue(stream.hasNext());
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), stream.next().value());
        assertFalse(stream.hasNext());
    }

    @Test
    void mockFilterDeliversMatchesOnly() throws Exception {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();
        client.messaging().publish(Record.of("events", "{\"kind\":\"skip\"}".getBytes(StandardCharsets.UTF_8))).join();
        client.messaging().publish(Record.of("events", "{\"kind\":\"take\"}".getBytes(StandardCharsets.UTF_8))).join();
        Filter filter = new Filter("$.kind", FilterOp.EQUALS, JSON.readTree("\"take\""));

        MessageStream stream = client.messaging().subscribe(List.of("events"), "group", Optional.of(filter));

        assertTrue(stream.hasNext());
        assertArrayEquals("{\"kind\":\"take\"}".getBytes(StandardCharsets.UTF_8), stream.next().value());
        assertFalse(stream.hasNext());
    }

    @Test
    void mockSubscribeRejectsMalformedFilterPath() throws Exception {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();
        Filter filter = new Filter("kind", FilterOp.EQUALS, JSON.readTree("\"take\""));

        InvalidArgumentException error = assertThrows(
                InvalidArgumentException.class,
                () -> client.messaging().subscribe(List.of("events"), "group", Optional.of(filter)));

        assertEquals("filter path must start with $.", error.getMessage());
    }

    @Test
    void liveSubscribeReadsGatewayStreamInsteadOfLocalPublish() throws Exception {
        byte[] localValue = "local-only".getBytes(StandardCharsets.UTF_8);
        byte[] gatewayValue = "from-gateway".getBytes(StandardCharsets.UTF_8);
        Filter filter = new Filter("$.kind", FilterOp.EQUALS, JSON.readTree("\"from-gateway\""));

        CopyOnWriteArrayList<Request> requests = new CopyOnWriteArrayList<>();
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request request = chain.request();
                    requests.add(request);
                    return switch (request.url().encodedPath()) {
                        case "/crabka.gateway.v1.Gateway/Send" -> gatewayResponse(
                                request, CONNECT_PROTO, gatewaySendResponse(0));
                        case "/crabka.gateway.v1.Gateway/Subscribe" -> gatewayResponse(
                                request, CONNECT_STREAM_PROTO, gatewaySubscribeResponse("live", gatewayValue));
                        default -> throw new AssertionError("unexpected gateway path " + request.url().encodedPath());
                    };
                })
                .build();
        LiveGatewayTransport transport = new LiveGatewayTransport(URI.create("http://gateway.test"), "", httpClient);

        RecordResult result = transport.send(Record.of("live", localValue));
        try (MessageStream stream = transport.subscribe(List.of("live"), "live-group", Optional.of(filter))) {
            Inbound inbound = stream.nextWithin(Duration.ofSeconds(1));
            assertNotNull(inbound);
            assertArrayEquals(gatewayValue, inbound.value());
        }

        assertEquals(new RecordResult(0, 0, false), result);
        assertEquals(List.of("/crabka.gateway.v1.Gateway/Send", "/crabka.gateway.v1.Gateway/Subscribe"),
                requests.stream().map(request -> request.url().encodedPath()).toList());

        Request subscribe = requests.get(1);
        assertEquals("application/connect+proto", subscribe.header("Content-Type"));
        assertEquals("application/connect+proto", subscribe.header("Accept"));
        assertEquals("1", subscribe.header("connect-protocol-version"));

        GatewayOuterClass.SubscribeStart start = readSubscribeStart(subscribe);
        assertEquals("live-group", start.getGroupId());
        assertEquals(List.of("live"), start.getTopicsList());
        assertEquals("kind = 'from-gateway'", start.getFilter());
        assertTrue(start.getAutoCommit());
    }

    @Test
    void liveSubscribeUsesSqlStandardStringLiteralEscaping() throws Exception {
        Filter filter = new Filter("$.path", FilterOp.EQUALS, JSON.readTree("\"C:\\\\tmp\\\\O'Brien\""));
        CopyOnWriteArrayList<Request> requests = new CopyOnWriteArrayList<>();
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    requests.add(chain.request());
                    return gatewayResponse(
                            chain.request(), CONNECT_STREAM_PROTO,
                            gatewaySubscribeResponse("live", new byte[0]));
                })
                .build();
        LiveGatewayTransport transport = new LiveGatewayTransport(URI.create("http://gateway.test"), "", httpClient);

        try (MessageStream stream = transport.subscribe(List.of("live"), "live-group", Optional.of(filter))) {
            assertNotNull(stream.nextWithin(Duration.ofSeconds(1)));
            assertEquals("path = 'C:\\tmp\\O''Brien'", readSubscribeStart(requests.getFirst()).getFilter());
        }
    }

    @Test
    void liveSendMapsInvalidArgumentErrorInfo() {
        InvalidArgumentException error = assertSendError(
                InvalidArgumentException.class,
                gatewayError(3, "topic is required", false));

        assertEquals("topic is required", error.getMessage());
    }

    @Test
    void liveSendMapsRetriableErrorInfoToTransport() {
        TransportException error = assertSendError(
                TransportException.class,
                gatewayError(13, "owner unavailable", true));

        assertEquals("owner unavailable", error.getMessage());
    }

    @Test
    void liveSendMapsUnavailableErrorInfoToTransport() {
        TransportException error = assertSendError(
                TransportException.class,
                gatewayError(14, "gateway unavailable", false));

        assertEquals("gateway unavailable", error.getMessage());
    }

    @Test
    void unaryConnectErrorsPreserveCodeAndMessage() {
        CrabkaException error = LiveGatewayTransport.errorForResponse(errorResponse(
                400, "{\"code\":\"invalid_argument\",\"message\":\"queue session expired; re-acquire\"}"));

        assertTrue(error instanceof InvalidArgumentException);
        assertEquals("queue session expired; re-acquire", error.getMessage());
    }

    @Test
    void unaryPermissionDeniedMapsToUnauthenticated() {
        CrabkaException error = LiveGatewayTransport.errorForResponse(errorResponse(
                403, "{\"code\":\"permission_denied\",\"message\":\"denied\"}"));

        assertTrue(error instanceof UnauthenticatedException);
        assertEquals("denied", error.getMessage());
    }

    @Test
    void unaryMalformedErrorFallsBackToHttpTaxonomy() {
        CrabkaException error = LiveGatewayTransport.errorForResponse(errorResponse(500, "broken"));

        assertTrue(error instanceof ServerException);
        assertEquals("broken", error.getMessage());
    }

    private static <T extends CrabkaException> T assertSendError(
            Class<T> expectedType,
            GatewayOuterClass.ErrorInfo errorInfo) {
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addInterceptor(chain -> gatewayResponse(
                        chain.request(), CONNECT_PROTO, gatewaySendErrorResponse(errorInfo)))
                .build();
        LiveGatewayTransport transport = new LiveGatewayTransport(URI.create("http://gateway.test"), "", httpClient);

        return assertThrows(expectedType, () -> transport.send(Record.of("live", bytes("payload"))));
    }

    private static byte[] gatewaySendResponse(long offset) {
        GatewayOuterClass.RecordResult result = GatewayOuterClass.RecordResult.newBuilder()
                .setPartition(0)
                .setOffset(offset)
                .setDeduplicated(false)
                .build();
        GatewayOuterClass.SendResponse response = GatewayOuterClass.SendResponse.newBuilder()
                .addResults(result)
                .build();
        return response.toByteArray();
    }

    private static byte[] gatewaySendErrorResponse(GatewayOuterClass.ErrorInfo errorInfo) {
        GatewayOuterClass.RecordResult result = GatewayOuterClass.RecordResult.newBuilder()
                .setError(errorInfo)
                .build();
        return GatewayOuterClass.SendResponse.newBuilder()
                .addResults(result)
                .build()
                .toByteArray();
    }

    private static GatewayOuterClass.ErrorInfo gatewayError(int code, String message, boolean retriable) {
        return GatewayOuterClass.ErrorInfo.newBuilder()
                .setCode(code)
                .setMessage(message)
                .setRetriable(retriable)
                .build();
    }

    private static byte[] gatewaySubscribeResponse(String topic, byte[] value) {
        GatewayOuterClass.Inbound inbound = GatewayOuterClass.Inbound.newBuilder()
                .setTopic(topic)
                .setPartition(0)
                .setOffset(7)
                .setValue(ByteString.copyFrom(value))
                .build();
        return connectFrame(inbound);
    }

    private static Response gatewayResponse(Request request, MediaType contentType, byte[] body) {
        return new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_2)
                .code(200)
                .message("OK")
                .header("Content-Type", contentType.toString())
                .body(ResponseBody.create(body, contentType))
                .build();
    }

    private static Response errorResponse(int code, String body) {
        Request request = new Request.Builder().url("http://gateway.test/error").build();
        return new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_2)
                .code(code)
                .message("error")
                .body(ResponseBody.create(body, MediaType.get("application/json")))
                .build();
    }

    private static GatewayOuterClass.SubscribeStart readSubscribeStart(Request request) throws Exception {
        assertTrue(request.body() instanceof ConnectFrameRequestBody);
        ConnectFrameRequestBody body = (ConnectFrameRequestBody) request.body();
        GatewayOuterClass.SubscribeFrame frame = GatewayOuterClass.SubscribeFrame.parseFrom(readConnectFrame(body.encodedFrame()));
        assertTrue(frame.hasStart());
        return frame.getStart();
    }

    private static byte[] connectFrame(Message message) {
        byte[] payload = message.toByteArray();
        return new Buffer()
                .writeByte(0)
                .writeInt(payload.length)
                .write(payload)
                .readByteArray();
    }

    private static byte[] readConnectFrame(byte[] framedBytes) throws Exception {
        Buffer body = new Buffer().write(framedBytes);
        int flags = body.readByte() & 0xff;
        int length = body.readInt();
        byte[] payload = body.readByteArray(length);
        assertEquals(0, flags);
        return payload;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
