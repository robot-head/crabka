package dev.crabka.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import crabka.gateway.v1.GatewayOuterClass;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.junit.jupiter.api.Test;

final class QueuesTest {
    private static final MediaType CONNECT_PROTO = MediaType.get("application/proto");

    @Test
    void mockAcquireAcknowledgeRenewAndReleaseUseQueueShapes() {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();
        client.messaging().publish(new Record("queue", bytes("first"), List.of(new Header("kind", bytes("queue"))))).join();
        client.messaging().publish(Record.of("queue", bytes("second"))).join();

        QueueAcquireResult acquired = client.queues().acquire("queue", "workers", 1, Duration.ofSeconds(30)).join();

        assertEquals("queue-session-1", acquired.sessionId());
        assertEquals(1, acquired.messages().size());
        QueueMessage message = acquired.messages().get(0);
        assertEquals("queue:0:0", message.messageId());
        assertEquals("queue", message.topic());
        assertEquals(0, message.partition());
        assertEquals(0, message.offset());
        assertEquals(1, message.deliveryCount());
        assertArrayEquals(bytes("first"), message.value());
        assertEquals(1, message.headers().size());
        assertEquals("kind", message.headers().get(0).name());
        assertArrayEquals(bytes("queue"), message.headers().get(0).value());

        QueueBatchResult renewed = client.queues().renew(acquired.sessionId(), List.of(
                new QueueRenewEntry("queue:0:0"),
                new QueueRenewEntry("missing:0:0"))).join();
        assertQueueResults(renewed.results(), List.of(
                QueueResult.success("queue:0:0"),
                QueueResult.notAcquired("missing:0:0")));

        QueueBatchResult acknowledged = client.queues().acknowledge(acquired.sessionId(), List.of(
                new QueueAckEntry("queue:0:0", QueueAckType.RELEASE))).join();
        assertQueueResults(acknowledged.results(), List.of(QueueResult.success("queue:0:0")));

        QueueAcquireResult redelivered = client.queues().acquire("queue", "workers", 1, Duration.ofSeconds(30)).join();
        assertEquals("queue-session-2", redelivered.sessionId());
        assertEquals("queue:0:0", redelivered.messages().get(0).messageId());
        assertEquals(2, redelivered.messages().get(0).deliveryCount());
    }

    @Test
    void validationErrorsUseConformanceMessages() {
        CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();

        assertInvalidArgument("queue group is required",
                () -> client.queues().acquire("queue", "", 1, Duration.ofSeconds(30)).join());
        assertInvalidArgument("queue lock_duration_ms must be 30000; per-acquire lock durations are not supported",
                () -> client.queues().acquire("queue", "workers", 1, Duration.ofSeconds(1)).join());
        assertInvalidArgument("queue session_id is required",
                () -> client.queues().renew("", List.of()).join());
    }

    @Test
    void liveQueueRpcRequestsAndResponsesAreMapped() throws Exception {
        CopyOnWriteArrayList<Request> requests = new CopyOnWriteArrayList<>();
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request request = chain.request();
                    requests.add(request);
                    return switch (request.url().encodedPath()) {
                        case "/crabka.gateway.v1.Gateway/QueueAcquire" -> gatewayResponse(queueAcquireResponse());
                        case "/crabka.gateway.v1.Gateway/QueueAcknowledge" -> gatewayResponse(queueBatchResponse(false));
                        case "/crabka.gateway.v1.Gateway/QueueRenew" -> gatewayResponse(queueBatchResponse(true));
                        default -> throw new AssertionError("unexpected gateway path " + request.url().encodedPath());
                    };
                })
                .build();
        LiveGatewayTransport transport = new LiveGatewayTransport(URI.create("http://gateway.test"), "", httpClient);

        QueueAcquireResult acquired = transport.queueAcquire("queue", "workers", 1, 30_000);
        QueueBatchResult acknowledged = transport.queueAcknowledge("actual-session", List.of(
                new QueueAckEntry("queue:0:7", QueueAckType.ACCEPT)));
        QueueBatchResult renewed = transport.queueRenew("actual-session", List.of(
                new QueueRenewEntry("queue:0:7")));

        assertEquals("actual-session", acquired.sessionId());
        assertEquals("queue:0:7", acquired.messages().get(0).messageId());
        assertArrayEquals(bytes("live"), acquired.messages().get(0).value());
        assertQueueResults(acknowledged.results(), List.of(QueueResult.success("queue:0:7")));
        assertQueueResults(renewed.results(), List.of(QueueResult.notAcquired("queue:0:7")));
        assertEquals(List.of(
                        "/crabka.gateway.v1.Gateway/QueueAcquire",
                        "/crabka.gateway.v1.Gateway/QueueAcknowledge",
                        "/crabka.gateway.v1.Gateway/QueueRenew"),
                requests.stream().map(request -> request.url().encodedPath()).toList());

        GatewayOuterClass.QueueAcquireRequest acquireRequest = GatewayOuterClass.QueueAcquireRequest.parseFrom(bodyBytes(requests.get(0)));
        assertEquals("workers", acquireRequest.getGroupId());
        assertEquals(List.of("queue"), acquireRequest.getTopicsList());
        assertEquals(1, acquireRequest.getMaxMessages());
        assertEquals(30_000, acquireRequest.getLockDurationMs());

        GatewayOuterClass.QueueAcknowledgeRequest ackRequest = GatewayOuterClass.QueueAcknowledgeRequest.parseFrom(bodyBytes(requests.get(1)));
        assertEquals("actual-session", ackRequest.getSessionId());
        assertEquals(GatewayOuterClass.QueueAckType.ACCEPT, ackRequest.getEntries(0).getType());

        GatewayOuterClass.QueueRenewRequest renewRequest = GatewayOuterClass.QueueRenewRequest.parseFrom(bodyBytes(requests.get(2)));
        assertEquals("actual-session", renewRequest.getSessionId());
        assertEquals(GatewayOuterClass.QueueAckType.ACCEPT, renewRequest.getEntries(0).getType());
    }

    private static void assertInvalidArgument(String message, Runnable action) {
        CompletionException error = assertThrows(CompletionException.class, action::run);
        InvalidArgumentException invalidArgument = assertInstanceOf(InvalidArgumentException.class, error.getCause());
        assertEquals(message, invalidArgument.getMessage());
    }

    private static void assertQueueResults(List<QueueResult> actual, List<QueueResult> expected) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).messageId(), actual.get(index).messageId());
            if (expected.get(index).error() == null) {
                assertNull(actual.get(index).error());
                continue;
            }
            assertEquals(expected.get(index).error(), actual.get(index).error());
        }
    }

    private static byte[] queueAcquireResponse() {
        GatewayOuterClass.QueuedMessage message = GatewayOuterClass.QueuedMessage.newBuilder()
                .setTopic("queue")
                .setPartition(0)
                .setOffset(7)
                .setValue(ByteString.copyFrom(bytes("live")))
                .setDeliveryCount(3)
                .build();
        return GatewayOuterClass.QueueAcquireResponse.newBuilder()
                .setSessionId("actual-session")
                .addMessages(message)
                .build()
                .toByteArray();
    }

    private static byte[] queueBatchResponse(boolean withError) {
        GatewayOuterClass.QueueAckResult.Builder result = GatewayOuterClass.QueueAckResult.newBuilder();
        if (withError) {
            result.setError(GatewayOuterClass.ErrorInfo.newBuilder().setMessage("queue message is not acquired"));
        }
        return GatewayOuterClass.QueueAcknowledgeResponse.newBuilder()
                .addResults(result)
                .build()
                .toByteArray();
    }

    private static Response gatewayResponse(byte[] body) {
        return new Response.Builder()
                .request(new Request.Builder().url("http://gateway.test").build())
                .protocol(Protocol.HTTP_2)
                .code(200)
                .message("OK")
                .header("Content-Type", CONNECT_PROTO.toString())
                .body(ResponseBody.create(body, CONNECT_PROTO))
                .build();
    }

    private static byte[] bodyBytes(Request request) throws Exception {
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        return buffer.readByteArray();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
