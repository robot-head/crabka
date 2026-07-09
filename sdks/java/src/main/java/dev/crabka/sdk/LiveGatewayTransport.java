package dev.crabka.sdk;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.fasterxml.jackson.databind.JsonNode;
import crabka.gateway.v1.GatewayOuterClass;
import dev.crabka.sdk.internal.GatewayCore;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

final class LiveGatewayTransport {
    private static final MediaType CONNECT_PROTO = MediaType.get("application/proto");
    private static final MediaType CONNECT_STREAM_PROTO = MediaType.get("application/connect+proto");
    private static final String GATEWAY_SERVICE_PATH = "/crabka.gateway.v1.Gateway/";

    private final URI endpoint;
    private final String bearerToken;
    private final OkHttpClient httpClient;
    private final boolean streamWithOkHttp;
    private final GatewayCore gatewayCore;

    LiveGatewayTransport(URI endpoint, String bearerToken) {
        this(endpoint, bearerToken, new OkHttpClient.Builder()
                .protocols(protocolsFor(endpoint.getScheme()))
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .build(), false);
    }

    LiveGatewayTransport(URI endpoint, String bearerToken, OkHttpClient httpClient) {
        this(endpoint, bearerToken, httpClient, true);
    }

    private LiveGatewayTransport(URI endpoint, String bearerToken, OkHttpClient httpClient, boolean streamWithOkHttp) {
        String scheme = endpoint.getScheme();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new InvalidArgumentException("live endpoint must use http or https");
        }
        this.endpoint = endpoint;
        this.bearerToken = bearerToken;
        this.httpClient = httpClient;
        this.streamWithOkHttp = streamWithOkHttp;
        this.gatewayCore = GatewayCore.withoutGeneratedClient();
    }

    RecordResult send(Record record) {
        GatewayOuterClass.SendRequest request = gatewayCore.toSendRequest(record);
        GatewayOuterClass.SendResponse response = executeUnary("Send", request, GatewayOuterClass.SendResponse.parser());
        if (response.getResultsCount() == 0) {
            throw new TransportException("Send response did not include a record result");
        }
        return fromGatewayRecordResult(response.getResults(0));
    }

    QueueAcquireResult queueAcquire(String topic, String group, int max, long lockDurationMs) {
        GatewayOuterClass.QueueAcquireRequest request = GatewayOuterClass.QueueAcquireRequest.newBuilder()
                .setGroupId(group)
                .addTopics(topic)
                .setMaxMessages(max)
                .setWaitMs(0)
                .setLockDurationMs(lockDurationMs)
                .build();
        GatewayOuterClass.QueueAcquireResponse response = executeUnary(
                "QueueAcquire", request, GatewayOuterClass.QueueAcquireResponse.parser());
        return new QueueAcquireResult(
                response.getSessionId(),
                response.getMessagesList().stream().map(LiveGatewayTransport::fromGatewayQueueMessage).toList());
    }

    QueueBatchResult queueAcknowledge(String sessionId, List<QueueAckEntry> entries) {
        GatewayOuterClass.QueueAcknowledgeRequest request = GatewayOuterClass.QueueAcknowledgeRequest.newBuilder()
                .setSessionId(sessionId)
                .addAllEntries(entries.stream().map(LiveGatewayTransport::toGatewayAckEntry).toList())
                .build();
        GatewayOuterClass.QueueAcknowledgeResponse response = executeUnary(
                "QueueAcknowledge", request, GatewayOuterClass.QueueAcknowledgeResponse.parser());
        return fromGatewayBatchResult(entries, response.getResultsList());
    }

    QueueBatchResult queueRenew(String sessionId, List<QueueRenewEntry> entries) {
        GatewayOuterClass.QueueRenewRequest request = GatewayOuterClass.QueueRenewRequest.newBuilder()
                .setSessionId(sessionId)
                .addAllEntries(entries.stream().map(LiveGatewayTransport::toGatewayRenewEntry).toList())
                .build();
        GatewayOuterClass.QueueRenewResponse response = executeUnary(
                "QueueRenew", request, GatewayOuterClass.QueueRenewResponse.parser());
        return fromGatewayBatchResult(entries, response.getResultsList());
    }

    MessageStream subscribe(List<String> topics, String group, Optional<Filter> filter) {
        GatewayOuterClass.SubscribeFrame frame = gatewayCore.toSubscribeFrame(
                topics, group, filter.map(this::toServerFilter).orElse(""));
        ConnectFrameRequestBody requestBody = new ConnectFrameRequestBody(frame);
        Request request = requestBuilder("Subscribe")
                .header("Content-Type", CONNECT_STREAM_PROTO.toString())
                .header("Accept", CONNECT_STREAM_PROTO.toString())
                .post(requestBody)
                .build();
        if (!streamWithOkHttp && "http".equals(endpoint.getScheme())) {
            URI subscribeUri = endpoint.resolve(GATEWAY_SERVICE_PATH + "Subscribe");
            return new MessageStream(new H2LiveGatewaySubscription(subscribeUri, bearerToken, requestBody.encodedFrame()));
        }
        Call call = httpClient.newCall(request);
        return new MessageStream(new LiveGatewaySubscription(call, requestBody));
    }

    private <T extends Message> T executeUnary(String method, Message message, Parser<T> parser) {
        Request request = requestBuilder(method)
                .header("Content-Type", CONNECT_PROTO.toString())
                .header("Accept", CONNECT_PROTO.toString())
                .post(RequestBody.create(message.toByteArray(), CONNECT_PROTO))
                .build();
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw errorForResponse(response);
            }
            ResponseBody body = response.body();
            if (body == null) {
                throw new TransportException(method + " response did not include a body");
            }
            return parser.parseFrom(body.bytes());
        } catch (InvalidProtocolBufferException error) {
            throw new TransportException(method + " response was not valid protobuf", error);
        } catch (IOException error) {
            throw new TransportException(method + " request failed", error);
        }
    }

    private Request.Builder requestBuilder(String method) {
        Request.Builder builder = new Request.Builder().url(endpoint.resolve(GATEWAY_SERVICE_PATH + method).toString());
        builder.header("connect-protocol-version", "1");
        if (!bearerToken.isBlank()) {
            builder.header("Authorization", "Bearer " + bearerToken);
        }
        return builder;
    }

    private static Header fromGatewayHeader(GatewayOuterClass.Header header) {
        byte[] value = header.hasValue() ? header.getValue().toByteArray() : null;
        return new Header(header.getKey(), value);
    }

    private static QueueMessage fromGatewayQueueMessage(GatewayOuterClass.QueuedMessage message) {
        return new QueueMessage(
                messageId(message.getTopic(), message.getPartition(), message.getOffset()),
                message.getTopic(),
                message.getPartition(),
                message.getOffset(),
                message.getValue().toByteArray(),
                message.getHeadersList().stream().map(LiveGatewayTransport::fromGatewayHeader).toList(),
                message.getDeliveryCount());
    }

    private static GatewayOuterClass.QueueAckEntry toGatewayAckEntry(QueueAckEntry entry) {
        ParsedMessageId messageId = parseMessageId(entry.messageId());
        return GatewayOuterClass.QueueAckEntry.newBuilder()
                .setTopic(messageId.topic())
                .setPartition(messageId.partition())
                .setOffset(messageId.offset())
                .setType(toGatewayAckType(entry.ackType()))
                .build();
    }

    private static GatewayOuterClass.QueueAckEntry toGatewayRenewEntry(QueueRenewEntry entry) {
        ParsedMessageId messageId = parseMessageId(entry.messageId());
        return GatewayOuterClass.QueueAckEntry.newBuilder()
                .setTopic(messageId.topic())
                .setPartition(messageId.partition())
                .setOffset(messageId.offset())
                .setType(GatewayOuterClass.QueueAckType.ACCEPT)
                .build();
    }

    private static GatewayOuterClass.QueueAckType toGatewayAckType(QueueAckType ackType) {
        return switch (ackType) {
            case ACCEPT -> GatewayOuterClass.QueueAckType.ACCEPT;
            case RELEASE -> GatewayOuterClass.QueueAckType.RELEASE;
            case REJECT -> GatewayOuterClass.QueueAckType.REJECT;
        };
    }

    private static QueueBatchResult fromGatewayBatchResult(
            List<? extends Object> entries,
            List<GatewayOuterClass.QueueAckResult> results) {
        List<QueueResult> queueResults = new ArrayList<>();
        for (int index = 0; index < results.size(); index++) {
            String messageId = messageIdAt(entries, index);
            if (results.get(index).hasError()) {
                queueResults.add(QueueResult.notAcquired(messageId));
                continue;
            }
            queueResults.add(QueueResult.success(messageId));
        }
        return new QueueBatchResult(queueResults);
    }

    private static String messageIdAt(List<? extends Object> entries, int index) {
        if (index >= entries.size()) {
            return "";
        }
        Object entry = entries.get(index);
        if (entry instanceof QueueAckEntry ackEntry) {
            return ackEntry.messageId();
        }
        if (entry instanceof QueueRenewEntry renewEntry) {
            return renewEntry.messageId();
        }
        throw new InvalidArgumentException("unsupported queue entry type");
    }

    private static ParsedMessageId parseMessageId(String messageId) {
        String[] parts = messageId.split(":", -1);
        if (parts.length != 3) {
            throw new InvalidArgumentException("queue message_id must be <topic>:<partition>:<offset>");
        }
        try {
            int partition = Integer.parseInt(parts[1]);
            long offset = Long.parseLong(parts[2]);
            if (parts[0].isBlank()) {
                throw new NumberFormatException("topic is blank");
            }
            return new ParsedMessageId(parts[0], partition, offset);
        } catch (NumberFormatException error) {
            throw new InvalidArgumentException("queue message_id must be <topic>:<partition>:<offset>");
        }
    }

    private static String messageId(String topic, int partition, long offset) {
        return topic + ":" + partition + ":" + offset;
    }

    private static RecordResult fromGatewayRecordResult(GatewayOuterClass.RecordResult result) {
        if (result.hasError()) {
            throw new ServerException(result.getError().getMessage());
        }
        return new RecordResult(result.getPartition(), result.getOffset(), result.getDeduplicated());
    }

    static CrabkaException errorForResponse(Response response) {
        String message = response.message().isBlank() ? "HTTP " + response.code() : response.message();
        response.close();
        return errorForStatus(response.code(), message);
    }

    static CrabkaException errorForStatus(int status, String message) {
        return switch (status) {
            case 400 -> new InvalidArgumentException(message);
            case 401 -> new UnauthenticatedException(message);
            case 404 -> new NotFoundException(message);
            default -> new TransportException(message);
        };
    }

    private String toServerFilter(Filter filter) {
        if (filter.op() != FilterOp.EQUALS) {
            throw new InvalidArgumentException("only equals filters are supported by live subscribe");
        }
        return serverFieldPath(filter.path()) + " = " + serverLiteral(filter.value());
    }

    private static String serverFieldPath(String path) {
        if (!path.startsWith("$.")) {
            throw new InvalidArgumentException("filter path must start with $. for live subscribe");
        }
        String fieldPath = path.substring(2);
        if (fieldPath.isBlank()) {
            throw new InvalidArgumentException("filter path must include a field name");
        }
        for (String segment : fieldPath.split("\\.", -1)) {
            if (!isIdentifier(segment)) {
                throw new InvalidArgumentException("unsupported filter path segment " + segment);
            }
        }
        return fieldPath;
    }

    private static String serverLiteral(JsonNode value) {
        if (value.isTextual()) {
            return "'" + escapeStringLiteral(value.textValue()) + "'";
        }
        if (value.isNumber()) {
            return value.asText();
        }
        if (value.isBoolean()) {
            return value.asBoolean() ? "true" : "false";
        }
        if (value.isNull()) {
            return "null";
        }
        throw new InvalidArgumentException("filter value must be a string, number, boolean, or null");
    }

    private static String escapeStringLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    private static boolean isIdentifier(String value) {
        if (value.isEmpty()) {
            return false;
        }
        char first = value.charAt(0);
        if (first != '_' && !isAsciiLetter(first)) {
            return false;
        }
        return value.chars().skip(1).allMatch(character -> character == '_' || isAsciiLetterOrDigit(character));
    }

    private static boolean isAsciiLetter(int character) {
        return (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z');
    }

    private static boolean isAsciiLetterOrDigit(int character) {
        return isAsciiLetter(character) || (character >= '0' && character <= '9');
    }

    private static List<Protocol> protocolsFor(String scheme) {
        if ("http".equals(scheme)) {
            return List.of(Protocol.H2_PRIOR_KNOWLEDGE);
        }
        return List.of(Protocol.HTTP_2, Protocol.HTTP_1_1);
    }

    private record ParsedMessageId(String topic, int partition, long offset) {}
}
