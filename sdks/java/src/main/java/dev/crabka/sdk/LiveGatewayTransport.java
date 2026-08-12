package dev.crabka.sdk;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final MediaType CONNECT_PROTO = MediaType.get("application/proto");
    private static final MediaType CONNECT_STREAM_PROTO = MediaType.get("application/connect+proto");
    private static final String GATEWAY_SERVICE_PATH = "/crabka.gateway.v1.Gateway/";
    private static final int CONNECT_CODE_CANCELED = 1;
    private static final int CONNECT_CODE_INVALID_ARGUMENT = 3;
    private static final int CONNECT_CODE_DEADLINE_EXCEEDED = 4;
    private static final int CONNECT_CODE_NOT_FOUND = 5;
    private static final int CONNECT_CODE_PERMISSION_DENIED = 7;
    private static final int CONNECT_CODE_FAILED_PRECONDITION = 9;
    private static final int CONNECT_CODE_OUT_OF_RANGE = 11;
    private static final int CONNECT_CODE_UNIMPLEMENTED = 12;
    private static final int CONNECT_CODE_UNAVAILABLE = 14;
    private static final int CONNECT_CODE_UNAUTHENTICATED = 16;
    private static final String ERROR_KIND_INVALID_ARGUMENT = "invalid_argument";
    private static final String ERROR_KIND_NOT_FOUND = "not_found";
    private static final String ERROR_KIND_SERVER_ERROR = "server_error";
    private static final String ERROR_KIND_TRANSPORT = "transport";
    private static final String ERROR_KIND_UNAUTHENTICATED = "unauthenticated";
    private static final String ERROR_KIND_UNIMPLEMENTED = "unimplemented";

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

    QueueAcquireResult queueAcquire(String topic, String group, int max, long lockDurationMs, String sessionId) {
        GatewayOuterClass.QueueAcquireRequest request = GatewayOuterClass.QueueAcquireRequest.newBuilder()
                .setGroupId(group)
                .addTopics(topic)
                .setMaxMessages(max)
                .setWaitMs(0)
                .setSessionId(sessionId)
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
        return fromGatewayBatchResult(response.getResultsList());
    }

    QueueBatchResult queueRenew(String sessionId, List<QueueRenewEntry> entries) {
        GatewayOuterClass.QueueRenewRequest request = GatewayOuterClass.QueueRenewRequest.newBuilder()
                .setSessionId(sessionId)
                .addAllEntries(entries.stream().map(LiveGatewayTransport::toGatewayRenewEntry).toList())
                .build();
        GatewayOuterClass.QueueRenewResponse response = executeUnary(
                "QueueRenew", request, GatewayOuterClass.QueueRenewResponse.parser());
        return fromGatewayBatchResult(response.getResultsList());
    }

    MessageStream subscribe(List<String> topics, String group, Optional<Filter> filter) {
        GatewayOuterClass.SubscribeFrame frame = gatewayCore.toSubscribeFrame(
                topics, group, filter.map(LiveGatewayTransport::toServerFilter).orElse(""));
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
                message.hasValue() ? message.getValue().toByteArray() : null,
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

    private static QueueBatchResult fromGatewayBatchResult(List<GatewayOuterClass.QueueAckResult> results) {
        List<QueueResult> queueResults = new ArrayList<>();
        for (GatewayOuterClass.QueueAckResult result : results) {
            if (!result.hasEntry()) {
                throw new TransportException("queue response result did not include an entry");
            }
            GatewayOuterClass.QueueAckEntry entry = result.getEntry();
            String messageId = messageId(entry.getTopic(), entry.getPartition(), entry.getOffset());
            if (result.hasError()) {
                queueResults.add(new QueueResult(messageId, fromGatewayQueueError(result.getError())));
                continue;
            }
            queueResults.add(QueueResult.success(messageId));
        }
        return new QueueBatchResult(queueResults);
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
            throw exceptionForGatewayError(result.getError());
        }
        return new RecordResult(result.getPartition(), result.getOffset(), result.getDeduplicated());
    }

    private static QueueOperationError fromGatewayQueueError(GatewayOuterClass.ErrorInfo error) {
        GatewayError gatewayError = fromGatewayError(error);
        return new QueueOperationError(gatewayError.kind(), gatewayError.message(), error.getRetriable());
    }

    private static CrabkaException exceptionForGatewayError(GatewayOuterClass.ErrorInfo error) {
        GatewayError gatewayError = fromGatewayError(error);
        return switch (gatewayError.kind()) {
            case ERROR_KIND_INVALID_ARGUMENT -> new InvalidArgumentException(gatewayError.message());
            case ERROR_KIND_NOT_FOUND -> new NotFoundException(gatewayError.message());
            case ERROR_KIND_TRANSPORT -> new TransportException(gatewayError.message());
            case ERROR_KIND_UNAUTHENTICATED -> new UnauthenticatedException(gatewayError.message());
            case ERROR_KIND_UNIMPLEMENTED -> new UnimplementedException(gatewayError.message());
            default -> new ServerException(gatewayError.message());
        };
    }

    private static GatewayError fromGatewayError(GatewayOuterClass.ErrorInfo error) {
        return new GatewayError(kindForGatewayError(error), error.getMessage());
    }

    private static String kindForGatewayError(GatewayOuterClass.ErrorInfo error) {
        if (error.getRetriable()) {
            return ERROR_KIND_TRANSPORT;
        }
        return switch (error.getCode()) {
            case CONNECT_CODE_INVALID_ARGUMENT, CONNECT_CODE_FAILED_PRECONDITION, CONNECT_CODE_OUT_OF_RANGE ->
                    ERROR_KIND_INVALID_ARGUMENT;
            case CONNECT_CODE_NOT_FOUND -> ERROR_KIND_NOT_FOUND;
            case CONNECT_CODE_PERMISSION_DENIED, CONNECT_CODE_UNAUTHENTICATED -> ERROR_KIND_UNAUTHENTICATED;
            case CONNECT_CODE_UNIMPLEMENTED -> ERROR_KIND_UNIMPLEMENTED;
            case CONNECT_CODE_CANCELED, CONNECT_CODE_DEADLINE_EXCEEDED, CONNECT_CODE_UNAVAILABLE ->
                    ERROR_KIND_TRANSPORT;
            default -> ERROR_KIND_SERVER_ERROR;
        };
    }

    static CrabkaException errorForResponse(Response response) {
        int status = response.code();
        String message = response.message().isBlank() ? "HTTP " + status : response.message();
        try {
            ResponseBody body = response.body();
            if (body != null) {
                String payload = body.string();
                if (!payload.isBlank()) {
                    message = payload;
                    JsonNode error = JSON.readTree(payload);
                    String code = error.path("code").asText("");
                    if (!code.isBlank()) {
                        return errorForConnectCode(code, error.path("message").asText(message));
                    }
                }
            }
        } catch (IOException ignored) {
            // Fall back to the HTTP status taxonomy and best available message.
        } finally {
            response.close();
        }
        return errorForStatus(status, message);
    }

    private static CrabkaException errorForConnectCode(String code, String message) {
        return switch (code) {
            case "invalid_argument", "failed_precondition", "out_of_range" ->
                    new InvalidArgumentException(message);
            case "not_found" -> new NotFoundException(message);
            case "permission_denied", "unauthenticated" -> new UnauthenticatedException(message);
            case "unimplemented" -> new UnimplementedException(message);
            case "canceled", "deadline_exceeded", "unavailable" -> new TransportException(message);
            default -> new ServerException(message);
        };
    }

    static CrabkaException errorForStatus(int status, String message) {
        return switch (status) {
            case 400 -> new InvalidArgumentException(message);
            case 401 -> new UnauthenticatedException(message);
            case 404 -> new NotFoundException(message);
            case 408, 429, 502, 503, 504 -> new TransportException(message);
            default -> new ServerException(message);
        };
    }

    static String toServerFilter(Filter filter) {
        if (filter.op() != FilterOp.EQUALS) {
            throw new InvalidArgumentException("only equals filters are supported by live subscribe");
        }
        return serverFieldPath(filter.path()) + " = " + serverLiteral(filter.value());
    }

    private static String serverFieldPath(String path) {
        if (!path.startsWith("$.")) {
            throw new InvalidArgumentException("filter path must start with $.");
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
        return value.replace("'", "''");
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

    private record GatewayError(String kind, String message) {}
}
