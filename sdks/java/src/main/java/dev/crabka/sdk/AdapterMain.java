package dev.crabka.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

public final class AdapterMain {
    private static final int CONTRACT_MAJOR = 1;
    private static final int CONTRACT_MINOR = 1;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String GATEWAY_RECORD_NOT_ACQUIRED = "record is not acquired by this session";
    private static final String CONTRACT_QUEUE_MESSAGE_NOT_ACQUIRED = "queue message is not acquired";

    private CrabkaClient client = CrabkaClient.builder().endpoint("mock://gateway").build();
    private MessageStream subscription;
    private int nextQueueSessionId = 1;
    private final Map<String, String> queueSessionAliases = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        new AdapterMain().run();
    }

    private void run() throws Exception {
        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                BufferedWriter output = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
            String line;
            while ((line = input.readLine()) != null) {
                JsonNode command = JSON.readTree(line);
                output.write(JSON.writeValueAsString(handle(command)));
                output.newLine();
                output.flush();
            }
        } finally {
            closeSubscription();
        }
    }

    private Map<String, Object> handle(JsonNode command) {
        try {
            return handleChecked(command);
        } catch (Exception error) {
            return errorResponse(unwrap(error));
        }
    }

    private Map<String, Object> handleChecked(JsonNode command) {
        return switch (text(command, "cmd")) {
            case "hello" -> Map.of("hello", Map.of(
                    "contract_major", CONTRACT_MAJOR,
                    "contract_minor", CONTRACT_MINOR,
                    "language", "java"));
            case "configure" -> configure(command);
            case "publish" -> publish(command);
            case "publish_event" -> publishEvent(command);
            case "subscribe" -> subscribe(command);
            case "next_message" -> nextMessage(command);
            case "queue_acquire" -> queueAcquire(command);
            case "queue_ack" -> waitOk(client.queues().ack(text(command, "message_id")));
            case "queue_acknowledge" -> queueAcknowledge(command);
            case "queue_renew" -> queueRenew(command);
            case "db_connect" -> waitOk(client.database().connect(text(command, "name")));
            case "auth_sign_in" -> waitOk(client.auth().signIn(text(command, "username"), text(command, "password")));
            case "blob_put" -> waitOk(client.blob().put(text(command, "key"), decode(text(command, "value_b64"))));
            case "blob_get" -> ok(Map.of("value_b64", encode(client.blob().get(text(command, "key")).join())));
            default -> errorResponse(new InvalidArgumentException("unknown command"));
        };
    }

    private Map<String, Object> configure(JsonNode command) {
        CrabkaClient.Builder builder = CrabkaClient.builder().endpoint(text(command, "endpoint"));
        if (!command.path("bearer").isNull()) {
            builder.bearerToken(text(command, "bearer"));
        }
        closeSubscription();
        client = builder.build();
        nextQueueSessionId = 1;
        queueSessionAliases.clear();
        return ok(Map.of("bearer_configured", !command.path("bearer").isNull()));
    }

    private Map<String, Object> publish(JsonNode command) {
        Record record = new Record(text(command, "topic"), decode(text(command, "value_b64")), headers(command.path("headers")));
        return publishResponse(client.messaging().publish(record).join());
    }

    private Map<String, Object> publishEvent(JsonNode command) {
        JsonNode event = command.path("event");
        Optional<String> datacontenttype = event.path("datacontenttype").isNull()
                ? Optional.empty()
                : Optional.of(text(event, "datacontenttype"));
        CloudEvent cloudEvent = new CloudEvent(
                text(event, "id"),
                text(event, "source"),
                text(event, "type"),
                text(event, "specversion"),
                datacontenttype,
                decode(text(event, "data_b64")));
        return publishResponse(client.messaging().publishEvent(text(command, "topic"), cloudEvent).join());
    }

    private Map<String, Object> subscribe(JsonNode command) {
        List<String> topics = new ArrayList<>();
        command.path("topics").forEach(topic -> topics.add(topic.asText()));
        closeSubscription();
        subscription = client.messaging().subscribe(topics, text(command, "group"), filter(command.path("filter")));
        return ok(Map.of());
    }

    private void closeSubscription() {
        if (subscription == null) {
            return;
        }
        subscription.close();
        subscription = null;
    }

    private Map<String, Object> nextMessage(JsonNode command) {
        if (subscription == null) {
            return errorResponse(new InvalidArgumentException("subscribe before next_message"));
        }
        Inbound message = subscription.nextWithin(Duration.ofMillis(longValue(command, "timeout_ms")));
        if (message == null) {
            return errorResponse(new NotFoundException("no message available"));
        }
        return Map.of("message", Map.of(
                "topic", message.topic(),
                "partition", message.partition(),
                "offset", message.offset(),
                "value_b64", encode(message.value()),
                "headers", encodedHeaders(message.headers())));
    }

    private Map<String, Object> queueAcquire(JsonNode command) {
        QueueAcquireResult result = client.queues()
                .acquireWithSession(
                        text(command, "topic"),
                        text(command, "group"),
                        integer(command, "max"),
                        Duration.ofMillis(longValue(command, "lock_duration_ms")),
                        actualQueueSessionId(text(command, "session_id")))
                .join();
        String publicSessionId = rememberQueueSession(result.sessionId());
        return ok(Map.of("session_id", publicSessionId, "messages", encodedQueueMessages(result.messages())));
    }

    private Map<String, Object> queueAcknowledge(JsonNode command) {
        QueueBatchResult result = client.queues()
                .acknowledge(actualQueueSessionId(text(command, "session_id")), queueAckEntries(command.path("entries")))
                .join();
        return ok(Map.of("results", encodedQueueResults(result.results())));
    }

    private Map<String, Object> queueRenew(JsonNode command) {
        QueueBatchResult result = client.queues()
                .renew(actualQueueSessionId(text(command, "session_id")), queueRenewEntries(command.path("entries")))
                .join();
        return ok(Map.of("results", encodedQueueResults(result.results())));
    }

    private String rememberQueueSession(String actualSessionId) {
        for (Map.Entry<String, String> entry : queueSessionAliases.entrySet()) {
            if (entry.getValue().equals(actualSessionId)) {
                return entry.getKey();
            }
        }
        String publicSessionId = "queue-session-" + nextQueueSessionId;
        nextQueueSessionId += 1;
        queueSessionAliases.put(publicSessionId, actualSessionId);
        return publicSessionId;
    }

    private String actualQueueSessionId(String publicSessionId) {
        return queueSessionAliases.getOrDefault(publicSessionId, publicSessionId);
    }

    private static Optional<Filter> filter(JsonNode node) {
        if (node == null || node.isNull()) {
            return Optional.empty();
        }
        return Optional.of(new Filter(text(node, "path"), FilterOp.EQUALS, node.path("value")));
    }

    private static List<Header> headers(JsonNode nodes) {
        List<Header> headers = new ArrayList<>();
        nodes.forEach(node -> headers.add(new Header(text(node, "name"), node.path("value_b64").isNull() ? null : decode(text(node, "value_b64")))));
        return List.copyOf(headers);
    }

    private static List<QueueAckEntry> queueAckEntries(JsonNode nodes) {
        List<QueueAckEntry> entries = new ArrayList<>();
        nodes.forEach(node -> entries.add(new QueueAckEntry(text(node, "message_id"), queueAckType(text(node, "ack_type")))));
        return List.copyOf(entries);
    }

    private static List<QueueRenewEntry> queueRenewEntries(JsonNode nodes) {
        List<QueueRenewEntry> entries = new ArrayList<>();
        nodes.forEach(node -> entries.add(new QueueRenewEntry(text(node, "message_id"))));
        return List.copyOf(entries);
    }

    private static QueueAckType queueAckType(String value) {
        return switch (value) {
            case "release" -> QueueAckType.RELEASE;
            case "reject" -> QueueAckType.REJECT;
            default -> QueueAckType.ACCEPT;
        };
    }

    private static List<Map<String, Object>> encodedHeaders(List<Header> headers) {
        List<Map<String, Object>> encoded = new ArrayList<>();
        headers.forEach(header -> {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", header.name());
            entry.put("value_b64", header.value() == null ? null : encode(header.value()));
            encoded.add(entry);
        });
        return List.copyOf(encoded);
    }

    private static List<Map<String, Object>> encodedQueueMessages(List<QueueMessage> messages) {
        List<Map<String, Object>> encoded = new ArrayList<>();
        messages.forEach(message -> {
            byte[] value = message.value();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("message_id", message.messageId());
            entry.put("topic", message.topic());
            entry.put("partition", message.partition());
            entry.put("offset", message.offset());
            entry.put("value_b64", value == null ? null : encode(value));
            entry.put("headers", encodedHeaders(message.headers()));
            entry.put("delivery_count", message.deliveryCount());
            encoded.add(entry);
        });
        return List.copyOf(encoded);
    }

    private static List<Map<String, Object>> encodedQueueResults(List<QueueResult> results) {
        List<Map<String, Object>> encoded = new ArrayList<>();
        results.forEach(result -> {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("message_id", result.messageId());
            entry.put("error", result.error() == null ? null : Map.of(
                    "kind", result.error().kind(),
                    "message", contractQueueErrorMessage(result.error().message())));
            encoded.add(entry);
        });
        return List.copyOf(encoded);
    }

    private static String contractQueueErrorMessage(String message) {
        return GATEWAY_RECORD_NOT_ACQUIRED.equals(message) ? CONTRACT_QUEUE_MESSAGE_NOT_ACQUIRED : message;
    }

    private static Map<String, Object> publishResponse(RecordResult result) {
        return ok(Map.of("partition", result.partition(), "offset", result.offset(), "deduplicated", result.deduplicated()));
    }

    private static Map<String, Object> waitOk(java.util.concurrent.CompletableFuture<Void> future) {
        future.join();
        return ok(Map.of());
    }

    private static Map<String, Object> ok(Map<String, ?> body) {
        return Map.of("ok", body);
    }

    private static Map<String, Object> errorResponse(Throwable error) {
        Throwable unwrapped = unwrap(error);
        if (unwrapped instanceof UnimplementedException unimplemented
                && !unimplemented.module().isBlank()
                && !unimplemented.gatedOn().isBlank()) {
            return Map.of("error", Map.of(
                    "kind", unimplemented.kind(),
                    "module", unimplemented.module(),
                    "gated_on", unimplemented.gatedOn()));
        }
        if (unwrapped instanceof CrabkaException crabkaException) {
            return Map.of("error", Map.of("kind", crabkaException.kind(), "message", crabkaException.getMessage()));
        }
        return Map.of("error", Map.of("kind", "server_error", "message", unwrapped.getMessage()));
    }

    private static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }

    private static String text(JsonNode node, String field) {
        return node.path(field).asText();
    }

    private static int integer(JsonNode node, String field) {
        return node.path(field).asInt();
    }

    private static long longValue(JsonNode node, String field) {
        return node.path(field).asLong();
    }

    private static byte[] decode(String value) {
        return Base64.getDecoder().decode(value);
    }

    private static String encode(byte[] value) {
        return Base64.getEncoder().encodeToString(value);
    }
}
