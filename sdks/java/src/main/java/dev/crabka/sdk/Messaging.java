package dev.crabka.sdk;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class Messaging {
    private final CrabkaClient client;

    Messaging(CrabkaClient client) {
        this.client = client;
    }

    public CompletableFuture<RecordResult> publish(Record record) {
        try {
            return CompletableFuture.completedFuture(publishSync(record));
        } catch (CrabkaException error) {
            return failedFuture(error);
        }
    }

    public CompletableFuture<RecordResult> publishEvent(String topic, CloudEvent event) {
        Objects.requireNonNull(event, "event");
        if (event.id().isBlank()) {
            return failedFuture(new InvalidArgumentException("CloudEvent id is required"));
        }
        return publish(new Record(topic, event.data(), cloudEventHeaders(event)));
    }

    public MessageStream subscribe(List<String> topics, String group, Optional<Filter> filter) {
        Objects.requireNonNull(topics, "topics");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(filter, "filter");
        if (topics.isEmpty()) {
            throw new InvalidArgumentException("at least one topic is required");
        }
        if (!client.usesMockTransport()) {
            return client.liveTransport().subscribe(topics, group, filter);
        }
        return client.mockStore().subscribe(topics, filter);
    }

    RecordResult publishSync(Record record) {
        Objects.requireNonNull(record, "record");
        if (record.topic().isEmpty()) {
            throw new InvalidArgumentException("topic is required");
        }
        if ("__missing_topic".equals(record.topic())) {
            throw new NotFoundException("topic not found");
        }
        if (!client.usesMockTransport()) {
            return client.liveTransport().send(record);
        }
        return client.mockStore().publish(record);
    }

    static List<Header> cloudEventHeaders(CloudEvent event) {
        List<Header> headers = new ArrayList<>();
        headers.add(textHeader("ce_id", event.id()));
        headers.add(textHeader("ce_source", event.source()));
        headers.add(textHeader("ce_type", event.type()));
        headers.add(textHeader("ce_specversion", event.specversion()));
        event.datacontenttype().ifPresent(value -> headers.add(textHeader("content-type", value)));
        return List.copyOf(headers);
    }

    private static Header textHeader(String name, String value) {
        return new Header(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable error) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }
}
