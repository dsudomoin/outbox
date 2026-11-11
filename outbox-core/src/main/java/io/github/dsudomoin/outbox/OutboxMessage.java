package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A message to relay. {@code key} orders messages of one topic ({@code null} = unordered), {@code payload} may be
 * {@code null} (Kafka tombstone).
 */
public record OutboxMessage(String topic, @Nullable byte[] key, @Nullable byte[] payload, List<OutboxHeader> headers) {

    private static final int MAX_TOPIC_LENGTH = 249; // Kafka's limit
    private static final int MAX_KEY_BYTES = 1024;

    public OutboxMessage {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (topic.length() > MAX_TOPIC_LENGTH) {
            throw new IllegalArgumentException("topic longer than " + MAX_TOPIC_LENGTH + " chars: " + topic.length());
        }
        requireStorable(topic);
        if (key != null && key.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("key longer than " + MAX_KEY_BYTES + " bytes: " + key.length);
        }
        headers = List.copyOf(headers);
    }

    /**
     * Postgres text cannot hold NUL, and the driver writes an unpaired surrogate as '?', so the row would carry another
     * topic than the one the shard and the key lock were hashed from. A char loop: no allocation per message.
     */
    private static void requireStorable(String topic) {
        for (int i = 0, n = topic.length(); i < n; i++) {
            char c = topic.charAt(i);
            if (c == 0) {
                throw new IllegalArgumentException("topic contains NUL at index " + i);
            }
            if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(topic.charAt(i + 1))) {
                i++;
            } else if (Character.isSurrogate(c)) {
                throw new IllegalArgumentException("topic contains an unpaired UTF-16 surrogate at index " + i);
            }
        }
    }

    /** Unkeyed message: no ordering relative to other messages. */
    public static OutboxMessage of(String topic, @Nullable byte[] payload) {
        return new OutboxMessage(topic, null, payload, List.of());
    }

    public static OutboxMessage of(String topic, @Nullable byte[] key, @Nullable byte[] payload) {
        return new OutboxMessage(topic, key, payload, List.of());
    }

    public static OutboxMessage of(String topic, @Nullable String key, @Nullable byte[] payload) {
        return of(topic, key == null ? null : key.getBytes(UTF_8), payload);
    }

    public OutboxMessage withHeader(String name, @Nullable byte[] value) {
        var copy = new ArrayList<>(headers);
        copy.add(new OutboxHeader(name, value));
        return new OutboxMessage(topic, key, payload, copy);
    }
}
