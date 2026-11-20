package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.common.KeyValues;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code outbox.enqueue}, span {@code outbox enqueue <topic>}. Low cardinality: {@code messaging.system}
 * ({@code outbox}) and {@code messaging.destination.name}. High cardinality: {@code outbox.message_id} and
 * {@code outbox.shard}.
 */
public class DefaultOutboxEnqueueObservationConvention implements OutboxEnqueueObservationConvention {

    public static final DefaultOutboxEnqueueObservationConvention INSTANCE =
            new DefaultOutboxEnqueueObservationConvention();

    /** Per topic: the contextual name and the low cardinality values (as many as the meters' {@code topic} tags). */
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return "outbox.enqueue";
    }

    @Override
    public String getContextualName(OutboxEnqueueContext context) {
        return topic(context).name();
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(OutboxEnqueueContext context) {
        return topic(context).values();
    }

    /** Computed once per message: the observation asks at start and at stop. */
    @Override
    public KeyValues getHighCardinalityKeyValues(OutboxEnqueueContext context) {
        var cached = context.highCardinality;
        if (cached == null) {
            cached = KeyValues.of("outbox.message_id", context.messageId().toString(),
                    "outbox.shard", Integer.toString(context.shard()));
            context.highCardinality = cached;
        }
        return cached;
    }

    private Topic topic(OutboxEnqueueContext context) {
        return topics.computeIfAbsent(context.message().topic(), Topic::of);
    }

    private record Topic(String name, KeyValues values) {
        static Topic of(String topic) {
            return new Topic("outbox enqueue " + topic,
                    KeyValues.of("messaging.system", "outbox", "messaging.destination.name", topic));
        }
    }
}
