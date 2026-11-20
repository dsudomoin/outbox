package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.common.KeyValues;
import java.util.stream.Collectors;

/**
 * {@code outbox.relay.step}, span {@code outbox relay step}. Low cardinality: {@code messaging.system}
 * ({@code outbox}). High cardinality: {@code outbox.shards}, the step's shards joined with {@code ,};
 * {@code outbox.records}, the records it fetched; and {@code outbox.shard} when it holds one shard.
 */
public class DefaultOutboxStepObservationConvention implements OutboxStepObservationConvention {

    public static final DefaultOutboxStepObservationConvention INSTANCE = new DefaultOutboxStepObservationConvention();

    private static final KeyValues LOW = KeyValues.of("messaging.system", "outbox");

    @Override
    public String getName() {
        return "outbox.relay.step";
    }

    @Override
    public String getContextualName(OutboxStepContext context) {
        return "outbox relay step";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(OutboxStepContext context) {
        return LOW;
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(OutboxStepContext context) {
        var shards = context.shards();
        var values = KeyValues.of(
                "outbox.shards", shards.stream().map(String::valueOf).collect(Collectors.joining(",")),
                "outbox.records", Integer.toString(context.records()));
        return shards.size() == 1 ? values.and("outbox.shard", Integer.toString(shards.get(0))) : values;
    }
}
