package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.common.KeyValues;

/**
 * {@code outbox.relay.settle}, span {@code outbox relay settle}. Low cardinality: {@code messaging.system}
 * ({@code outbox}). High cardinality: {@code outbox.records}, the records whose state the step's commit records.
 */
public class DefaultOutboxSettleObservationConvention implements OutboxSettleObservationConvention {

    public static final DefaultOutboxSettleObservationConvention INSTANCE =
            new DefaultOutboxSettleObservationConvention();

    private static final KeyValues LOW = KeyValues.of("messaging.system", "outbox");

    @Override
    public String getName() {
        return "outbox.relay.settle";
    }

    @Override
    public String getContextualName(OutboxSettleContext context) {
        return "outbox relay settle";
    }

    @Override
    public KeyValues getLowCardinalityKeyValues(OutboxSettleContext context) {
        return LOW;
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(OutboxSettleContext context) {
        return KeyValues.of("outbox.records", Integer.toString(context.records()));
    }
}
