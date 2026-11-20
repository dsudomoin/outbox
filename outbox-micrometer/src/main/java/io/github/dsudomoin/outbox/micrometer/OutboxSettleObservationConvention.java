package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/** Names and key values of the settle observation; {@link DefaultOutboxSettleObservationConvention} by default. */
public interface OutboxSettleObservationConvention extends ObservationConvention<OutboxSettleContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxSettleContext;
    }
}
