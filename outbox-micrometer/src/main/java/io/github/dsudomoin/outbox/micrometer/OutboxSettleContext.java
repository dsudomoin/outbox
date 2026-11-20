package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;

/**
 * The observation of one relay step's writes, from its last handler call to its transaction's end: a child of the
 * step's, current on the thread that steps meanwhile. A client sender context with nothing to carry, so the
 * application's propagating sender handler makes its span of kind CLIENT.
 */
public final class OutboxSettleContext extends SenderContext<Object> {

    private int records;

    OutboxSettleContext() {
        super((carrier, key, value) -> {}, Kind.CLIENT);
        setCarrier(OutboxStepContext.NO_CARRIER);
    }

    /** The records whose state the step's commit records; 0 until the settle ends. */
    public int records() {
        return records;
    }

    void records(int records) {
        this.records = records;
    }
}
