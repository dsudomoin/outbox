package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.ReceiverContext;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * The observation of one delivered record. Its carrier is the record as stored, so the application's propagating
 * receiver handler parents the observation by the trace the sender stored (the last header of a name wins).
 */
public final class OutboxDeliveryContext extends ReceiverContext<OutboxRecord> {

    private static final AtomicIntegerFieldUpdater<OutboxDeliveryContext> ENDED =
            AtomicIntegerFieldUpdater.newUpdater(OutboxDeliveryContext.class, "ended");

    private final OutboxRecord record;
    private final Instant now;
    @Nullable
    private final Observation step;
    @Nullable
    private volatile Outcome outcome;
    private volatile boolean failed;
    /** 1 once some thread took the end of the observation: the first end of a record wins. */
    private volatile int ended;
    /**
     * The default convention's high cardinality values, computed as the relay thread starts the observation, before
     * the record reaches the handler thread that may stop it.
     */
    @Nullable
    KeyValues highCardinality;

    OutboxDeliveryContext(OutboxRecord record, Instant now, @Nullable Observation step) {
        super((carrier, key) -> {
            var headers = carrier.headers();
            for (int i = headers.size() - 1; i >= 0; i--) {
                var h = headers.get(i);
                if (h.name().equals(key)) {
                    return h.value() == null ? null : new String(h.value(), UTF_8);
                }
            }
            return null;
        }, Kind.CONSUMER);
        this.record = record;
        this.now = now;
        this.step = step;
        setCarrier(record);
    }

    /** The record as stored. */
    public OutboxRecord record() {
        return record;
    }

    /** The relay step's clock, the end of the record's delay. */
    public Instant now() {
        return now;
    }

    /**
     * The observation of the relay step whose handler call delivers the record, for a link to its span
     * ({@link OutboxDeliveryTracingObservationHandler}); null when the step has none.
     */
    @Nullable
    public Observation step() {
        return step;
    }

    /**
     * The record's outcome, set before the observation stops: the handler's report, or the call's outcome when the
     * handler reported none; null before that, or when {@link #failed()}.
     */
    @Nullable
    public Outcome outcome() {
        return outcome;
    }

    /** The handler call ended without outcomes: interrupted, or the relay closing. */
    public boolean failed() {
        return failed;
    }

    /** Whether this caller ends the observation: true once, for the first. */
    boolean takeEnd() {
        return ENDED.compareAndSet(this, 0, 1);
    }

    void outcome(Outcome outcome) {
        this.outcome = outcome;
    }

    void markFailed() {
        this.failed = true;
    }
}
