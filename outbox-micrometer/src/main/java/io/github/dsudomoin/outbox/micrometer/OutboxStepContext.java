package io.github.dsudomoin.outbox.micrometer;

import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;
import java.util.List;

/**
 * The observation of one relay step, current on the thread that steps (a relay worker, or the caller of
 * {@code OutboxRelay.drain()}) from its first shard with work until its transaction ended. A client sender context
 * with nothing to carry, so the application's propagating sender handler makes its span of kind CLIENT.
 */
public final class OutboxStepContext extends SenderContext<Object> {

    /** The carrier of the client contexts: not null, for a propagator that rejects a null one; nothing is kept. */
    static final Object NO_CARRIER = new Object();

    private final List<Integer> shards;
    /** Written and read on the thread that steps. */
    private int records;

    OutboxStepContext(List<Integer> shards) {
        super((carrier, key, value) -> {}, Kind.CLIENT);
        this.shards = List.copyOf(shards);
        setCarrier(NO_CARRIER);
    }

    /** The shards whose locks the step holds. */
    public List<Integer> shards() {
        return shards;
    }

    /** The records the step fetched so far, over its shards with work. */
    public int records() {
        return records;
    }

    void fetched(int records) {
        this.records += records;
    }
}
