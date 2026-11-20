package io.github.dsudomoin.outbox.kafka;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.internal.RecordScope;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The record scope outside a relay call for this module's tests (as the Kora adapter's is in a Kora application):
 * runs the call alone, counting the scopes entered per thread.
 */
public final class CountingScopeFallback implements RecordScope.Fallback {

    private static final Map<Thread, AtomicInteger> ENTERED = new ConcurrentHashMap<>();

    /** How many record scopes {@code thread} entered through this fallback. */
    static int entered(Thread thread) {
        var count = ENTERED.get(thread);
        return count == null ? 0 : count.get();
    }

    @Override
    public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
        ENTERED.computeIfAbsent(Thread.currentThread(), t -> new AtomicInteger()).incrementAndGet();
        return call.call();
    }
}
