package io.github.dsudomoin.outbox.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A report logged at most once an interval: {@link #due} for one that drops the rest, {@link #pass} for one that
 * counts what it dropped. Allocates nothing, so the paths that handle an {@code OutOfMemoryError} may use it.
 */
public final class LogLimit {

    private final long intervalNanos;
    private final AtomicLong last;
    private final AtomicInteger suppressed = new AtomicInteger();

    /** Due at once, then at most once every {@code intervalNanos}. */
    public LogLimit(long intervalNanos) {
        this.intervalNanos = intervalNanos;
        this.last = new AtomicLong(System.nanoTime() - intervalNanos);
    }

    /** -1 when the report is not due, counting it; else the number not logged since the previous report. */
    public int pass() {
        if (!due(last, intervalNanos)) {
            suppressed.incrementAndGet();
            return -1;
        }
        return suppressed.getAndSet(0);
    }

    /**
     * Whether a report last logged at {@code last} ({@code nanoTime}) may log now, at most once an
     * {@code intervalNanos}; if so, {@code last} becomes now, for one caller only.
     */
    public static boolean due(AtomicLong last, long intervalNanos) {
        long now = System.nanoTime();
        long at = last.get();
        return now - at >= intervalNanos && last.compareAndSet(at, now);
    }
}
