package io.github.dsudomoin.outbox.internal;

import java.time.Duration;

public final class Backoff {

    private Backoff() {}

    /** {@code initial * 2^failures}, capped at {@code max}; never overflows; negative {@code failures} count as 0. */
    public static Duration delay(int failures, Duration initial, Duration max) {
        long factor = 1L << Math.min(Math.max(failures, 0), 62); // 1L << -1 would be Long.MIN_VALUE
        return initial.compareTo(max.dividedBy(factor)) > 0 ? max : initial.multipliedBy(factor);
    }

    /** {@link #delay} in nanoseconds, allocating nothing: for the paths that handle an OutOfMemoryError. */
    public static long delayNanos(int failures, long initialNanos, long maxNanos) {
        long factor = 1L << Math.min(Math.max(failures, 0), 62);
        return initialNanos > maxNanos / factor ? maxNanos : initialNanos * factor;
    }
}
