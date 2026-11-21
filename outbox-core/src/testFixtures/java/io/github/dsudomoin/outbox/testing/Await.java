package io.github.dsudomoin.outbox.testing;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** The tests' polling waits: a condition until it holds, an AssertionError naming it once the timeout passes. */
public final class Await {

    private Await() {}

    /** A condition that may throw: a query, a metric read. */
    @FunctionalInterface
    public interface Condition {
        boolean met() throws Exception;
    }

    /** {@code condition} polled every 20 ms until it holds; an AssertionError "{what} not met within {timeout}" after. */
    public static void until(Duration timeout, String what, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.met()) {
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError(what + " not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }

    /** {@link #until(Duration, String, Condition)} as "condition". */
    public static void until(Duration timeout, Condition condition) throws Exception {
        until(timeout, "condition", condition);
    }

    /** {@code latch} counted down within {@code timeout}, else an AssertionError. */
    public static void latch(CountDownLatch latch, Duration timeout) throws InterruptedException {
        if (!latch.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
            throw new AssertionError("latch not counted down within " + timeout);
        }
    }
}
