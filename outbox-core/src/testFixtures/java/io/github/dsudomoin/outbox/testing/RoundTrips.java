package io.github.dsudomoin.outbox.testing;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Counts the round trips pgjdbc makes to the server on the calling thread: every exchange ends with a Sync message,
 * after which the driver waits for the server's ReadyForQuery, and the driver logs each one it sends at FINEST
 * ({@code " FE=> Sync"}). Statements sent together in one call (a query of several statements) share one Sync.
 */
public final class RoundTrips {

    private static final Logger DRIVER = Logger.getLogger("org.postgresql.core.v3.QueryExecutorImpl");

    /** What {@link #count} returns: the action's result and the round trips it made. */
    public record Counted<T>(T result, int roundTrips) {}

    private RoundTrips() {}

    /** Runs {@code action} on this thread, counting the Sync messages pgjdbc sends from it. */
    @SuppressWarnings("deprecation") // Thread.getId: the build targets Java 17, before threadId()
    public static synchronized <T> Counted<T> count(Callable<T> action) throws Exception {
        long thread = Thread.currentThread().getId();
        var syncs = new AtomicInteger();
        var handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLongThreadID() == thread && record.getMessage() != null
                        && record.getMessage().contains("FE=> Sync")) {
                    syncs.incrementAndGet();
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        handler.setLevel(Level.FINEST);
        var level = DRIVER.getLevel();
        DRIVER.setLevel(Level.FINEST);
        DRIVER.addHandler(handler);
        try {
            var result = action.call();
            return new Counted<>(result, syncs.get());
        } finally {
            DRIVER.removeHandler(handler);
            DRIVER.setLevel(level);
        }
    }
}
