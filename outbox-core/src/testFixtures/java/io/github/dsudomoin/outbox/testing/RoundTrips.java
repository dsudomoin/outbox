package io.github.dsudomoin.outbox.testing;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Counts the round trips pgjdbc makes to the server: every exchange ends with a Sync message, after which the driver
 * waits for the server's ReadyForQuery, and the driver logs each one it sends at FINEST ({@code " FE=> Sync"}).
 * Statements sent together in one call (a query of several statements) share one Sync.
 */
public final class RoundTrips implements AutoCloseable {

    private static final Logger DRIVER = Logger.getLogger("org.postgresql.core.v3.QueryExecutorImpl");
    private static final Object INSTALLED = new Object();

    /** What {@link #count} returns: the action's result and the round trips it made. */
    public record Counted<T>(T result, int roundTrips) {}

    private final AtomicInteger syncs = new AtomicInteger();
    private final Handler handler;
    private final Level level;

    private RoundTrips(Predicate<Long> thread) {
        handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null && record.getMessage().contains("FE=> Sync")
                        && thread.test(record.getLongThreadID())) {
                    syncs.incrementAndGet();
                }
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
        };
        handler.setLevel(Level.FINEST);
        synchronized (INSTALLED) {
            level = DRIVER.getLevel();
            DRIVER.setLevel(Level.FINEST);
            DRIVER.addHandler(handler);
        }
    }

    /** Runs {@code action} on this thread, counting the Sync messages pgjdbc sends from it. */
    @SuppressWarnings("deprecation") // Thread.getId: the build targets Java 17, before threadId()
    public static <T> Counted<T> count(Callable<T> action) throws Exception {
        long thread = Thread.currentThread().getId();
        try (var counting = new RoundTrips(id -> id == thread)) {
            var result = action.call();
            return new Counted<>(result, counting.get());
        }
    }

    /** Counts, until closed, the Sync messages pgjdbc sends from the threads whose name starts with {@code prefix}. */
    @SuppressWarnings("deprecation") // Thread.getId, as in count
    public static RoundTrips onThreads(String prefix) {
        return new RoundTrips(id -> Thread.getAllStackTraces().keySet().stream()
                .anyMatch(t -> t.getId() == id && t.getName().startsWith(prefix)));
    }

    /** The round trips counted so far. */
    public int get() {
        return syncs.get();
    }

    @Override
    public void close() {
        synchronized (INSTALLED) {
            DRIVER.removeHandler(handler);
            DRIVER.setLevel(level);
        }
    }
}
