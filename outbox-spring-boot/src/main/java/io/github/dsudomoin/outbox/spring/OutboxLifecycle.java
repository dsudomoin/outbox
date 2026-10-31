package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxRuntime;
import jakarta.annotation.Nullable;
import java.util.function.Supplier;
import org.springframework.context.SmartLifecycle;

/**
 * Runs the outbox with the application context: {@link #start()} creates or checks the tables, then starts the relay;
 * {@link #stop()} stops the relay within {@code outbox.relay.shutdown-timeout}, then deletes this instance's heartbeat
 * row. Its phase, {@code outbox.lifecycle.phase}, is {@link #DEFAULT_PHASE} by default: below the web server's, the
 * Kafka listener containers' and the default one, so the outbox starts before them and stops after them, while the
 * DataSource is still open. A start after a stop ({@code context.stop()}, then {@code start()}) runs a fresh runtime.
 * <p>
 * The {@code Outbox} and {@code OutboxAdmin} beans use the runtime of the latest start: before the first one they
 * refuse, so send from an {@code ApplicationRunner} or later, not from {@code @PostConstruct}; after a stop a send
 * still writes its row, delivered after the next start or by another instance.
 */
public final class OutboxLifecycle implements SmartLifecycle {

    /** Below the web server's phase ({@code MAX_VALUE - 2048}) and the Kafka listener containers' ({@code - 100}). */
    public static final int DEFAULT_PHASE = Integer.MAX_VALUE - 4096;

    static final String NOT_STARTED = "the outbox starts with the application context's lifecycle; send from an"
        + " ApplicationRunner or later, not from @PostConstruct";

    private final Supplier<Built> runtimes;
    private final int phase;
    /** Built, not started: the next start's runtime. */
    @Nullable
    private Built next;
    @Nullable
    private volatile Started current;
    private volatile boolean running;

    /** Builds the first runtime at once, so a config error fails the bean's creation. */
    OutboxLifecycle(Supplier<Built> runtimes, int phase) {
        this.runtimes = runtimes;
        this.phase = phase;
        this.next = runtimes.get();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        var built = next != null ? next : runtimes.get(); // a runtime starts once
        next = null;
        var runtime = built.runtime();
        try {
            runtime.start();
        } catch (RuntimeException | Error e) {
            built.release().run();
            throw e;
        }
        current = new Started(runtime, runtime.outbox(), runtime.admin(), built.release());
        running = true;
    }

    /**
     * Synchronous: the relay has stopped and the heartbeat row is gone when it returns. Also releases a runtime built
     * and never started, as on a context whose refresh failed before the start.
     */
    @Override
    public synchronized void stop() {
        if (running) {
            running = false;
            var started = current();
            try {
                started.runtime.close(); // deletes this instance's row through the heartbeat's pool first
            } finally {
                started.release.run();
            }
        } else if (next != null) {
            next.release().run();
            next = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    /**
     * The runtime of the latest start, stopped or not.
     *
     * @throws IllegalStateException before the first start
     */
    public OutboxRuntime runtime() {
        return current().runtime;
    }

    /** One volatile read per call of the delegates. */
    Started current() {
        var started = current;
        if (started == null) {
            throw new IllegalStateException(NOT_STARTED);
        }
        return started;
    }

    /** A runtime, built and not started, and what to release once it has closed or failed to start. */
    record Built(OutboxRuntime runtime, Runnable release) {}

    record Started(OutboxRuntime runtime, Outbox outbox, OutboxAdmin admin, Runnable release) {}
}
