package io.github.dsudomoin.outbox.internal;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Blocking work at shutdown, on a daemon thread of its own: a connection source that hands out nothing, or a server
 * that does not answer, then holds up the thread and not the shutdown. JDBC offers no timeout across both.
 */
final class Bounded {

    private Bounded() {}

    /**
     * Runs {@code work} on a new daemon thread {@code name} and waits for it for {@code timeout}. Returns whether it
     * ended; if not, it is interrupted, which ends a wait for a pool's connection but not one for a socket, and left to
     * itself. An interrupt of the caller ends the wait at once, stays set and leaves {@code work} to go on.
     */
    static boolean run(String name, Duration timeout, Runnable work) {
        var thread = new Thread(work, name);
        thread.setDaemon(true);
        thread.start();
        try {
            TimeUnit.NANOSECONDS.timedJoin(thread, timeout.toNanos());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return !thread.isAlive();
        }
        if (thread.isAlive()) {
            thread.interrupt();
            return false;
        }
        return true;
    }
}
