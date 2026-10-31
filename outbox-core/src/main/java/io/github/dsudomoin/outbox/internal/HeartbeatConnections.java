package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.ConnectionSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The heartbeat's connections, from a source of its own, so that business load exhausting the application's pool does
 * not starve the beats. When the heartbeat source fails to connect, that beat takes the application's connection
 * instead, with a WARN at most once a minute, so a broken heartbeat source is never worse than none. The heartbeat
 * source must reach the application's database, as {@link SchemaCheck#where} tells it: {@link #start} fails on a
 * mismatch. When it cannot connect at start, the first beat that gets a connection of its own measures it against an
 * application connection taken for it, and on a mismatch logs an ERROR and stays on the application's connections until
 * restart. Both sides are always measured together, never against a value recorded earlier: {@code where} includes the
 * server's start time, which changes on every restart and failover.
 */
final class HeartbeatConnections implements ConnectionSource {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatConnections.class);
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);

    private enum State {
        /** Not measured yet: the start could not reach the heartbeat source. */
        UNCHECKED,
        /** Reaches the application's database: the beats take its connections. */
        CHECKED,
        /** Reaches another database: the beats take the application's connections until restart. */
        OFF
    }

    private final ConnectionSource application;
    private final ConnectionSource heartbeat;
    /** When the fallback WARN last logged ({@link System#nanoTime()}). */
    private final AtomicLong warned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    private volatile State state = State.UNCHECKED;
    private volatile String table;

    HeartbeatConnections(ConnectionSource application, ConnectionSource heartbeat) {
        this.application = application;
        this.heartbeat = heartbeat;
    }

    /**
     * Checks the heartbeat source against {@code reference}, a connection to the application's database, on
     * {@code table}, the resolved, schema-qualified base name; an unreachable heartbeat source only logs a WARN.
     */
    void start(Connection reference, String table) throws SQLException {
        this.table = table;
        Connection own;
        try {
            own = heartbeat.newConnection();
        } catch (SQLException | RuntimeException e) {
            log.warn("outbox heartbeat connection source of {} unreachable at start: {}; the beats take the"
                    + " application's connections until it connects, and its first connection is then checked against"
                    + " the application's database", table, e.toString());
            return;
        }
        try (own) {
            var app = SchemaCheck.where(reference, table);
            var mine = SchemaCheck.where(own, table);
            if (!app.equals(mine)) {
                throw new IllegalStateException(mismatch(mine, app) + "; point it at the database of " + table);
            }
        }
        state = State.CHECKED;
    }

    @Override
    public Connection newConnection() throws SQLException {
        var at = state;
        if (at == State.OFF) {
            return application.newConnection();
        }
        Connection own;
        try {
            own = heartbeat.newConnection();
        } catch (SQLException | RuntimeException e) {
            if (due()) {
                log.warn("outbox heartbeat connection source of {} failed to connect, this beat uses the application's"
                        + " connections; this WARN repeats at most once a minute: {}", table, e.toString());
            }
            return application.newConnection();
        }
        return at == State.CHECKED ? own : checked(own);
    }

    /**
     * {@code own}, the heartbeat source's first connection after a start that could not reach it, when it reaches the
     * database of an application connection taken now, else that application connection; without one, the beat fails.
     */
    private Connection checked(Connection own) throws SQLException {
        Connection app = null;
        try {
            app = application.newConnection();
            var expected = SchemaCheck.where(app, table);
            var mine = SchemaCheck.where(own, table);
            boolean same = expected.equals(mine);
            if (!same) {
                log.error("{}; the heartbeat of {} uses the application's connections until restart, point the"
                        + " heartbeat source at its database", mismatch(mine, expected), table);
            }
            state = same ? State.CHECKED : State.OFF;
            (same ? app : own).close();
            return same ? own : app;
        } catch (SQLException | RuntimeException e) { // closing one already closed is a no-op
            close(own, e);
            if (app != null) {
                close(app, e);
            }
            throw e;
        }
    }

    private static String mismatch(String heartbeat, String application) {
        return "outbox heartbeat connection source reaches another database than the application's: heartbeat "
                + heartbeat + ", application " + application;
    }

    /** Whether the fallback WARN may log now, at most once a {@link #WARN_INTERVAL}. */
    private boolean due() {
        long now = System.nanoTime();
        long last = warned.get();
        return now - last >= WARN_INTERVAL && warned.compareAndSet(last, now);
    }

    private static void close(Connection c, Throwable cause) {
        try {
            c.close();
        } catch (SQLException e) {
            cause.addSuppressed(e);
        }
    }
}
