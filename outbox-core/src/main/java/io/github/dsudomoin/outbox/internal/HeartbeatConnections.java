package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.ConnectionSource;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The heartbeat's connections, from a source of its own, so that business load exhausting the application's pool does
 * not starve the beats. When the heartbeat source fails to connect, or its check query fails, that beat takes the
 * application's connection instead, with a WARN at most once a minute, so a broken heartbeat source is never worse than
 * none. The heartbeat source must reach the application's database, as {@link SchemaCheck#where} tells it on the table
 * name the beats use: {@link #start} fails on a mismatch, and on nothing else. When it cannot check at start, the first
 * beat that gets an answer from a connection of its own measures it against an application connection taken for it,
 * and on a mismatch logs an ERROR and stays on the application's connections until restart. Both sides are always
 * measured together, back to back, never against a value recorded earlier: {@code where} includes the server's start
 * time, which changes on every restart and failover. A mismatch is measured once more before it counts, as another
 * instance's first bootstrap may create the table between the two measures, while another database stays another.
 */
final class HeartbeatConnections implements ConnectionSource {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatConnections.class);
    private static final long WARN_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    private static final String SEARCH_PATH = " (and, for an unqualified name, give both roles the same search_path)";

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
    private final SocketBound socket;
    /** When the fallback WARN last logged ({@link System#nanoTime()}). */
    private final AtomicLong warned = new AtomicLong(System.nanoTime() - WARN_INTERVAL);
    private volatile State state = State.UNCHECKED;
    private volatile String table;

    HeartbeatConnections(ConnectionSource application, ConnectionSource heartbeat) {
        this(application, heartbeat, new SocketBound(Duration.ZERO));
    }

    HeartbeatConnections(ConnectionSource application, ConnectionSource heartbeat, SocketBound socket) {
        this.application = application;
        this.heartbeat = heartbeat;
        this.socket = socket;
    }

    /**
     * Checks the heartbeat source against {@code reference}, a connection to the application's database, on
     * {@code table}, the base table name the beats use. Only another database fails it, and a failure of
     * {@code reference} is thrown. A heartbeat source that fails to connect, or whose check query fails, only logs a
     * WARN, as does one going silent on its check past the beat's socket bound. One that does not answer its login holds
     * the start until its own connection timeout: a pool has one, a bare DataSource needs {@code loginTimeout}.
     */
    void start(Connection reference, String table) throws SQLException {
        this.table = table;
        Connection own;
        try {
            own = heartbeat.newConnection(); // first, so that both measures follow each other
        } catch (SQLException | RuntimeException e) {
            unreachable(e);
            return;
        }
        Measure m;
        try {
            m = measure(reference, own);
        } finally {
            try {
                own.close();
            } catch (SQLException e) { // the measures are taken: a failed close changes none of them
            }
        }
        if (m.failure() != null) {
            unreachable(m.failure());
        } else if (!m.same()) {
            throw new IllegalStateException(m.mismatch() + "; point it at the database of " + table + SEARCH_PATH);
        } else {
            state = State.CHECKED;
        }
    }

    /** The WARN of a start that could not measure the heartbeat source. */
    private void unreachable(Exception e) {
        log.warn("outbox heartbeat connection source of {} unreachable at start: {}; the beats take the application's"
                + " connections until it connects, and its first connection is then checked against the application's"
                + " database", table, e.toString());
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
            fallback(e);
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
            var m = measure(app, own);
            if (m.failure() != null) {
                close(own, m.failure());
                fallback(m.failure());
                return app;
            }
            boolean same = m.same();
            if (!same) {
                log.error("{}; the heartbeat of {} uses the application's connections until restart, point the"
                        + " heartbeat source at its database" + SEARCH_PATH, m.mismatch(), table);
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

    /**
     * {@link #once}, and once more on a mismatch: another instance's first bootstrap may create the table between the
     * two measures, while another database stays another.
     */
    private Measure measure(Connection app, Connection own) throws SQLException {
        try (var appBound = socket.on(app); var ownBound = socket.on(own)) {
            var first = once(app, own);
            return first.failure() != null || first.same() ? first : once(app, own);
        }
    }

    /**
     * {@code where} on {@code app}, then at once on {@code own}; a failure on {@code app} is thrown, while one on
     * {@code own} is kept in the measure.
     */
    private Measure once(Connection app, Connection own) throws SQLException {
        var expected = SchemaCheck.where(app, table);
        try {
            return new Measure(expected, SchemaCheck.where(own, table), null);
        } catch (SQLException | RuntimeException e) {
            return new Measure(expected, null, e);
        }
    }

    /** Both sides' {@code where}; {@code own} is null when its query failed with {@code failure}. */
    private record Measure(String app, @Nullable String own, @Nullable Exception failure) {

        boolean same() {
            return app.equals(own);
        }

        String mismatch() {
            return HeartbeatConnections.mismatch(own, app);
        }
    }

    /** The heartbeat source failed for this beat, which takes the application's connection instead. */
    private void fallback(Exception e) {
        if (due()) {
            log.warn("outbox heartbeat connection source of {} failed, this beat uses the application's connections;"
                    + " this WARN repeats at most once a minute: {}", table, e.toString());
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
