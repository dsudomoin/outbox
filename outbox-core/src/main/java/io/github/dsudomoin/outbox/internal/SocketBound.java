package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.internal.Sql.SqlFunction0;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code outbox.networkTimeout} on the library's own connections: without a bound, a connection the network
 * dropped silently holds its thread on the socket for good, and with pgjdbc's default {@code socketTimeout = 0} there
 * is none. From the client a silent socket and a long statement look alike, so the library tells them apart by what it
 * knows of the statement: most have no reason to last, and get this bound; one under a {@code statement_timeout} of the
 * library's gets that long instead ({@link #atLeast}), as the server ends it by then and answers; and the one without
 * any, the concurrent build of the dead table's index, gets none, and {@link DeadIndexBuilder} asks the database whether
 * its backend still runs it. A bound the connection has of its own (the DataSource's {@code socketTimeout}) stands.
 */
public final class SocketBound {

    private static final Logger log = LoggerFactory.getLogger(SocketBound.class);
    /** Past a statement's own {@code statement_timeout}, for the server's answer to come. */
    static final Duration ANSWER_MARGIN = Duration.ofSeconds(5);

    /** Once per JVM: every component of an instance has a bound of its own, on one source's connections. */
    private static final AtomicBoolean WARNED = new AtomicBoolean();

    private final int millis;

    /** {@code timeout}: zero for none, at most {@link Integer#MAX_VALUE} ms, as the config check says. */
    public SocketBound(Duration timeout) {
        this.millis = (int) timeout.toMillis();
    }

    /**
     * The bound on {@code c}, unless it has one of its own; closing the result takes it back, before {@code c} goes back
     * to whichever pool it came from, where another user may run a longer statement. Neither call goes to the database.
     * A connection that does not take it still works: a WARN, once.
     */
    public Held on(Connection c) {
        if (millis == 0) {
            return new Held(c, false);
        }
        try {
            if (c.getNetworkTimeout() != 0) {
                return new Held(c, false);
            }
            c.setNetworkTimeout(Runnable::run, millis);
            return new Held(c, true);
        } catch (SQLException | RuntimeException e) {
            if (WARNED.compareAndSet(false, true)) {
                log.warn("outbox.networkTimeout could not be set on a connection of the library ({}), so a"
                        + " connection the network drops silently holds its thread until the OS closes the socket; give"
                        + " the DataSource a socketTimeout. Logged once", e.toString());
            }
            return new Held(c, false);
        }
    }

    /** What {@link #on} set, taken back by {@link #close}. A dead connection, which does not take it, goes anyway. */
    public static final class Held implements AutoCloseable {

        private final Connection c;
        private final boolean bound;

        private Held(Connection c, boolean bound) {
            this.c = c;
            this.bound = bound;
        }

        @Override
        public void close() {
            if (bound) {
                try {
                    c.setNetworkTimeout(Runnable::run, 0);
                } catch (SQLException | RuntimeException e) {
                    log.debug("outbox could not take back the network timeout of a connection", e);
                }
            }
        }
    }

    /**
     * One WARN when connections of {@code application}, whose transactions the library does not own and cannot bound,
     * have no socket bound. Asks one connection, once; one that does not say is left alone: only pgjdbc's are known.
     */
    public static void warnIfUnbounded(ConnectionSource application, String table) {
        try (var c = application.newConnection()) {
            if (c.getNetworkTimeout() == 0) {
                log.warn("outbox {}: the application's connections have no socket timeout (pgjdbc socketTimeout = 0;"
                        + " in Kora, db.dsProperties.socketTimeout). The library bounds its own threads with"
                        + " outbox.networkTimeout, but not a business transaction: one on a connection the"
                        + " network dropped silently waits until the instance restarts. Set socketTimeout, in seconds,"
                        + " above the longest statement of the service", table);
            }
        } catch (SQLException | RuntimeException e) {
            log.debug("outbox {} could not read the socket bound of the application's connections", table, e);
        }
    }

    /**
     * {@code body} under a network timeout long enough for statements the library's own {@code budget} ends, whatever
     * bound {@code c} has now, given back after it; zero: without one, for a statement with no budget. A connection
     * without a bound stays so.
     */
    static <T> T atLeast(Connection c, Duration budget, SqlFunction0<T> body) throws SQLException {
        int prior;
        try {
            prior = c.getNetworkTimeout();
        } catch (SQLException | RuntimeException e) {
            return body.apply(); // takes none: see on
        }
        long wanted = budget.isZero() ? 0 : budget.plus(ANSWER_MARGIN).toMillis();
        if (prior == 0 || (wanted != 0 && wanted <= prior)) {
            return body.apply();
        }
        c.setNetworkTimeout(Runnable::run, (int) Math.min(wanted, Integer.MAX_VALUE));
        try {
            return body.apply();
        } finally {
            try {
                c.setNetworkTimeout(Runnable::run, prior);
            } catch (SQLException | RuntimeException e) {
                log.debug("outbox could not give a connection its network timeout back", e);
            }
        }
    }
}
