package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.ConnectionSource;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advisory locks held for a span of work that runs on other connections, without a session of its own: a transaction
 * on a connection of its own takes them with {@code pg_advisory_xact_lock} and stays open, idle, until {@link #close},
 * which ends it and frees them. Nothing outlives the transaction, so behind a transaction-pooling proxy (pgbouncer,
 * Odyssey) no lock reaches another client's session, and the proxy keeps one server session for the transaction, so
 * the lock is held where it was taken. Read committed whatever the role's default, and every call ends in a
 * statement that takes no snapshot ({@link #DROP_PORTAL}): idle, the transaction holds no snapshot and no transaction
 * id, so it holds back neither vacuum nor a concurrent detach or index build.
 *
 * <p>Postgres ends the transaction of a client that stays silent past {@code idle}
 * ({@code idle_in_transaction_session_timeout}, set for the transaction), which frees the locks of a holder whose JVM
 * froze or whose network went; a process that dies frees them at once, as its connection closes (a proxy closes the
 * server session of a client that left mid-transaction). Meanwhile a shared daemon thread sends a statement every
 * {@link #KEEPALIVE}, so a holder whose work runs long is not taken for a frozen one.
 *
 * <p>The work's connection outlives a holder Postgres ended: a JVM that froze past {@code idle} and woke up would go on
 * with its work beside the instance that took the lock since. So the holder also takes a lock no one else does, on a
 * random {@code bigint} token, and {@link #fence} proves it alive on the work's connection: a shared try of the token
 * succeeds only once the holder is gone, and then fails the work with SQLState {@link #LOST}. While {@link #bind}
 * binds it to the work's thread and connection, every DDL statement of {@link PartitionDdl} fences first, and a
 * keepalive that finds the holder gone cancels the long statement the work runs ({@link #running}: a JDBC cancel,
 * which a proxy forwards to the server session running it) and aborts the work's connection.
 */
final class LockHolder implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LockHolder.class);
    /** How often an idle holder tells Postgres it is alive. */
    static final Duration KEEPALIVE = Duration.ofSeconds(2);
    /** SQLState of a work fenced off because its holder is gone: the lock may be another instance's by now. */
    static final String LOST = "OB001";
    /** How often a holder that waits for a lock tries it again. */
    static final Duration RETRY = Duration.ofMillis(50);
    /**
     * The most a statement of the holder may take before its connection counts as lost: each is short (a try, a
     * savepoint), and a keepalive stuck longer would let the idle timeout end the holder anyway.
     */
    static final Duration STATEMENT_BOUND = Duration.ofSeconds(10);
    /**
     * What every call on the holder ends with: a statement that takes no snapshot. A query's unnamed portal, which the
     * extended protocol leaves open until the next statement, keeps its snapshot, and so the backend's xmin, for as long
     * as the transaction idles after it; the next statement's drops it. Also the keepalive's statement.
     */
    private static final String DROP_PORTAL = "savepoint outbox_idle; release savepoint outbox_idle";

    private static final ScheduledExecutorService KEEPALIVES = Executors.newScheduledThreadPool(2, r -> {
        var t = new Thread(r, "outbox-lock-keepalive");
        t.setDaemon(true);
        return t;
    });
    private static final ThreadLocal<LockHolder> CURRENT = new ThreadLocal<>();

    private final Connection c;
    private final SocketBound.Held bound;
    private final long token;
    /** Serialises the holder's connection between its owner and the keepalive. */
    private final ReentrantLock use = new ReentrantLock();
    private final AtomicInteger savepoints = new AtomicInteger();
    private final ScheduledFuture<?> keepalive;
    /** The work's connection while {@link #bind} binds it: what a lost holder cancels and aborts. */
    @Nullable
    private volatile Connection work;
    /** The work's statement that may run long, while it runs ({@link #running}): what a lost holder cancels. */
    @Nullable
    private volatile Statement statement;
    /** Why the holder is gone, null while it holds. */
    @Nullable
    private volatile String lost;
    private boolean closed;

    private LockHolder(Connection c, SocketBound.Held bound, long token, Duration every) {
        this.c = c;
        this.bound = bound;
        this.token = token;
        this.keepalive = KEEPALIVES.scheduleWithFixedDelay(this::beat, every.toMillis(), every.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Opens the holder's transaction on a new connection of {@code source}, under {@code socket} capped at
     * {@link #STATEMENT_BOUND} (none stays none); Postgres ends it after {@code idle} without a statement. No lock yet
     * but the token's.
     */
    static LockHolder open(ConnectionSource source, SocketBound socket, Duration idle) throws SQLException {
        return open(source, socket, idle, KEEPALIVE);
    }

    /** {@link #open(ConnectionSource, SocketBound, Duration)} with a keepalive every {@code every}, for tests. */
    static LockHolder open(ConnectionSource source, SocketBound socket, Duration idle, Duration every)
            throws SQLException {
        var c = source.newConnection();
        SocketBound.Held bound = null;
        try {
            bound = socket.cappedAt(STATEMENT_BOUND).on(c);
            c.setAutoCommit(false);
            long token = ThreadLocalRandom.current().nextLong();
            try (var st = c.createStatement()) {
                // first: isolation must precede every query of the transaction
                st.execute("set transaction isolation level read committed; set local"
                        + " idle_in_transaction_session_timeout = " + idle.toMillis() + Ticks.noTransactionTimeout(c));
            }
            try (var ps = c.prepareStatement("select pg_advisory_xact_lock(?)")) {
                ps.setLong(1, token);
                ps.execute();
            }
            dropPortal(c);
            return new LockHolder(c, bound, token, every);
        } catch (SQLException | RuntimeException | Error e) {
            abort(c, e);
            if (bound != null) {
                bound.close();
            }
            throw e;
        }
    }

    /** {@code read}, queries only, on the holder's connection inside its transaction, its portal dropped after. */
    <T> T read(Sql.SqlFunction1<Connection, T> read) throws SQLException {
        use.lock();
        try {
            requireOpen();
            var result = read.apply(c);
            dropPortal(c);
            return result;
        } finally {
            use.unlock();
        }
    }

    /** A lock this holder took, and the savepoint before it, which {@link #release} rolls back to. */
    record Held(String savepoint) {}

    /**
     * Takes {@code pg_advisory_xact_lock(ns, key)}, trying again every {@link #RETRY} for up to {@code wait} while
     * another session holds it (zero: one try); null when it did not get it. Held until {@link #release} or
     * {@link #close}. Tries rather than a queued wait: a statement waiting for a lock holds a snapshot, and a
     * concurrent detach or index build that samples it then waits for the holder's whole transaction, as long as the
     * work under it lasts; a try holds one for microseconds, its portal dropped right after.
     */
    @Nullable
    Held lock(int ns, int key, Duration wait) throws SQLException {
        use.lock();
        try {
            requireOpen();
            var savepoint = "outbox_lock_" + savepoints.incrementAndGet();
            Sql.execute(c, "savepoint " + savepoint);
            long deadline = System.nanoTime() + wait.toNanos();
            while (!take(ns, key)) {
                dropPortal(c);
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    Sql.execute(c, "rollback to savepoint " + savepoint);
                    return null;
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(Math.min(left, RETRY.toNanos()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("interrupted waiting for an outbox lock", "57014", e);
                }
            }
            dropPortal(c);
            return new Held(savepoint);
        } finally {
            use.unlock();
        }
    }

    private boolean take(int ns, int key) throws SQLException {
        try (var ps = c.prepareStatement("select pg_try_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, ns);
            ps.setInt(2, key);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /**
     * Frees {@code held} and every lock taken after it, keeping the ones taken before. Never throws, as it runs in
     * {@code finally} blocks: a holder already gone has nothing left to free, and one whose release fails is taken for
     * gone ({@link #holds()} false), its work fenced off from then on.
     */
    void release(Held held) {
        use.lock();
        try {
            if (closed || lost != null) {
                return;
            }
            try (var st = c.createStatement()) {
                st.execute("rollback to savepoint " + held.savepoint() + "; release savepoint " + held.savepoint());
            } catch (SQLException | RuntimeException e) {
                lose(e.toString());
            }
        } finally {
            use.unlock();
        }
    }

    /**
     * Proves the holder alive in {@code work}'s transaction, or as one autocommit statement: throws SQLState
     * {@link #LOST} once it is gone. A shared try of the token, which only the holder holds exclusively: one that
     * succeeds finds it free (and holds it shared until {@code work}'s transaction ends, which keeps out nobody).
     */
    void fence(Connection work) throws SQLException {
        var why = lost;
        if (why == null) {
            try (var ps = work.prepareStatement("select pg_try_advisory_xact_lock_shared(?)")) {
                ps.setLong(1, token);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    if (!rs.getBoolean(1)) {
                        return;
                    }
                }
            }
            why = "its transaction is gone";
            lost = why;
        }
        throw new SQLException("outbox lock holder lost (" + why + "): the lock may be another session's by now, so"
                + " the work under it stops", LOST);
    }

    /** {@link #fence} with the holder {@link #bind} bound to this thread; nothing without one. */
    static void fenceCurrent(Connection work) throws SQLException {
        var holder = CURRENT.get();
        if (holder != null) {
            holder.fence(work);
        }
    }

    /**
     * {@link #fence} of the holder bound to this thread on the work's connection it was bound to; nothing without one.
     * Its first failure throws SQLState {@link #LOST}; once the holder is known gone, every later call throws
     * {@link Gone}, which callers that already reported the loss pass over quietly.
     */
    static void fenceBound() throws SQLException {
        var holder = CURRENT.get();
        if (holder == null) {
            return;
        }
        if (holder.lost != null) {
            throw new Gone();
        }
        var w = holder.work;
        if (w != null) {
            holder.fence(w);
        }
    }

    /** {@link #fenceBound} once the loss was reported: the work under the holder stops without saying it again. */
    static final class Gone extends SQLException {
        Gone() {
            super("outbox lock holder lost", LOST);
        }
    }

    /** Whether {@code e} is a fence's: the holder is gone and the work under it must stop, not go on to its next part. */
    static boolean lost(SQLException e) {
        return LOST.equals(e.getSQLState());
    }

    /**
     * Marks {@code st}, a statement of the bound work that may run long, as running until {@code null} is passed; nothing
     * without a bound holder.
     */
    static void running(@Nullable Statement st) {
        var holder = CURRENT.get();
        if (holder != null) {
            holder.statement = st;
        }
    }

    /** False once the holder is known gone. */
    boolean holds() {
        return lost == null;
    }

    /**
     * Binds the holder to this thread and to {@code work}, until the result is closed: {@link PartitionDdl}'s DDL
     * fences first ({@link #fenceCurrent}), and a keepalive that finds the holder gone cancels the statement in flight
     * on {@code work} and aborts it.
     */
    Scope bind(Connection work) {
        var prior = CURRENT.get();
        CURRENT.set(this);
        this.work = work;
        return () -> {
            this.work = null;
            this.statement = null;
            if (prior == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(prior);
            }
        };
    }

    /** What {@link #bind} returns. */
    interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private void beat() {
        if (!use.tryLock()) {
            return; // a statement of the holder runs: not idle
        }
        try {
            if (closed || lost != null) {
                return;
            }
            dropPortal(c);
        } catch (SQLException | RuntimeException e) {
            lose(e.toString());
        } finally {
            use.unlock();
        }
    }

    private void lose(String why) {
        lost = why;
        var w = work;
        if (w == null) {
            return;
        }
        log.warn("outbox lock holder lost ({}): stopping the work under it", why);
        var st = statement;
        if (st != null) {
            try {
                st.cancel();
            } catch (SQLException | RuntimeException e) {
                log.debug("outbox could not cancel the work of a lost lock holder", e);
            }
        }
        abort(w, null);
    }

    private static void dropPortal(Connection c) throws SQLException {
        Sql.execute(c, DROP_PORTAL);
    }

    private void requireOpen() throws SQLException {
        if (closed) {
            throw new IllegalStateException("lock holder closed");
        }
        var why = lost;
        if (why != null) {
            throw new SQLException("outbox lock holder lost (" + why + ")", LOST);
        }
    }

    /** Ends the transaction, freeing every lock, and closes the connection; one that fails to end it is aborted. */
    @Override
    public void close() {
        keepalive.cancel(false);
        use.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                c.rollback();
                c.setAutoCommit(true);
                bound.close();
                c.close();
            } catch (SQLException | RuntimeException e) {
                log.debug("outbox could not end a lock holder's transaction; aborting its connection", e);
                bound.close();
                abort(c, null);
            }
        } finally {
            use.unlock();
        }
    }

    /** Closes {@code c}'s session at once, so no pool hands it out mid-transaction; a failure joins {@code cause}. */
    private static void abort(Connection c, @Nullable Throwable cause) {
        try {
            c.abort(Runnable::run);
        } catch (SQLException | RuntimeException e) {
            if (cause != null) {
                cause.addSuppressed(e);
            } else {
                log.debug("outbox could not abort a connection", e);
            }
        }
    }
}
