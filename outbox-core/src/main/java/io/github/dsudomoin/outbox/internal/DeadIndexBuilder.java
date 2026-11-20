package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the dead table's {@code <dead>_due} when bootstrap left it out (a dead table with rows) or an interrupted
 * build left it INVALID, off the maintenance lock and thread: a concurrent build waits for every transaction with an
 * older snapshot, a session queued for {@code LOCK_MAINTENANCE} included, so holding that lock would deadlock with it,
 * and the scan may take hours. {@link #startIfDue} only checks, from the maintenance step; the build runs on one daemon
 * thread {@code outbox-dead-index} per instance, on its own autocommit pool connection, under
 * {@code LOCK_DEAD_INDEX}, which a {@link LockHolder} on a second connection tries: an instance that does not get it
 * leaves the build to the holder. Holding it, the thread reads the catalog again, drops an INVALID index concurrently
 * (its wait cut at {@code lockTimeout}) and builds it concurrently: SHARE UPDATE EXCLUSIVE, which dead inserts pass,
 * no {@code statement_timeout} of the library's, as the scan may take hours ({@link PartitionDdl#executeConcurrently}:
 * nothing is set for the session, so it runs behind a transaction-pooling proxy too, and a role's or database's
 * {@code statement_timeout} that cuts it fails it, with the remedy in its WARN). No {@code lock_timeout} can be set for
 * it either, so a watch ({@link #watchWaits}) bounds each of its lock waits (the wait for older transactions included):
 * one past {@code lockTimeout}, read from {@code pg_locks.waitstart} on the holder's connection, is cancelled and fails
 * the build as a lock timeout (55P03), so it never holds its lock on the dead table, which a later ALTER of the table
 * needs, behind a long transaction.
 * Any failure is a WARN and backs the next try off by 1, 2, 4… maintenance intervals, at most {@link #MAX_BACKOFF}; a
 * success resets it. The backoff is in memory: a restart tries at its first run. {@link #close} cancels a statement in
 * flight (a JDBC cancel, which a proxy forwards to the server session running it); a thread still running then aborts
 * its connection however its build ended, which the pool evicts rather than reuse where a late cancel may reach, and the
 * next build drops an INVALID leftover. An {@link Error} is logged at ERROR and ends the thread.
 */
final class DeadIndexBuilder {

    /** How often {@link #watchWaits} looks, at most: a fraction of {@code lockTimeout}. */
    private static final Duration WATCH_EVERY = Duration.ofSeconds(1);
    private static final ScheduledExecutorService WATCH = Executors.newSingleThreadScheduledExecutor(r -> {
        var t = new Thread(r, "outbox-dead-index-watch");
        t.setDaemon(true);
        return t;
    });
    /** The longest wait for a lock of a backend holding the build's SHARE UPDATE EXCLUSIVE on the dead table, s. */
    private static final String BUILD_WAIT = "select coalesce(extract(epoch from clock_timestamp() - min(w.waitstart)),"
            + " 0)::float8 from pg_locks l join pg_locks w on w.pid = l.pid and not w.granted and w.waitstart is not null"
            + " where l.locktype = 'relation' and l.relation = to_regclass(?) and l.mode = 'ShareUpdateExclusiveLock'"
            + " and l.granted";

    /** The longest backoff after a failed build. */
    static final Duration MAX_BACKOFF = Duration.ofHours(24);
    static final String THREAD_NAME = "outbox-dead-index";

    private static final Logger log = LoggerFactory.getLogger(DeadIndexBuilder.class);
    private static final String STATE = "select indisvalid from pg_index where indexrelid = to_regclass(?)";

    private final OutboxTransactions db;
    private final SocketBound socket;
    private final TableNames names;
    private final String index;
    private final Duration interval;
    private final Duration lockTimeout;
    /** The most {@link #close} waits for a connection and the cancel of a build, on top of its timeout. */
    private static final Duration CANCEL_WAIT = Duration.ofSeconds(1);
    /** Guards {@link #closed}, {@link #thread} and {@link #inDdl}, so a close either sees the DDL or prevents it. */
    private final Object monitor = new Object();
    private boolean closed;
    @Nullable
    private Thread thread;
    /** Whether a drop or build runs. */
    private boolean inDdl;
    /** Its statement once it runs, null otherwise: what {@link #close} cancels. */
    @Nullable
    private Statement ddlStatement;
    /** The connection it runs on, null otherwise. */
    @Nullable
    private Connection ddlConnection;
    /** Set once {@link #watchWaits} cancelled the statement: its 57014 is then a lock timeout. */
    private boolean waitCut;
    /** Runs of maintenance in a row that found no backend running the drop or build; on maintenance's thread. */
    private int silentRuns;
    /** Whether {@link #letGoIfStopped} has said that it cannot see the build's state; on maintenance's thread. */
    private boolean stateHiddenLogged;
    private volatile int failures;
    private volatile long retryAt = System.nanoTime();

    DeadIndexBuilder(OutboxTransactions db, TableNames names, Duration interval, Duration lockTimeout,
            SocketBound socket) {
        this.db = db;
        this.socket = socket;
        this.names = names;
        this.index = names.dead() + "_due";
        this.interval = interval;
        this.lockTimeout = lockTimeout;
    }

    /** From the maintenance step, on its connection {@code c}: starts the thread when the index needs it and may. */
    void startIfDue(Connection c) throws SQLException {
        Connection on = null;
        synchronized (monitor) {
            if (closed) {
                return;
            }
            if (thread != null && thread.isAlive()) {
                on = ddlConnection;
                if (!inDdl || on == null) {
                    silentRuns = 0;
                    return;
                }
            } else if (System.nanoTime() - retryAt < 0) {
                return;
            }
        }
        if (on != null) {
            letGoIfStopped(c, on);
            return;
        }
        if (Boolean.TRUE.equals(state(c))) {
            return;
        }
        synchronized (monitor) {
            if (closed || (thread != null && thread.isAlive())) {
                return;
            }
            var t = new Thread(this::build, THREAD_NAME);
            t.setDaemon(true);
            thread = t;
            t.start();
        }
    }

    /**
     * The build has no bound on its socket, as it may run for hours, so a connection the network dropped silently would
     * hold its thread for good. From the client that looks the same as a long build; the database can tell: a drop or
     * build that runs holds SHARE UPDATE EXCLUSIVE on the dead table in a backend that runs a statement. None at two
     * runs of maintenance in a row (between the build's own statements it idles a moment) means its backend has
     * answered into the void: {@code on} is aborted, which ends the thread as any failed build. Found by its lock, not
     * by a pid, which a transaction-pooling proxy would not keep. A state Postgres hides (another role's backend without
     * {@code pg_read_all_stats}, as during a credential rotation; {@code track_activities = off}) counts as running,
     * logged once; a vacuum of the table, which holds the same lock, delays the abort.
     */
    private void letGoIfStopped(Connection c, Connection on) throws SQLException {
        String state;
        try (var ps = c.prepareStatement("select coalesce(min(case when a.state = 'active' then 'active'"
                + " when a.state is null or a.state = 'disabled' then 'hidden' else 'idle' end), 'gone')"
                + " from pg_locks l left join pg_stat_activity a on a.pid = l.pid where l.locktype = 'relation'"
                + " and l.relation = to_regclass(?) and l.mode = 'ShareUpdateExclusiveLock' and l.granted")) {
            ps.setString(1, names.dead());
            try (var rs = ps.executeQuery()) {
                rs.next();
                state = rs.getString(1);
            }
        }
        if (state.equals("hidden")) {
            if (!stateHiddenLogged) {
                stateHiddenLogged = true;
                log.warn("outbox build of {}: Postgres hides the state of its backend from maintenance's role, so"
                        + " only a backend that is gone counts as stopped; grant pg_read_all_stats (or pg_monitor)"
                        + " to the role that runs maintenance", index);
            }
            state = "active";
        }
        if (state.equals("active")) {
            silentRuns = 0;
            return;
        }
        if (++silentRuns >= 2) {
            silentRuns = 0;
            log.warn("outbox build of {}: no backend runs it any more, so its answer was lost: aborting its"
                    + " connection, and the next run looks at the index again", index);
            abort(on, null);
        }
    }

    /** True while the thread runs. */
    boolean running() {
        synchronized (monitor) {
            return thread != null && thread.isAlive();
        }
    }

    /** Stops starting builds, cancels a drop or build in flight and waits for the thread up to {@code timeout}. */
    void close(Duration timeout) {
        Thread t;
        synchronized (monitor) {
            closed = true;
            t = thread;
            // under the monitor, so the thread cannot begin another DDL statement meanwhile, and past its DDL finds
            // closed set and aborts the connection: however late this cancel comes, no other user of the pool is there
            var st = ddlStatement;
            if (st != null) {
                if (!Bounded.run(THREAD_NAME + "-cancel", CANCEL_WAIT, () -> cancel(st))) { // an unanswered cancel
                    log.warn("outbox gave up cancelling the build of {} after {} at shutdown; the next build drops"
                            + " what it leaves", index, CANCEL_WAIT);
                }
            }
        }
        if (t == null) {
            return;
        }
        try {
            TimeUnit.NANOSECONDS.timedJoin(t, timeout.toNanos());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            log.warn("outbox thread {} still busy {} after shutdown began; it ends with its statement", THREAD_NAME,
                    timeout);
        }
    }

    private void cancel(Statement st) {
        try {
            st.cancel(); // a JDBC cancel: a transaction-pooling proxy forwards it to the server session that runs it
        } catch (SQLException | RuntimeException e) {
            log.warn("outbox could not cancel the build of {} at shutdown: {}", index, e.toString());
        }
    }

    private void build() {
        long start = System.nanoTime();
        // a holder the network lost frees the lock once idle; the build itself runs on c, which this never cuts. The
        // build's connection is taken once the lock is held: none is held while waiting for another
        try (var holder = LockHolder.open(db, socket, Maintenance.HOLDER_IDLE)) {
            if (holder.lock(names.internalNamespace(), TableNames.LOCK_DEAD_INDEX, Duration.ZERO) == null) {
                return; // another instance builds it
            }
            var period = Duration.ofMillis(Math.max(50, Math.min(WATCH_EVERY.toMillis(), lockTimeout.toMillis() / 4)));
            var watch = WATCH.scheduleWithFixedDelay(() -> watchWaits(holder), period.toMillis(), period.toMillis(),
                    TimeUnit.MILLISECONDS);
            try (var c = db.newConnection(); var bound = socket.on(c); var scope = holder.bind(c)) {
                c.setAutoCommit(true);
                Exception failure = null;
                try {
                    var valid = state(c);
                    if (Boolean.TRUE.equals(valid)) {
                        return;
                    }
                    if (valid != null) {
                        if (!ddl(c, lockTimeout, "drop index concurrently if exists " + index)) {
                            return;
                        }
                        log.info("outbox dropped {}, left invalid by an interrupted concurrent build", index);
                    }
                    if (!ddl(c, Duration.ZERO, "create index concurrently if not exists " + names.unqualified(index)
                            + " on " + names.dead() + " (due_at) where due_at is not null")) {
                        return;
                    }
                } catch (SQLException | RuntimeException e) {
                    failure = e;
                    throw e;
                } finally {
                    // past a close, a cancel may still come for it: the pool evicts it instead of reusing it
                    if (isClosed()) {
                        abort(c, failure);
                    }
                }
            } finally {
                watch.cancel(false);
            }
            failures = 0;
            log.info("outbox built {} concurrently in {} ms", index, (System.nanoTime() - start) / 1_000_000);
        } catch (Error e) {
            log.error("outbox thread {} stopped by a fatal error", THREAD_NAME, e);
            throw e;
        } catch (SQLException | RuntimeException e) {
            if (isClosed()) {
                log.info("outbox build of {} ended at shutdown; the next one drops anything it left", index);
                return;
            }
            int failed = ++failures;
            var backoff = interval.multipliedBy(1L << Math.min(failed - 1, 20));
            if (backoff.compareTo(MAX_BACKOFF) > 0) {
                backoff = MAX_BACKOFF;
            }
            retryAt = System.nanoTime() + backoff.toNanos();
            String state = e instanceof SQLException s ? s.getSQLState() : e.getClass().getSimpleName();
            log.warn("outbox could not build {} concurrently after {} ms ({}: {}); a drop's lock wait past {} ms means"
                    + " a long transaction on the database, see pg_stat_activity, oldest xact_start; a build cut by a"
                    + " statement_timeout (57014) was cut by the role's or database's, which nothing set for the"
                    + " session may lift behind a transaction-pooling proxy: build the index in a migration"
                    + " (OutboxSql.outbox) or exempt the role. Until it exists the dead-letter pass scans all of {} on"
                    + " each claim; next try in {} s (failure {} in a row)", index,
                    (System.nanoTime() - start) / 1_000_000, state, e.getMessage(), lockTimeout.toMillis(),
                    names.dead(), backoff.toSeconds(), failed);
        }
    }

    private boolean isClosed() {
        synchronized (monitor) {
            return closed;
        }
    }

    /** Closes {@code c}'s session at once; false when that failed, added to {@code cause} if any, else logged. */
    private boolean abort(Connection c, @Nullable Exception cause) {
        try {
            c.abort(Runnable::run);
            return true;
        } catch (SQLException e) {
            if (cause != null) {
                cause.addSuppressed(e);
            } else {
                log.warn("outbox could not abort the connection of the build of {} at shutdown: {}", index,
                        e.toString());
            }
            return false;
        }
    }

    /**
     * Cancels the drop or build in flight once a backend holding its lock on the dead table has waited for another lock
     * longer than {@code lockTimeout}; read on the holder's connection, which idles meanwhile.
     */
    private void watchWaits(LockHolder holder) {
        Statement st;
        synchronized (monitor) {
            st = ddlStatement;
        }
        if (st == null) {
            return;
        }
        try {
            double waited = holder.read(h -> {
                try (var ps = h.prepareStatement(BUILD_WAIT)) {
                    ps.setString(1, names.dead());
                    try (var rs = ps.executeQuery()) {
                        rs.next();
                        return rs.getDouble(1);
                    }
                }
            });
            if (waited * 1000 < lockTimeout.toMillis()) {
                return;
            }
            synchronized (monitor) {
                if (ddlStatement != st) {
                    return; // ended meanwhile
                }
                waitCut = true;
            }
            st.cancel(); // a JDBC cancel: a transaction-pooling proxy forwards it to the server session that runs it
        } catch (SQLException | RuntimeException e) {
            log.debug("outbox could not watch the build of {}", index, e);
        }
    }

    /**
     * Runs {@code sql} unless closed ({@link PartitionDdl#executeConcurrently}, its wait cut at {@code waitBound}, zero
     * for none), cancellable by {@link #close} and by {@link #watchWaits}; false when closed.
     */
    private boolean ddl(Connection c, Duration waitBound, String sql) throws SQLException {
        synchronized (monitor) {
            if (closed) {
                return false;
            }
            inDdl = true;
            waitCut = false;
            ddlConnection = c;
        }
        try {
            PartitionDdl.executeConcurrently(c, waitBound, sql, st -> {
                synchronized (monitor) {
                    if (closed) { // a close that came before the statement had one to cancel
                        throw new SQLException("outbox build of " + index + " stopped at shutdown", "57014");
                    }
                    ddlStatement = st;
                }
            });
        } catch (SQLException e) {
            boolean cut;
            synchronized (monitor) {
                cut = waitCut;
            }
            if (cut && "57014".equals(e.getSQLState())) {
                throw new SQLException(e.getMessage() + " (a lock timeout: the library cut " + sql + " after it waited"
                        + " " + lockTimeout.toMillis() + " ms for a lock)", "55P03", e);
            }
            throw e;
        } finally {
            synchronized (monitor) {
                inDdl = false;
                ddlStatement = null;
                ddlConnection = null;
            }
        }
        return true;
    }

    @Nullable
    private Boolean state(Connection c) throws SQLException {
        try (var ps = c.prepareStatement(STATE)) {
            ps.setString(1, index);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getBoolean(1) : null;
            }
        }
    }
}
