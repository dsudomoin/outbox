package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.DeadLetterPage;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxStatus.DeadLetterCount;
import io.github.dsudomoin.outbox.OutboxStatus.OrphanKind;
import io.github.dsudomoin.outbox.OutboxStatus;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.internal.PartitionDdl.Attachment;
import io.github.dsudomoin.outbox.internal.Sql.SqlConsumer;
import io.github.dsudomoin.outbox.internal.Sql.SqlFunction0;
import io.github.dsudomoin.outbox.internal.Sql.SqlFunction1;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OutboxAdmin} on Postgres. A bulk call acts on the letters dead when it began: it first reads the highest id
 * and the time, then claims chunks of {@link #CHUNK} letters matching it with an id up to that one and a
 * {@code failed_at} up to that time (a redriven letter dying again comes back with a new, higher id; an older message
 * dying meanwhile, with a later {@code failed_at}), in id order, each above the last chunk's,
 * {@code for update skip locked}, in a read-committed transaction of its own. A redrive chunk then tries their key
 * locks without waiting ({@link DeadLetters#tryKeyLocks}) and redrives the ready ones as the dead-letter pass does
 * ({@link DeadLetters#redrive}), so no writer waits for one of its key locks; a key found held stays skipped in the
 * call's later chunks, so its letters go back whole and in id order on a later call. Filters are bound parameters,
 * never interpolated; user text is logged with control and format characters escaped ({@link #printable}).
 * {@code restoreOrphan}, {@code recoverXidRegression} and {@code recoverStrandedShards} hold the maintenance lock for
 * their whole run, so maintenance's steps never run beside them. The reads, {@code status} and the dead-letter find
 * and count, give up on a statement after 60 s (57014); {@code status} counts the dead letters in a read of its own
 * after the rest and reports them unknown (null) when that count is the one cut.
 * {@code abandonWaiting}, {@code abandonKey} and {@code abandonRecord} move each chunk under its shard's lock, which
 * every relay step holds while it writes the retry and parked tables, so no step adds a record to a key a chunk moves
 * or makes a key it reads wait meanwhile; {@code abandonKey} moves only the rows its first chunk finds, so what a step
 * adds between its chunks stays, with a head.
 */
public final class PgOutboxAdmin implements OutboxAdmin {

    private static final Logger log = LoggerFactory.getLogger(PgOutboxAdmin.class);
    /** Letters, so key locks, per transaction of a bulk call; also the largest page. */
    static final int CHUNK = 1000;
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String QUERY_CANCELED = "57014";

    private final OutboxTransactions db;
    /**
     * Where {@link #status}, {@link #restoreOrphan}, {@link #recoverXidRegression} and {@link #recoverStrandedShards}
     * run: {@link #db}, or the owner's connections ({@link OwnerTransactions}): they read partitions directly, run DDL
     * or hold the maintenance lock.
     */
    private final OutboxTransactions ddl;
    private final TableNames names;
    private final int shards;
    private final Maintenance maintenance;
    private final Runnable wakeRelay;
    private final DeadLetters rows;
    private final XidPartitions partitions;
    private final RetryStore retries;
    private final ParkedStore parked;
    private final XidRecovery recovery;
    private final SocketBound socket;
    private final Ticks ticks;
    private final Cursors cursors;
    private final Resender resender;
    /** How long a {@link #recoverStranded} chunk may wait for a lock while it holds key locks. */
    private final Duration keyLockWait;
    final OrphanRestore orphanRestore; // for a test's seam
    private final String dead;
    /** Moves, then counts, the rows of a chunk's waiting heads: of the retry table, then of the parked one. */
    private final List<String> abandon;
    private final List<String> countAbandoned;
    private final String waitingOldest;
    /** How long a call under the maintenance lock waits for a maintenance run in progress; shorter in tests. */
    Duration maintenanceWait = Duration.ofSeconds(60);
    /** Each statement's limit in {@code status} and the dead-letter reads; shorter in tests. */
    Duration readTimeout = Duration.ofSeconds(60);
    /** How long {@code abandonWaiting} waits for a relay step holding a shard: batchTimeout + 5 s; shorter in tests. */
    Duration shardWait;
    /** For tests: runs after each bulk chunk's commit. */
    @Nullable
    SqlConsumer<Connection> afterChunk;

    /** A chunk's claim: the letters claimed, the last one's id, and how many it acted on. */
    private record Chunk(int claimed, long lastId, long done) {}

    /** One chunk in {@code tx}: up to {@link #CHUNK} letters matching {@code where} with an id above {@code after}. */
    private interface Step {
        Chunk run(Connection tx, Where where, long after) throws SQLException;
    }

    public PgOutboxAdmin(OutboxTransactions db, TableNames names, OutboxConfig config, Maintenance maintenance,
            Runnable wakeRelay) {
        this(db, db, names, config, maintenance, wakeRelay);
    }

    public PgOutboxAdmin(OutboxTransactions db, OutboxTransactions ddl, TableNames names, OutboxConfig config,
            Maintenance maintenance, Runnable wakeRelay) {
        this.db = db;
        this.ddl = ddl;
        this.names = names;
        this.shards = config.shards();
        this.maintenance = maintenance;
        this.wakeRelay = wakeRelay;
        this.rows = new DeadLetters(names, config);
        this.partitions = new XidPartitions(names, config.partitions());
        this.retries = new RetryStore(names, false); // its dead rows are kept: the abandoned ones
        this.parked = new ParkedStore(names, retries);
        this.recovery = new XidRecovery(names, config);
        this.socket = new SocketBound(config.networkTimeout());
        this.ticks = new Ticks(names, shards);
        this.cursors = new Cursors(names, shards);
        this.resender = new Resender(names, config);
        this.keyLockWait = XidPartitions.lockWait(config.partitions());
        this.orphanRestore = new OrphanRestore(names, resender, keyLockWait);
        this.dead = names.dead();
        this.shardWait = Maintenance.quiesceTimeout(config);
        // the rows of a chunk's waiting heads: each head by id (in the retry table), and every row of its key
        var ofHeads = " where shard = ? and topic = ? and (id = any(?) or key = any(?))";
        var tables = List.of(names.retry(), names.parked());
        this.abandon = tables.stream().map(t -> retries.toDead("delete from " + t + ofHeads + " returning "
                + RetryStore.COLUMNS + ", attempts", "attempts", "?")).toList();
        this.countAbandoned = tables.stream().map(t -> "select count(*) from " + t + ofHeads).toList();
        // a relay of an earlier release set waiting_since by its JVM clock: it may be ahead of the database's
        this.waitingOldest = "select greatest(coalesce(extract(epoch from now() - min(waiting_since)), 0), 0)::float8"
                + " from " + names.retry() + " where waiting_since is not null";
    }

    @Override
    public OutboxStatus status() {
        requireOwnTransactions();
        try (var conn = ddl.newConnection(); var bound = socket.on(conn)) {
            // in a transaction of its own: its lock on the tail lasts until the commit, and a swap's hold waits 10 ms at
            // most for it, so the reads after it would make a swap running meanwhile miss
            boolean tailOverflow = read(conn, c -> PartitionDdl.attachment(c, names.base(), names.tail())
                    != Attachment.MISSING && PartitionDdl.hasRows(c, names.tail()));
            var s = read(conn, c -> new OutboxStatus(
                    queryBoolean(c, "select exists (select 1 from " + names.halt() + ")"),
                    maintenance.lagSeconds(c),
                    retries.size(c),
                    parked.size(c),
                    RetryStore.count(c, "select count(*) from " + names.retry() + " where waiting_since is not null"),
                    queryDouble(c, waitingOldest),
                    null,
                    partitions.orphans(c).stream().map(o -> new OutboxStatus.Orphan(o.table(), o.setAside()
                            ? OrphanKind.SET_ASIDE : o.ours() ? OrphanKind.DETACHED_BY_LIBRARY : OrphanKind.FOREIGN))
                            .toList(),
                    tailOverflow,
                    partitions.premadeAhead(c),
                    partitions.xidsLeft(c),
                    maintenance.secondsSinceSuccess(c),
                    shards,
                    instances(c)));
            // a full scan of the dead table, kept forever by default: on its own, so a count cut by readTimeout
            // leaves the rest of the status standing
            List<DeadLetterCount> deadLetters;
            try {
                deadLetters = read(conn, this::deadLetterCounts);
            } catch (SQLException e) {
                if (!QUERY_CANCELED.equals(e.getSQLState())) {
                    throw e;
                }
                log.warn("outbox admin status: the dead-letter counts took longer than {}, reported unknown; a"
                        + " retention (outbox.deadLetters.retention) or a discard keeps the dead table small",
                        readTimeout);
                deadLetters = null;
            }
            return new OutboxStatus(s.halted(), s.lagSeconds(), s.retrySize(), s.parkedSize(), s.waitingSize(),
                    s.waitingOldestSeconds(), deadLetters, s.orphans(), s.tailOverflow(), s.premade(), s.xidsLeft(),
                    s.maintenanceSinceSuccessSeconds(), s.shards(), s.instances());
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }

    private List<OutboxStatus.Instance> instances(Connection c) throws SQLException {
        var instances = new ArrayList<OutboxStatus.Instance>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select instance_id, shards, last_seen,"
                + " hook_active, relay, " + ContractGuard.REPORT + ", " + ContractGuard.ROUTES + ","
                + " coalesce(expires_at > now(), false) from " + names.instance() + " order by instance_id")) {
            while (rs.next()) {
                instances.add(new OutboxStatus.Instance(rs.getString(1), rs.getInt(2),
                        rs.getObject(3, OffsetDateTime.class).toInstant(), rs.getBoolean(4), rs.getBoolean(5),
                        rs.getBoolean(10), ContractGuard.report(rs, 6), ContractGuard.routes(rs, 9)));
            }
        }
        return instances;
    }

    @Override
    public DeadLetterPage findDeadLetters(DeadLetterFilter filter, @Nullable Long afterId, int limit) {
        requireOwnTransactions();
        if (limit < 1 || limit > CHUNK) {
            throw new IllegalArgumentException("limit must be within 1.." + CHUNK + ": " + limit);
        }
        var where = Where.of(filter);
        try (var conn = db.newConnection(); var bound = socket.on(conn)) {
            var letters = read(conn, c -> {
                try (var ps = c.prepareStatement("select " + DeadLetters.COLUMNS + " from " + dead + " where "
                        + where.sql() + " and id > ? order by id limit ?")) {
                    int next = where.bind(ps);
                    ps.setLong(next, afterId == null ? Long.MIN_VALUE : afterId);
                    ps.setInt(next + 1, limit);
                    var page = new ArrayList<DeadLetter>();
                    try (var rs = ps.executeQuery()) {
                        while (rs.next()) {
                            page.add(DeadLetters.read(rs).letter());
                        }
                    }
                    return page;
                }
            });
            return new DeadLetterPage(letters, letters.size() < limit ? null : letters.get(letters.size() - 1).id());
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }

    @Override
    public long countDeadLetters(DeadLetterFilter filter) {
        requireOwnTransactions();
        var where = Where.of(filter);
        try (var conn = db.newConnection(); var bound = socket.on(conn)) {
            return read(conn, c -> {
                try (var ps = c.prepareStatement("select count(*) from " + dead + " where " + where.sql())) {
                    where.bind(ps);
                    try (var rs = ps.executeQuery()) {
                        rs.next();
                        return rs.getLong(1);
                    }
                }
            });
        } catch (SQLException e) {
            throw db.translate(e);
        }
    }

    @Override
    public long redriveDeadLetters(Collection<Long> ids) {
        requireOwnTransactions();
        return bulk("redriveDeadLetters(" + logged(ids) + ")", Where.ids(ids), true, redrive(new HashSet<>()));
    }

    @Override
    public long redriveDeadLetters(DeadLetterFilter filter) {
        requireOwnTransactions();
        return bulk("redriveDeadLetters(" + filter + ")", Where.of(nonEmpty(filter)), true, redrive(new HashSet<>()));
    }

    @Override
    public long discardDeadLetters(Collection<Long> ids) {
        requireOwnTransactions();
        return bulk("discardDeadLetters(" + logged(ids) + ")", Where.ids(ids), false, this::discard);
    }

    @Override
    public long discardDeadLetters(DeadLetterFilter filter) {
        requireOwnTransactions();
        return bulk("discardDeadLetters(" + filter + ")", Where.of(nonEmpty(filter)), false, this::discard);
    }

    /**
     * Under the maintenance lock ({@link #underMaintenanceLock}), so the tail's lower bound, which tells an orphan from
     * a range the extension made ahead of its swap, holds still: {@code table} must be one
     * {@link XidPartitions#orphans} lists, then {@link OrphanRestore} validates it again and moves its rows.
     */
    @Override
    public long restoreOrphan(String table, long fromXid) {
        requireOwnTransactions();
        var call = printable("restoreOrphan(" + table + ", " + fromXid + ")");
        long moved;
        var restoring = new AtomicBoolean();
        try (var c = ddl.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(true);
            moved = underMaintenanceLock(c, call, () -> {
                var name = names.unqualified(table);
                if (partitions.orphans(c).stream().noneMatch(o -> names.unqualified(o.table()).equals(name))) {
                    throw new IllegalArgumentException(table + " is not an orphan of the outbox " + names.base()
                            + " (OutboxAdmin.status() lists them): a table " + names.partitionPrefix() + "<digits>"
                            + " below the partition tail's lower bound, or " + names.setAsidePrefix() + "<digits>, in"
                            + " its schema and in no partition tree");
                }
                restoring.set(true);
                return orphanRestore.restore(c, table, fromXid, PartitionDdl.DETACH_LOCK_TIMEOUT);
            });
        } catch (SQLException | RuntimeException e) {
            // refused, nothing changed: OrphanRestore validates before any SQL that names the table
            boolean refused = e instanceof IllegalArgumentException
                    || e instanceof IllegalStateException && !restoring.get();
            if (!refused) {
                log.warn("outbox admin {} on {} failed; rows moved so far stay moved, and calling again goes on: {}",
                        call, names.base(), printable(e.toString()));
            }
            if (restoring.get() && !(e instanceof IllegalArgumentException)) {
                wakeRelay.run(); // for the chunks it may have moved before it failed
            }
            throw e instanceof SQLException s ? db.translate(s) : (RuntimeException) e;
        }
        log.info("outbox admin {} on {} returned {}", call, names.base(), moved);
        if (moved > 0) {
            wakeRelay.run();
        }
        return moved;
    }

    /**
     * Under the maintenance lock ({@link #underMaintenanceLock}), the recovery maintenance runs
     * ({@link XidRecovery#recover}), whatever {@code outbox.recovery} says. Once it committed the call no longer fails:
     * the INFO line, then this instance's halt refreshed ({@link Maintenance#checkHalt}), so the relay woken resumes
     * at once (other instances resume at their next maintenance run); a failure of that refresh, or of releasing the
     * lock and the session setting, is a WARN, and the relay is woken whatever happens. A commit that fails may still
     * have gone through on the server ({@link #recoveredAfterAll}): when a read on a new connection finds the recovery
     * done, the call goes on as committed and returns -1, the count unknown; otherwise it fails with "outcome unknown".
     */
    @Override
    public long recoverXidRegression() {
        requireOwnTransactions();
        var call = "recoverXidRegression()";
        var recovering = new AtomicBoolean();
        var committing = new AtomicBoolean();
        var sent = new AtomicLong(-1); // the rows re-sent, once the recovery committed
        Exception cleanup = null;
        try (var c = ddl.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(true);
            underMaintenanceLock(c, call, () -> {
                var check = recovery.check(c);
                if (check.current() >= check.tickXmax() && check.recorded() == null) {
                    throw new IllegalStateException("no xid regression to recover: the xid counter of " + names.base()
                            + " (" + check.current() + ") is at or above every tick's xmax (" + check.tickXmax()
                            + ") and " + names.halt() + " holds no row");
                }
                recovering.set(true);
                sent.set(recovery.recover(c, committing));
                return null;
            });
        } catch (SQLException | RuntimeException e) {
            if (sent.get() < 0 && committing.get() && !recoveredAfterAll(e)) {
                log.warn("outbox admin {} on {} failed at its commit, outcome unknown: call again; a repeat is refused"
                        + " with \"no xid regression to recover\" if it committed: {}", call, names.base(),
                        printable(e.toString()));
                wakeRelay.run(); // in case it committed
                throw new IllegalStateException("the xid-regression recovery of " + names.base() + " failed at its"
                        + " commit, outcome unknown, call again: a repeat is refused with \"no xid regression to"
                        + " recover\" if it committed", e);
            }
            if (sent.get() < 0 && !committing.get()) {
                // refused, nothing changed: maintenance running, no regression, or a transaction-pooled connection
                boolean refused = e instanceof IllegalStateException && !recovering.get();
                if (!refused) {
                    log.warn("outbox admin {} on {} failed, nothing re-sent: {}", call, names.base(),
                            printable(e.toString()));
                }
                throw e instanceof SQLException s ? db.translate(s) : (RuntimeException) e;
            }
            cleanup = e; // after the commit: releasing the maintenance lock, the session setting or the connection
        }
        try {
            if (sent.get() < 0) {
                log.info("outbox admin {} on {} returned -1: its commit failed ({}), but a read on a new connection"
                        + " found the recovery done; how many rows it re-sent is unknown", call, names.base(),
                        printable(cleanup.toString()));
            } else {
                log.info("outbox admin {} on {} returned {}, the rows re-sent", call, names.base(), sent.get());
            }
            if (cleanup != null && sent.get() >= 0) {
                log.warn("outbox admin {} on {}: the recovery committed, then releasing the maintenance lock,"
                        + " giving idle_session_timeout back or closing the connection failed: {}", call, names.base(),
                        printable(cleanup.toString()));
            }
            try {
                maintenance.checkHalt();
            } catch (RuntimeException e) {
                log.warn("outbox admin {} on {}: the recovery committed, but reading the halt again failed, so this"
                        + " instance's relay resumes at its next maintenance run: {}", call, names.base(),
                        printable(e.toString()));
            }
        } finally {
            wakeRelay.run();
        }
        return sent.get();
    }

    /**
     * After the recovery's commit failed: whether a read on a new connection finds no regression and no halt row, so
     * the commit went through (or another recovery ran since). A {@code lock_timeout} of 5 s bounds it: a session
     * whose reply was lost may still hold the recovery's locks. False, its failure added to {@code failure}, when the
     * read fails.
     */
    private boolean recoveredAfterAll(Exception failure) {
        try (var c = ddl.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(false);
            try (var st = c.createStatement()) {
                st.execute("set local lock_timeout = 5000");
                var check = recovery.check(c);
                return check.current() >= check.tickXmax() && check.recorded() == null;
            } finally {
                c.rollback();
                c.setAutoCommit(true);
            }
        } catch (SQLException | RuntimeException e) {
            failure.addSuppressed(e);
            return false;
        }
    }

    /**
     * Under the maintenance lock ({@link #underMaintenanceLock}), so no recovery or other such call goes on beside it,
     * shard by shard ({@link #recoverStranded}) as {@link Maintenance#strandedShards} finds them.
     */
    @Override
    public long recoverStrandedShards() {
        requireOwnTransactions();
        var call = "recoverStrandedShards()";
        var recovering = new AtomicBoolean();
        var moved = new AtomicLong();
        var stranded = new ArrayList<Integer>();
        try (var c = ddl.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(true);
            underMaintenanceLock(c, call, () -> {
                if (queryBoolean(c, "select exists (select 1 from " + names.halt() + ")")) {
                    // its re-send goes by xids, which moved rows would not keep, and it rebuilds the cursors anyway
                    throw new IllegalStateException("an xid regression halt is recorded in " + names.halt()
                            + ": recover the xid regression first (OutboxAdmin.recoverXidRegression()); it rebuilds"
                            + " every cursor, so no shard stays stranded");
                }
                var missing = maintenance.strandedShards(c);
                stranded.addAll(missing.keySet());
                // the pins of shards repaired since, as by a previous release, which kept none
                try (var ps = c.prepareStatement("delete from " + names.stranded() + " where shard <> all(?)")) {
                    ps.setArray(1, c.createArrayOf("int4", stranded.toArray()));
                    ps.executeUpdate();
                }
                if (stranded.isEmpty()) {
                    throw new IllegalStateException("no stranded shards in " + names.base() + ": every shard's"
                            + " effective tick exists");
                }
                recovering.set(true);
                for (var shard : stranded) {
                    recoverStranded(c, shard, missing.get(shard), moved);
                }
                return null;
            });
        } catch (SQLException | RuntimeException e) {
            // refused, nothing changed: maintenance running, a halt, no shard stranded or a transaction-pooled connection
            if (recovering.get()) {
                log.warn("outbox admin {} on {} failed after {} rows re-sent, which stay re-sent; calling again goes"
                        + " on: {}", call, names.base(), moved.get(), printable(e.toString()));
                wakeRelay.run(); // for the shards it repaired before it failed
            }
            throw e instanceof SQLException s ? db.translate(s) : (RuntimeException) e;
        }
        log.info("outbox admin {} on {} returned {}, the rows re-sent, for shards {}", call, names.base(), moved.get(),
                stranded);
        wakeRelay.run();
        return moved.get();
    }

    /**
     * Repairs one stranded shard: its rows the tick L covers are moved back into the outbox, then its cursor
     * goes to L, due as its retry and parked tables say, with no batch in progress. When only its batch's target is
     * gone, those its effective tick sees stay: they went through the relay ({@link Cursors}). L is read first, so what its snapshot covers had committed
     * before the read of the rows, which so finds all of it. What it does not cover is the relay's from the new cursor
     * on and is not touched: a transaction committing at any point of this, the re-sent rows included, whose xids L
     * cannot cover, as they were taken after it. The rows move rather than copy, the insert and the delete in one
     * transaction, so a call that fails midway repeats none of them: until the cursor moves, no step delivers any.
     * Those of a key another transaction holds stay, in this pass's later chunks too, so that each key's rows go in id
     * order; each pass after the first waits for one such key, so a key held almost all the time gets through too,
     * and for at most {@link XidPartitions#lockWait}, only that key's writers waiting meanwhile. After
     * {@link OrphanRestore#HELD_KEYS_WAITED} waits, or a wait that ran out, the call gives up. L is the latest tick
     * when the first call begins, pinned ({@link #pinnedTick}) until the cursor moves to it: a call repeated after one
     * that failed midway goes on from the same L, so the rows that call moved, which L does not cover, stay the
     * relay's from L on, delivered ahead of the rows the later call moves, and are not moved again behind their key's
     * older rows.
     *
     * <p>ponytail: the ids and xids of a shard's rows sit in memory for the call, about 40 bytes a row, 40 MB per
     * million; the outbox has no index on id, so reading them in id order per chunk would scan the shard each time.
     */
    private void recoverStranded(Connection c, int shard, long missing, AtomicLong moved) throws SQLException {
        var latest = pinnedTick(c, shard, missing);
        var pending = coveredRows(c, shard, latest, cursors.latest(c, shard).tick());
        try {
            Integer waitFor = null;
            for (int waits = 0; !pending.isEmpty(); waits++) {
                var left = new ArrayList<CoveredRow>();
                var held = new LinkedHashSet<Integer>();
                for (int from = 0; from < pending.size(); from += CHUNK) {
                    var chunk = pending.subList(from, Math.min(from + CHUNK, pending.size()));
                    var firstWait = from == 0 ? waitFor : null;
                    moved.addAndGet(inTx(c, tx -> moveChunk(tx, shard, chunk, firstWait, held, left)));
                    if (afterChunk != null) {
                        afterChunk.accept(c);
                    }
                }
                if (!left.isEmpty() && waits == OrphanRestore.HELD_KEYS_WAITED) {
                    throw new IllegalStateException("shard " + shard + " of " + names.base() + " is busy, retry: "
                            + left.size() + " of its rows are of keys open transactions held all along");
                }
                pending = left;
                waitFor = held.isEmpty() ? null : held.iterator().next();
            }
            inTx(c, tx -> {
                lockShard(tx, shard);
                cursors.append(tx, new Cursors.Cursor(shard, latest.id(), null, 0, null, 0, retries.minDue(tx, shard),
                        parked.minDue(tx, shard)));
                try (var ps = tx.prepareStatement("delete from " + names.stranded() + " where shard = ?")) {
                    ps.setShort(1, (short) shard);
                    ps.executeUpdate();
                }
                return null;
            });
        } catch (Throwable e) { // an inTx rolled back, and left the connection out of autocommit
            try {
                c.setAutoCommit(true);
            } catch (SQLException cleanup) { // a terminated session
                e.addSuppressed(cleanup);
            }
            if (e instanceof SQLException x && LOCK_NOT_AVAILABLE.equals(x.getSQLState())) {
                throw new IllegalStateException("shard " + shard + " of " + names.base() + " is busy, retry: a key of"
                        + " its rows or a table they go to stayed locked for longer than "
                        + keyLockWait.toMillis() / 1e3 + " s", e);
            }
            throw e;
        }
    }

    /**
     * The tick the repair of {@code shard} goes to, committed on its own before any row moves: the one pinned in
     * {@code <base>_stranded} while the shard is still stranded at {@code missing} with the same latest cursor row and
     * that tick exists, else the latest tick, pinned now.
     */
    private Ticks.Tick pinnedTick(Connection c, int shard, long missing) throws SQLException {
        try (var ps = c.prepareStatement("insert into " + names.stranded() + " as p (shard, stranded_tick_id,"
                + " cursor_seq, tick_id) select ?, ?, (select max(seq) from " + names.cursor() + " where shard = ?),"
                + " (select max(id) from " + names.tick() + ") on conflict (shard) do update set"
                + " stranded_tick_id = excluded.stranded_tick_id, cursor_seq = excluded.cursor_seq,"
                + " tick_id = excluded.tick_id where p.stranded_tick_id <> excluded.stranded_tick_id"
                + " or p.cursor_seq <> excluded.cursor_seq"
                + " or not exists (select 1 from " + names.tick() + " t where t.id = p.tick_id)")) {
            ps.setShort(1, (short) shard);
            ps.setLong(2, missing);
            ps.setShort(3, (short) shard);
            ps.executeUpdate();
        }
        try (var ps = c.prepareStatement("select tick_id from " + names.stranded() + " where shard = ?")) {
            ps.setShort(1, (short) shard);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return ticks.get(c, rs.getLong(1));
            }
        }
    }

    /** A row of a stranded shard, as much of it as finds it again: the outbox has an index on (shard, xid) alone. */
    private record CoveredRow(long id, long xid) {}

    /** The rows of {@code shard} that {@code tick}'s snapshot covers and {@code relayed}'s, if any, does not, by id. */
    private List<CoveredRow> coveredRows(Connection c, int shard, Ticks.Tick tick, @Nullable Ticks.Tick relayed)
            throws SQLException {
        try (var ps = c.prepareStatement("select id, xid::text from " + names.base() + " where shard = ?"
                + " and pg_visible_in_snapshot(xid, ?::pg_snapshot)"
                + (relayed == null ? "" : " and not pg_visible_in_snapshot(xid, ?::pg_snapshot)") + " order by id")) {
            ps.setShort(1, (short) shard);
            ps.setString(2, tick.snapshot().text());
            if (relayed != null) {
                ps.setString(3, relayed.snapshot().text());
            }
            var rows = new ArrayList<CoveredRow>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new CoveredRow(rs.getLong(1), Long.parseLong(rs.getString(2))));
                }
            }
            return rows;
        }
    }

    /**
     * One transaction under the shard's lock: first, with {@code waitFor}, that key's lock, waited for; then the rows
     * of {@code chunk} not in the retry, parked or dead tables, each of which keeps the id and message id of its outbox
     * row, moved ({@link Resender}), but those of a key another transaction holds, tried without waiting, which go to
     * {@code left}, their keys to {@code held}. Returns the rows moved.
     */
    private long moveChunk(Connection tx, int shard, List<CoveredRow> chunk, @Nullable Integer waitFor,
            Set<Integer> held, List<CoveredRow> left) throws SQLException {
        lockShard(tx, shard);
        // from here on, writers may queue behind this chunk's key locks: wait no longer than the swap may
        execute(tx, "set local lock_timeout = " + keyLockWait.toMillis());
        if (waitFor != null) {
            try (var ps = tx.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
                ps.setInt(1, names.keyNamespace());
                ps.setInt(2, waitFor);
                ps.execute();
            }
        }
        var xids = tx.createArrayOf("text", chunk.stream().map(r -> Long.toString(r.xid())).distinct().toArray());
        var rows = new ArrayList<Resender.Source>(chunk.size());
        var xidsOf = new ArrayList<Long>(chunk.size());
        try (var ps = tx.prepareStatement("select o.id, o.xid::text, o.topic, o.key from " + names.base() + " o"
                + " where o.shard = ? and o.xid = any(?::text[]::xid8[]) and o.id = any(?)"
                + " and not exists (select 1 from " + names.retry() + " r where r.id = o.id"
                + " and r.message_id = o.message_id)"
                + " and not exists (select 1 from " + names.parked() + " p where p.id = o.id"
                + " and p.message_id = o.message_id)"
                + " and not exists (select 1 from " + dead + " d where d.id = o.id and d.message_id = o.message_id)"
                + " order by o.id")) {
            ps.setShort(1, (short) shard);
            ps.setArray(2, xids);
            ps.setArray(3, tx.createArrayOf("int8", chunk.stream().map(CoveredRow::id).toArray()));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) { // the payloads and headers stay in the database: insertFrom copies them
                    rows.add(new Resender.Source(rs.getLong(1), rs.getString(3), rs.getBytes(4)));
                    xidsOf.add(Long.parseLong(rs.getString(2)));
                }
            }
        }
        var hashes = rows.stream().map(r -> r.key() == null ? null : Hashing.keyHash(r.topic(), r.key())).toList();
        held.addAll(resender.tryKeyLocks(tx, hashes.stream().filter(h -> h != null && !held.contains(h)).toList()));
        var ready = new ArrayList<Resender.Source>(rows.size());
        var readyIds = new ArrayList<Long>(rows.size());
        var readyXids = new ArrayList<String>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            if (hashes.get(i) != null && held.contains(hashes.get(i))) {
                left.add(new CoveredRow(rows.get(i).id(), xidsOf.get(i)));
            } else {
                ready.add(rows.get(i));
                readyIds.add(rows.get(i).id());
                readyXids.add(Long.toString(xidsOf.get(i)));
            }
        }
        var xidList = String.join(",", readyXids.stream().distinct().toList());
        // the shard and xids find the rows by the (shard, xid) index: the outbox has none on id. Numbers only, inlined
        resender.insertFrom(tx, "(select id, message_id, topic, key, payload, headers from " + names.base()
                + " where shard = " + shard + " and xid = any('{" + xidList + "}'::xid8[]))", ready);
        try (var ps = tx.prepareStatement("delete from " + names.base() + " where shard = ?"
                + " and xid = any(?::text[]::xid8[]) and id = any(?)")) {
            ps.setShort(1, (short) shard);
            ps.setArray(2, tx.createArrayOf("text", readyXids.stream().distinct().toArray()));
            ps.setArray(3, tx.createArrayOf("int8", readyIds.toArray()));
            ps.executeUpdate();
        }
        return ready.size();
    }

    /**
     * The shards with a head of {@code topic} (and {@code key}) waiting when the call began, each chunk after chunk
     * ({@link #abandonChunk}) until one finds fewer than {@link #CHUNK} heads; one INFO line, or a WARN on a failure.
     */
    @Override
    public Abandoned abandonWaiting(String topic, @Nullable byte[] key, String reason, boolean dryRun) {
        requireOwnTransactions();
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("name the topic whose waiting records to abandon");
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("give a reason to abandon waiting records: it goes into the log line"
                    + " of the call and their dead letters' error");
        }
        var call = printable("abandonWaiting(" + topic + ", " + (key == null ? null : HexFormat.of().formatHex(key))
                + ", " + reason + ", " + (dryRun ? "dry run" : "for real") + ")");
        var error = "abandoned: " + reason;
        long keys = 0;
        long records = 0;
        try (var c = db.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(true);
            for (int shard : waitingShards(c, topic, key)) {
                long after = Long.MIN_VALUE;
                while (true) {
                    long above = after;
                    var chunk = dryRun ? read(c, tx -> abandonChunk(tx, shard, topic, key, above, true, error))
                            : inTx(c, tx -> abandonChunk(tx, shard, topic, key, above, false, error));
                    keys += chunk.claimed();
                    records += chunk.done();
                    if (chunk.claimed() < CHUNK) {
                        break;
                    }
                    after = chunk.lastId();
                }
                if (!dryRun) {
                    inTx(c, tx -> refreshDueTimes(tx, shard));
                }
            }
        } catch (SQLException | RuntimeException e) {
            log.warn("outbox admin {} on {} failed after {} keys ({} records) {}; calling again goes on: {}", call,
                    names.base(), keys, records, dryRun ? "counted" : "moved, which stay dead letters",
                    printable(e.toString()));
            throw e instanceof SQLException s ? db.translate(s) : (RuntimeException) e;
        }
        var abandoned = new Abandoned(keys, records);
        log.info("outbox admin {} on {} returned {}", call, names.base(), abandoned);
        return abandoned;
    }

    @Override
    public Abandoned abandonKey(String topic, byte[] key, String reason, boolean dryRun) {
        if (key == null) {
            throw new IllegalArgumentException("name the key whose records to abandon; an unkeyed record goes by its"
                    + " message id");
        }
        return abandonRows(topic, " and key = ?", key, reason, dryRun, "abandonKey(" + topic + ", "
                + HexFormat.of().formatHex(key));
    }

    @Override
    public Abandoned abandonRecord(String topic, UUID messageId, String reason, boolean dryRun) {
        if (messageId == null) {
            throw new IllegalArgumentException("name the message id of the unkeyed record to abandon");
        }
        return abandonRows(topic, " and message_id = ?", messageId, reason, dryRun, "abandonRecord(" + topic + ", "
                + messageId);
    }

    /**
     * The rows of {@code topic} matching {@code condition} ({@code value} its parameter) in the retry and parked
     * tables, shard by shard, chunk after chunk ({@link #abandonRowsChunk}); one INFO line, or a WARN on a failure.
     */
    private Abandoned abandonRows(String topic, String condition, Object value, String reason, boolean dryRun,
            String named) {
        requireOwnTransactions();
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("name the topic of the records to abandon");
        }
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("give a reason to abandon records: it goes into the log line of the"
                    + " call and their dead letters' error");
        }
        var call = printable(named + ", " + reason + ", " + (dryRun ? "dry run" : "for real") + ")");
        var error = "abandoned: " + reason;
        long records = 0;
        try (var c = db.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(true);
            for (int shard : read(c, tx -> shardsOf(tx, topic, condition, value))) {
                if (dryRun) {
                    records += read(c, tx -> countRows(tx, shard, topic, condition, value));
                    continue;
                }
                var upTo = new AtomicReference<Long>();
                int moved;
                do {
                    moved = inTx(c, tx -> abandonRowsChunk(tx, shard, topic, condition, value, upTo, error));
                    records += moved;
                    if (afterChunk != null) {
                        afterChunk.accept(c);
                    }
                } while (moved == CHUNK);
                inTx(c, tx -> refreshDueTimes(tx, shard));
            }
        } catch (SQLException | RuntimeException e) {
            log.warn("outbox admin {} on {} failed after {} records {}; calling again goes on: {}", call, names.base(),
                    records, dryRun ? "counted" : "moved, which stay dead letters", printable(e.toString()));
            throw e instanceof SQLException s ? db.translate(s) : (RuntimeException) e;
        }
        var abandoned = new Abandoned(records > 0 ? 1 : 0, records);
        log.info("outbox admin {} on {} returned {}", call, names.base(), abandoned);
        return abandoned;
    }

    /**
     * The shard's due times as the tables hold them after an abandon, under its lock, as a relay step writes them: rows
     * gone would otherwise leave them in the past, and the shard stepped for nothing until its next full check.
     */
    private Void refreshDueTimes(Connection tx, int shard) throws SQLException {
        lockShard(tx, shard);
        var cursor = cursors.latest(tx, shard).cursor();
        var refreshed = cursor.withRetryDueAt(retries.minDue(tx, shard)).withParkedDueAt(parked.minDue(tx, shard));
        if (!refreshed.equals(cursor)) {
            cursors.append(tx, refreshed);
        }
        return null;
    }

    /** The shards with such rows; refuses a record by message id that has a key. */
    private List<Integer> shardsOf(Connection tx, String topic, String condition, Object value) throws SQLException {
        var shards = new ArrayList<Integer>();
        var selects = new ArrayList<String>();
        for (var table : List.of(names.retry(), names.parked())) {
            selects.add("select shard, key from " + table + " where topic = ?" + condition);
        }
        try (var ps = tx.prepareStatement(String.join(" union all ", selects))) {
            ps.setString(1, topic);
            ps.setObject(2, value);
            ps.setString(3, topic);
            ps.setObject(4, value);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (value instanceof UUID && rs.getBytes(2) != null) {
                        throw new IllegalArgumentException("message " + value + " has a key: it cannot go without the"
                                + " rest of its key, abandon the key");
                    }
                    if (!shards.contains(rs.getInt(1))) {
                        shards.add(rs.getInt(1));
                    }
                }
            }
        }
        return shards;
    }

    private long countRows(Connection tx, int shard, String topic, String condition, Object value)
            throws SQLException {
        long rows = 0;
        for (var table : List.of(names.retry(), names.parked())) {
            try (var ps = tx.prepareStatement("select count(*) from " + table + " where shard = ? and topic = ?"
                    + condition)) {
                ps.setShort(1, (short) shard);
                ps.setString(2, topic);
                ps.setObject(3, value);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    rows += rs.getLong(1);
                }
            }
        }
        return rows;
    }

    /**
     * Up to {@link #CHUNK} such rows moved to the dead table with {@code error}, read after the shard's lock
     * ({@link #lockShard}), so after any relay step that held it. The retry table first and its newest rows first: a
     * call that stops between chunks leaves the key's retry rows with their head, and its parked rows, which block
     * nothing. Only rows up to {@code bound}, the highest id of such rows the first chunk finds: a relay step between
     * chunks reads the key's new records as followers of its head, and they stay. The chunk that leaves the key no
     * retry row up to the bound, and the last chunk, make the lowest one the key's head, due now on the cursor too, or
     * nothing would ever send them. A writer's transaction committing a record of the key with an id below the bound
     * during the call still sees it go.
     */
    private int abandonRowsChunk(Connection tx, int shard, String topic, String condition, Object value,
            AtomicReference<Long> bound, String error) throws SQLException {
        lockShard(tx, shard);
        if (bound.get() == null) {
            try (var ps = tx.prepareStatement("select coalesce(greatest((select max(id) from " + names.retry()
                    + " where shard = ? and topic = ?" + condition + "), (select max(id) from " + names.parked()
                    + " where shard = ? and topic = ?" + condition + ")), " + Long.MIN_VALUE + ")")) {
                ps.setShort(1, (short) shard);
                ps.setString(2, topic);
                ps.setObject(3, value);
                ps.setShort(4, (short) shard);
                ps.setString(5, topic);
                ps.setObject(6, value);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    bound.set(rs.getLong(1));
                }
            }
        }
        int moved = 0;
        for (var table : List.of(names.retry(), names.parked())) {
            if (moved == CHUNK) {
                break;
            }
            var delete = "delete from " + table + " where id in (select id from " + table + " where shard = ? and"
                    + " topic = ?" + condition + " and id <= ? order by id desc limit ?) returning "
                    + RetryStore.COLUMNS + ", attempts";
            try (var ps = tx.prepareStatement(retries.toDead(delete, "attempts", "?"))) {
                ps.setShort(1, (short) shard);
                ps.setString(2, topic);
                ps.setObject(3, value);
                ps.setLong(4, bound.get());
                ps.setInt(5, CHUNK - moved);
                ps.setString(6, RetryStore.errorText(error));
                moved += ps.executeUpdate();
            }
        }
        if (value instanceof byte[]) {
            // in the chunk that took the key's last retry row up to the bound, short or full, or a call cut before the
            // next one leaves the followers a relay step added without a head; "is null": a record failing in such a
            // step, with no retry row of the key before it, is a head already
            try (var ps = tx.prepareStatement("update " + names.retry() + " set next_attempt_at = ? where id ="
                    + " (select id from " + names.retry() + " where shard = ? and topic = ?" + condition + " order by id"
                    + " limit 1) and (? or id > ?) and next_attempt_at is null")) {
                ps.setObject(1, RetryStore.timestamp(Instant.now()));
                ps.setShort(2, (short) shard);
                ps.setString(3, topic);
                ps.setObject(4, value);
                ps.setBoolean(5, moved < CHUNK);
                ps.setLong(6, bound.get());
                if (ps.executeUpdate() > 0) {
                    refreshDueTimes(tx, shard);
                }
            }
        }
        return moved;
    }

    private List<Integer> waitingShards(Connection c, String topic, @Nullable byte[] key) throws SQLException {
        try (var ps = c.prepareStatement("select distinct shard from " + names.retry() + " where topic = ? and"
                + " waiting_since is not null" + (key == null ? "" : " and key = ?") + " order by shard")) {
            ps.setString(1, topic);
            if (key != null) {
                ps.setBytes(2, key);
            }
            var shards = new ArrayList<Integer>();
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    shards.add(rs.getInt(1));
                }
            }
            return shards;
        }
    }

    /**
     * Up to {@link #CHUNK} heads of {@code shard} waiting with an id above {@code after}, read after the shard's lock
     * ({@link #lockShard}), so after any relay step that held it: each with every row of its key in the retry and
     * parked tables (an unkeyed head alone) moved to the dead table with {@code error}; with {@code dryRun}, counted,
     * without the lock.
     */
    private Chunk abandonChunk(Connection tx, int shard, String topic, @Nullable byte[] key, long after,
            boolean dryRun, String error) throws SQLException {
        if (!dryRun) {
            lockShard(tx, shard);
        }
        var ids = new ArrayList<Long>();
        var keys = new ArrayList<byte[]>();
        try (var ps = tx.prepareStatement("select id, key from " + names.retry() + " where shard = ? and topic = ?"
                + " and waiting_since is not null" + (key == null ? "" : " and key = ?") + " and id > ? order by id"
                + " limit " + CHUNK)) {
            int i = 1;
            ps.setShort(i++, (short) shard);
            ps.setString(i++, topic);
            if (key != null) {
                ps.setBytes(i++, key);
            }
            ps.setLong(i, after);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                    var headKey = rs.getBytes(2);
                    if (headKey != null) {
                        keys.add(headKey);
                    }
                }
            }
        }
        if (ids.isEmpty()) {
            return new Chunk(0, after, 0);
        }
        var idArray = tx.createArrayOf("bigint", ids.toArray());
        var keyArray = tx.createArrayOf("bytea", keys.toArray(byte[][]::new));
        long rows = 0;
        for (var sql : dryRun ? countAbandoned : abandon) {
            try (var ps = tx.prepareStatement(sql)) {
                ps.setShort(1, (short) shard);
                ps.setString(2, topic);
                ps.setArray(3, idArray);
                ps.setArray(4, keyArray);
                if (dryRun) {
                    try (var rs = ps.executeQuery()) {
                        rs.next();
                        rows += rs.getLong(1);
                    }
                } else {
                    ps.setString(5, RetryStore.errorText(error));
                    rows += ps.executeUpdate();
                }
            }
        }
        return new Chunk(ids.size(), ids.get(ids.size() - 1), rows);
    }

    /**
     * The shard's lock, the one every relay step holds ({@code pg_advisory_xact_lock(internalNs, LOCK_SHARD_BASE +
     * shard)}), waited for at most {@link #shardWait}; held until the transaction ends.
     */
    private void lockShard(Connection tx, int shard) throws SQLException {
        execute(tx, "set local lock_timeout = " + shardWait.toMillis());
        try (var ps = tx.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_SHARD_BASE + shard);
            SocketBound.atLeast(tx, shardWait, ps::execute);
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                throw e;
            }
            throw new IllegalStateException("shard " + shard + " of " + names.base() + " is busy, retry: a relay step"
                    + " held its lock pg_advisory_xact_lock(" + names.internalNamespace() + ", "
                    + (TableNames.LOCK_SHARD_BASE + shard) + ") for longer than " + shardWait.toMillis() / 1e3 + " s",
                    e);
        }
    }

    /**
     * {@code body} on {@code c} under the maintenance lock, waited for at most {@link #maintenanceWait}, so no
     * maintenance run goes on beside it. The session gets {@code idle_session_timeout = 60s} first, as
     * {@link Maintenance} does, so Postgres ends the session of a frozen holder and frees the lock; the session's own
     * value given back, and the lock released, before the connection goes back to the pool.
     */
    private <T> T underMaintenanceLock(Connection c, String call, SqlFunction0<T> body) throws SQLException {
        PartitionDdl.requireSessionStable(c, "outbox admin " + call + " on " + names.base());
        // before the lock, so a holder frozen right after taking it loses it too
        var session = SessionSettings.set(c, Map.of("idle_session_timeout", "'60s'"));
        boolean locked = false;
        Throwable failure = null;
        try {
            lockMaintenance(c);
            locked = true;
            return body.apply();
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            try {
                if (locked) {
                    advisory(c, "pg_advisory_unlock");
                }
                session.restore(c);
            } catch (SQLException e) { // the session died, and its lock and setting with it: keep the cause
                if (failure == null) {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    /** {@code <n> ids [<the first ten>, ...]}: a thousand ids would make the log line huge. */
    private static String logged(Collection<Long> ids) {
        return ids.size() + " ids " + ids.stream().limit(10).map(String::valueOf)
                .collect(Collectors.joining(", ", "[", ids.size() > 10 ? ", ...]" : "]"));
    }

    /**
     * Chunk after chunk until one claims fewer than {@link #CHUNK}, among the letters dead when the call began; one
     * INFO line, or a WARN on a failure. {@code putsBack}: the relay is woken when it put rows back into the outbox,
     * also when it failed after some chunks did.
     */
    private long bulk(String call, Where filter, boolean putsBack, Step step) {
        call = printable(call);
        long done = 0;
        try (var c = db.newConnection(); var bound = socket.on(c)) {
            c.setAutoCommit(true);
            Where where;
            try (var st = c.createStatement(); var rs = st.executeQuery("select max(id), now() from " + dead)) {
                rs.next();
                long last = rs.getLong(1);
                where = filter.and("id <= ?", rs.wasNull() ? Long.MIN_VALUE : last)
                        .and("failed_at <= ?", rs.getObject(2, OffsetDateTime.class));
            }
            long after = Long.MIN_VALUE;
            while (true) {
                long above = after;
                var chunk = inTx(c, tx -> step.run(tx, where, above));
                done += chunk.done();
                if (afterChunk != null) {
                    afterChunk.accept(c);
                }
                if (chunk.claimed() < CHUNK) {
                    break;
                }
                after = chunk.lastId();
            }
        } catch (SQLException | RuntimeException e) {
            log.warn("outbox admin {} on {} failed after {} letters, which stay done; calling again goes on: {}",
                    call, dead, done, printable(e.toString()));
            if (putsBack && done > 0) {
                wakeRelay.run();
            }
            throw e instanceof SQLException s ? db.translate(s) : (RuntimeException) e;
        }
        log.info("outbox admin {} on {} returned {}", call, dead, done);
        if (putsBack && done > 0) {
            wakeRelay.run();
        }
        return done;
    }

    /**
     * Redrives the claimed letters but those whose key another transaction holds, skipped without waiting; a key
     * found held goes in {@code held}, the call's, and its letters in the call's later chunks are skipped too, so a
     * later letter of the key never goes back ahead of an earlier one.
     */
    private Step redrive(Set<Integer> held) {
        return (tx, where, after) -> redrive(tx, where, after, held);
    }

    private Chunk redrive(Connection tx, Where where, long after, Set<Integer> held) throws SQLException {
        var claimed = new ArrayList<DeadLetters.Ref>();
        try (var ps = tx.prepareStatement("select " + DeadLetters.REF_COLUMNS + " from " + dead + " where "
                + where.sql() + " and id > ? order by id limit " + CHUNK + " for update skip locked")) {
            ps.setLong(where.bind(ps), after);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    claimed.add(DeadLetters.readRef(rs));
                }
            }
        }
        rows.limitLockWaits(tx); // before the key locks: from them on, writers of those keys wait with this chunk
        var ready = rows.tryKeyLocks(tx, claimed, held).ready();
        rows.redrive(tx, ready);
        return new Chunk(claimed.size(), claimed.isEmpty() ? after : claimed.get(claimed.size() - 1).id(),
                ready.size());
    }

    private Chunk discard(Connection tx, Where where, long after) throws SQLException {
        var ids = new ArrayList<Long>();
        try (var ps = tx.prepareStatement("delete from " + dead + " where id in (select id from " + dead + " where "
                + where.sql() + " and id > ? order by id limit " + CHUNK + " for update skip locked) returning id")) {
            ps.setLong(where.bind(ps), after);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
        }
        return new Chunk(ids.size(), ids.stream().mapToLong(Long::longValue).max().orElse(after), ids.size());
    }

    /**
     * A read-only call's {@code body} in one transaction ({@link #inTx}) under {@code statement_timeout} =
     * {@link #readTimeout} (each statement's, lock waits included), so a slow read gives its pool connection back.
     */
    private <T> T read(Connection c, SqlFunction1<Connection, T> body) throws SQLException {
        return SocketBound.atLeast(c, readTimeout, () -> inTx(c, tx -> {
            execute(tx, "set local statement_timeout = " + readTimeout.toMillis());
            return body.apply(tx);
        }));
    }

    /** {@code body} in one read-committed transaction on {@code c}, committed, or rolled back on any failure. */
    private static <T> T inTx(Connection c, SqlFunction1<Connection, T> body) throws SQLException {
        ShardProcessor.prepareTransaction(c, Duration.ZERO); // never idle: the 30 s idle timeout is ample
        try {
            T result = body.apply(c);
            c.commit();
            c.setAutoCommit(true);
            return result;
        } catch (Throwable t) {
            try {
                c.rollback();
            } catch (SQLException | RuntimeException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    /**
     * {@code text} with control characters (CR, LF, tab, any other C0 or C1 one), the Unicode line and paragraph
     * separators and format characters (a bidi override reorders a line on screen) escaped, so text a caller supplied
     * cannot forge log lines.
     */
    static String printable(String text) {
        var out = new StringBuilder(text.length());
        text.codePoints().forEach(ch -> {
            if (ch == '\n') {
                out.append("\\n");
            } else if (ch == '\r') {
                out.append("\\r");
            } else if (ch == '\t') {
                out.append("\\t");
            } else if (Character.isISOControl(ch) || ch == '\u2028' || ch == '\u2029'
                    || Character.getType(ch) == Character.FORMAT) {
                for (var unit : Character.toChars(ch)) { // beyond U+FFFF, Java's escape is two of them
                    out.append(String.format("\\u%04x", (int) unit));
                }
            } else {
                out.appendCodePoint(ch);
            }
        });
        return out.toString();
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private List<DeadLetterCount> deadLetterCounts(Connection c) throws SQLException {
        var counts = new ArrayList<DeadLetterCount>();
        try (var st = c.createStatement(); var rs = st.executeQuery(
                "select topic, status, count(*) from " + dead + " group by topic, status order by topic, status")) {
            while (rs.next()) {
                counts.add(new DeadLetterCount(rs.getString(1),
                        DeadLetter.Status.valueOf(rs.getString(2).toUpperCase(Locale.ROOT)), rs.getLong(3)));
            }
        }
        return counts;
    }

    /** {@code pg_advisory_lock(internalNs, LOCK_MAINTENANCE)} within {@link #maintenanceWait}. */
    private void lockMaintenance(Connection c) throws SQLException {
        try {
            PartitionDdl.execute(c, maintenanceWait, "select pg_advisory_lock(" + names.internalNamespace() + ", "
                    + TableNames.LOCK_MAINTENANCE + ")");
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                throw e;
            }
            // a maintenance run holds it, or another admin call under it: the first try of a retried one, say
            throw new IllegalStateException("maintenance or another outbox admin recovery call is running, retry: the"
                    + " maintenance lock of " + names.base() + ", pg_advisory_lock(" + names.internalNamespace() + ", "
                    + TableNames.LOCK_MAINTENANCE + "), was held for longer than " + maintenanceWait.toMillis() / 1e3
                    + " s", e);
        }
    }

    private void advisory(Connection c, String function) throws SQLException {
        try (var ps = c.prepareStatement("select " + function + "(?, ?)")) {
            ps.setInt(1, names.internalNamespace());
            ps.setInt(2, TableNames.LOCK_MAINTENANCE);
            ps.execute();
        }
    }

    private void requireOwnTransactions() {
        if (db.currentConnection() != null) {
            throw new IllegalStateException("OutboxAdmin manages its own transactions: call it outside "
                    + db.transactionHint());
        }
    }

    /** A bulk call's filter: one that names something, with no blank string, which would widen it. */
    private static DeadLetterFilter nonEmpty(DeadLetterFilter filter) {
        notBlank("topic", filter.topic());
        notBlank("errorContains", filter.errorContains());
        if (filter.isEmpty()) {
            throw new IllegalArgumentException("an empty DeadLetterFilter matches every dead letter: name what to"
                    + " act on (topic, key, errorContains, failedFrom, failedTo or status), or pass the ids");
        }
        return filter;
    }

    private static void notBlank(String field, @Nullable String value) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException("a blank " + field + " in a filter matches every " + field
                    + ": leave it out, or name one");
        }
    }

    private static boolean queryBoolean(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    private static double queryDouble(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getDouble(1);
        }
    }

    /** A condition on the dead table, each value a bound parameter, never interpolated. */
    private record Where(String sql, List<Object> params) {

        static Where of(DeadLetterFilter f) {
            var sql = new ArrayList<String>();
            var params = new ArrayList<Object>();
            // a blank string counts as absent: reads take it so, bulk calls refuse it first (nonEmpty)
            add(sql, params, "topic = ?", absentIfBlank(f.topic()));
            add(sql, params, "key = ?", f.key());
            // a substring: no pattern, no escapes
            add(sql, params, "position(? in error) > 0", absentIfBlank(f.errorContains()));
            add(sql, params, "failed_at >= ?", f.failedFrom() == null ? null : f.failedFrom().atOffset(ZoneOffset.UTC));
            add(sql, params, "failed_at < ?", f.failedTo() == null ? null : f.failedTo().atOffset(ZoneOffset.UTC));
            add(sql, params, "status = ?", f.status() == null ? null : f.status().name().toLowerCase(Locale.ROOT));
            return new Where(sql.isEmpty() ? "true" : String.join(" and ", sql), params);
        }

        Where and(String condition, Object value) {
            var all = new ArrayList<>(params);
            all.add(value);
            return new Where(sql + " and " + condition, all);
        }

        static Where ids(Collection<Long> ids) {
            // every chunk binds all the ids: quadratic for huge lists, fine for a page of them (a filter serves bulk)
            return new Where("id = any(?)", List.of(ids.stream().mapToLong(Long::longValue).toArray()));
        }

        @Nullable
        private static String absentIfBlank(@Nullable String s) {
            return s == null || s.isBlank() ? null : s;
        }

        private static void add(List<String> sql, List<Object> params, String condition, @Nullable Object value) {
            if (value != null) {
                sql.add(condition);
                params.add(value);
            }
        }

        /** Binds the parameters from index 1; returns the next index. */
        int bind(PreparedStatement ps) throws SQLException {
            int i = 1;
            for (var param : params) {
                ps.setObject(i++, param);
            }
            return i;
        }
    }
}
