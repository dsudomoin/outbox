package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.internal.PartitionDdl.Attachment;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.dsudomoin.outbox.internal.Sql.SqlFunction0;

/**
 * Keeps xid ranges {@code <base>_x<lower>} = {@code [lower, lower + xidsPerPartition)} premade above the current xid,
 * followed by the tail {@code [horizon, MAXVALUE)} that catches rows if maintenance falls behind (README "tail
 * overflow") until the next extension turns it into a range. The current xid is
 * {@code pg_snapshot_xmax(pg_current_snapshot())}, the next xid Postgres will assign. All methods need an autocommit
 * connection (but {@link #ensurePremade} for a missing tail, which {@link XidRecovery} runs in its transaction: it
 * only creates and attaches), and callers serialise them under {@code LOCK_MAINTENANCE}: bootstrap takes it after
 * {@code LOCK_SCHEMA}; {@link Maintenance} takes it with {@code pg_try_advisory_lock} and never takes
 * {@code LOCK_SCHEMA}. Besides the xid-regression recovery, the extension's tail swap ({@link #ensurePremade}) is the
 * one place the library takes ACCESS EXCLUSIVE on the parent, for milliseconds, taken without waiting unless
 * {@code outbox.partitions.swapLockTimeout} asks for a wait; range drops detach concurrently.
 */
public final class XidPartitions {

    private static final Logger log = LoggerFactory.getLogger(XidPartitions.class);

    /**
     * A transaction open this long with a lock on the parent is taken as one that may outlast the swap's lock tries:
     * the swap would not get its lock.
     */
    private static final Duration LONG_TRANSACTION = Duration.ofSeconds(20);
    /**
     * How long the swap tries for ACCESS EXCLUSIVE on the parent without waiting ({@code NOWAIT}, every millisecond),
     * under the quiesce: a try never queues, so no writer waits behind it.
     */
    static final Duration NOWAIT_WINDOW = Duration.ofMillis(200);
    /** A swap statement's {@code statement_timeout} past its hold's bound: its one try for the lock. */
    private static final Duration TRY_MARGIN = Duration.ofMillis(10);
    /**
     * The hold's bound by day, past its tries: the attaches and a scan within maxTailScan. Longer, the server cuts the
     * swap's statement (57014) and it rolls back; the next run tries again. Writers' bound by day: this plus
     * {@link #TRY_MARGIN} and the commit's local WAL flush.
     */
    static final Duration DAY_HOLD = Duration.ofMillis(250);
    /**
     * Every other lock wait while the swap holds the parent: none is expected, as the tables it attaches are locked
     * before and the tail is locked only through the parent, but by a session that reads it directly.
     */
    private static final Duration HOLD_LOCK_TIMEOUT = Duration.ofMillis(10);

    /** {@code idle_session_timeout} across the quiesce: a client silent this long under it has frozen or gone. */
    private static final Map<String, String> QUIESCE_IDLE = Map.of("idle_session_timeout", "'5s'");

    /**
     * The scan rate, bytes a second, a tail's attach back is given time for past its lock wait and
     * {@link PartitionDdl#DDL_MARGIN}: well below a cold sequential read, so a slow disk fits too. The scan holds no
     * writer back (SHARE UPDATE EXCLUSIVE on the parent, ACCESS EXCLUSIVE on the standalone tail alone), so its bound
     * is only there to tell a dead socket from a long statement.
     */
    static final long ATTACH_SCAN_RATE = 20L * 1024 * 1024;
    /** How many times its {@link #ATTACH_SCAN_RATE} time an attach back gets at most after cuts of the ones before. */
    static final int ATTACH_SLOWDOWN_CAP = 16;
    /** Ranges a swap attaches at most besides the former and the new tail: each attach adds to its hold. */
    static final int RANGES_PER_SWAP = 4;
    /** Where a swap statement leaves what it did, for the next statement of the session to read. */
    private static final String SWAP_RESULT = "kora_outbox.swap";
    /** Where {@link #dropPending}'s statement leaves what it did. */
    private static final String DROP_RESULT = "kora_outbox.drop_pending";
    /** {@link #lockWait} when {@code swapLockTimeout} is 0: past the swap's hold, which writers wait for. */
    private static final Duration DEFAULT_LOCK_WAIT = Duration.ofMillis(200);
    /**
     * Who would hold the swap's ACCESS EXCLUSIVE back: a description of the first, else null; and the virtual
     * transactions of lock holders whose age is hidden. Snapshots do not count: a detach that is not concurrent waits
     * for no snapshot. A relay step (it holds {@code LOCK_SWAP} shared) does not count either: the quiesce waits for
     * it, up to {@code batchTimeout} + 5 s from maintenance, without blocking writers (every shard pauses meanwhile);
     * one running longer defers the swap to the next run. Of the rest, short holders are fine, the swap waits them out;
     * a lock awaited on the parent counts at any age (the swap would queue behind it), as do a prepared transaction's
     * and a pg_dump's. A visible holder counts once its transaction is older than the threshold; one without
     * {@code xact_start} was between transactions when {@code pg_stat_activity} was read, so the lock seen is in a
     * transaction that has ended or begun since: young either way. A holder logged in as another role (without
     * {@code pg_read_all_stats}) or with {@code track_activities = off} has its {@code state} and age hidden: it is
     * returned for a second sample instead ({@link #swapBlocker}). {@code pg_locks} is read once, so a step's two locks
     * are seen together.
     */
    private static final String SWAP_BLOCKERS = """
            with p as materialized (select * from pg_locks
                                     where database = (select oid from pg_database where datname = current_database())),
                 l as (select * from p
                        where p.locktype = 'relation' and p.relation = to_regclass(?)
                          and p.pid is distinct from pg_backend_pid()
                          and not exists (select 1 from p s where s.locktype = 'advisory' and s.pid = p.pid
                                  and s.classid::bigint = ? and s.objid::bigint = ? and s.objsubid = 2
                                  and s.mode = 'ShareLock' and s.granted))
            select (select format('%s %s %s on %s',
                             case when l.pid is null then 'a prepared transaction'
                                  else format('pid %s (%s)', l.pid, a.application_name) end,
                             case when l.granted then 'holds' else 'waits for' end, l.mode, ?)
                      from l left join pg_stat_activity a on a.pid = l.pid
                     where not l.granted or l.pid is null or a.application_name like 'pg_dump%'
                        or a.state <> 'disabled' and a.xact_start < now() - make_interval(secs => ?)
                     limit 1),
                   h.vxids, h.holders
              from (select array_agg(l.virtualtransaction order by l.virtualtransaction) vxids,
                           array_agg(format('pid %s, which has held %s on %s', l.pid, l.mode, ?)
                                     order by l.virtualtransaction) holders
                      from l
                     where l.granted and exists (select 1 from pg_stat_activity a where a.pid = l.pid
                             and coalesce(a.application_name, '') not like 'pg_dump%'
                             and (a.state is null or a.state = 'disabled'))) h""";
    /** One {@link #SWAP_BLOCKERS} read: who defers at once, and the lock holders whose age is hidden. */
    private record Sample(@Nullable String blocker, List<String> hidden, List<String> holders) {}
    private static final long LOG_INTERVAL = TimeUnit.MINUTES.toNanos(1);
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String QUERY_CANCELED = "57014";
    private static final long STUCK_LOG_INTERVAL = TimeUnit.HOURS.toNanos(1);
    /** Due extensions in a row whose swap did not get its lock or was deferred, after which the ERROR says so. */
    static final int STUCK_SWAPS = 10;
    /**
     * The transactions holding a lock on a table, oldest first: how many, and the first five with their age. The
     * holders seen right after a lock wait timed out, so the ones it waited for and some that came since.
     */
    private static final String LOCK_HOLDERS = """
            with h as (select a.pid, a.application_name, a.xact_start from pg_stat_activity a
                        where a.pid in (select l.pid from pg_locks l where l.locktype = 'relation'
                                          and l.relation = to_regclass(?) and l.granted
                                          and l.database = (select oid from pg_database
                                                             where datname = current_database()))
                          and a.pid <> pg_backend_pid())
            select (select count(*) from h),
                   (select string_agg(format('pid %s (%s) in a transaction open %s', pid, application_name,
                                             coalesce(round(extract(epoch from now() - xact_start)::numeric, 1)
                                                      || ' s', 'for a time hidden from this role')), ', ')
                      from (select * from h order by xact_start nulls last limit 5) o)""";
    /**
     * The sessions holding {@code LOCK_SWAP} (key {@code ?, ?}), oldest first: how many, and the first five with
     * their age; relay steps hold it shared. Read right after the quiesce timed out on them.
     */
    private static final String STEP_HOLDERS = """
            with h as (select a.pid, a.application_name, a.xact_start, l.mode from pg_stat_activity a
                         join pg_locks l on l.pid = a.pid
                        where l.locktype = 'advisory' and l.classid::bigint = ? and l.objid::bigint = ?
                          and l.objsubid = 2 and l.granted
                          and l.database = (select oid from pg_database where datname = current_database())
                          and a.pid <> pg_backend_pid())
            select (select count(*) from h),
                   (select string_agg(format('pid %s (%s, %s) in a transaction open %s', pid, application_name,
                                             case when mode = 'ShareLock' then 'a relay step'
                                                  else mode || ', no relay step' end,
                                             coalesce(round(extract(epoch from now() - xact_start)::numeric, 1)
                                                      || ' s', 'for a time hidden from this role')), ', ')
                      from (select * from h order by xact_start nulls last limit 5) o)""";
    /**
     * The cluster's oldest prepared transaction and its oldest other session holding an xid, in any database: either
     * keeps {@code pg_snapshot_xmin} of every snapshot, and so the drop horizon, at its xid. Each is null when none
     * is seen; another role's session shows its xid to every role, but its state and xact_start only with
     * {@code pg_read_all_stats}.
     */
    private static final String OLDEST_XID_HOLDERS = """
            select (select format('prepared transaction %L in database %s, prepared %s s ago', gid, database,
                                  round(extract(epoch from now() - prepared)))
                      from pg_prepared_xacts order by age(transaction) desc limit 1),
                   (select format('pid %s in database %s (user %s, application %L, %s) in a transaction open %s',
                                  pid, datname, usename, application_name, coalesce(state, 'state hidden'),
                                  coalesce(round(extract(epoch from now() - xact_start)) || ' s',
                                           'for a time hidden from this role'))
                      from pg_stat_activity where backend_xid is not null and pid <> pg_backend_pid()
                     order by age(backend_xid) desc limit 1)""";
    /** A row of a standalone range at or above the bound, shard by shard through the (shard, xid) index it keeps. */
    private static final String ROW_AT_OR_ABOVE = """
            with recursive s(shard) as (
              select min(shard) from {table}
              union all
              select (select min(o.shard) from {table} o where o.shard > s.shard) from s where s.shard is not null)
            select 1 from s where s.shard is not null
              and exists (select 1 from {table} o where o.shard = s.shard and o.xid >= ?::text::xid8)
            limit 1""";

    /**
     * A standalone range {@link #orphans} lists and {@link #dropOrphans} kept; {@code ours} when maintenance detached
     * it as relayed, and then {@link Maintenance} moves its rows back ({@link OrphanRestore}) unless
     * {@code restoreOrphans} is off; {@code setAside} when the xid-regression recovery set it aside
     * ({@link XidRecovery}).
     */
    record Orphan(String table, boolean ours, boolean setAside) {}

    private final TableNames names;
    private final String markDropping;
    private final String unmarkDropping;
    private final String deleteStaleMarkers;
    private final String markOversized;
    private final String clearOversized;
    private final long size;
    private final int premake;
    private final Duration swapLockTimeout;
    private final long maxTailScan;
    /** {@code tailConversion.window}, null when empty. */
    @Nullable
    private final Window window;
    private final Duration maxStall;
    private final ZoneId zone;
    /**
     * The end of the window in which this instance's oversized conversion did not fit maxStall, null when none: no
     * other attempt until then, and an ERROR once it has passed ({@link #windowEnded}).
     */
    @Nullable
    private volatile ZonedDateTime cutUntil;
    private volatile long cutBytes;
    /**
     * When the hot tail over maxTailScan is stuck ({@link #tailStuck}): the end of the first whole window after any
     * instance first saw it ({@code tail_oversized_since} of the maintenance table), then at once when there is no
     * window; null while there is no such tail.
     */
    @Nullable
    private volatile ZonedDateTime stuckAt;
    private final AtomicLong stallLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final String quiesce;
    private final String unquiesce;
    private final Duration longTransaction;
    private final boolean bootstrap;
    private final AtomicLong deferLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong overflowLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong strangersLogged = new AtomicLong(System.nanoTime() - LOG_INTERVAL);
    private final AtomicLong stuckLogged = new AtomicLong(System.nanoTime() - STUCK_LOG_INTERVAL);
    private final AtomicLong largeLogged = new AtomicLong(System.nanoTime() - STUCK_LOG_INTERVAL);
    /** Due extensions in a row whose swap did not get its lock or was deferred; 0 after a swap. */
    private volatile int missedSwaps;
    private volatile boolean overflow;
    /** For tests: runs under the quiesce right before the swap's statements. */
    @Nullable
    Runnable beforeSwap;
    /** For tests: runs in {@link #heal} right after its size gate, before anything is locked. */
    @Nullable
    Runnable afterGate;
    /** For tests: oversized tails' conversions this instance began, inside the window. */
    final java.util.concurrent.atomic.AtomicInteger oversizedAttempts = new java.util.concurrent.atomic.AtomicInteger();
    /** The time {@code tailConversion.window} is read against, in {@code tailConversion.zone}; for tests. */
    Clock clock;
    /** For tests: PL/pgSQL run inside the swap's statement right after the tail's detach, to fail or stall it there. */
    @Nullable
    String inHold;
    /** For tests: the warnings of a hot tail's attach as a range, its DEBUG1 lines once the session asks for them. */
    @Nullable
    Consumer<SQLWarning> attachWarnings;
    /** Replaces {@link PartitionDdl#DDL_MARGIN} in the attach-back's statement bound, for tests. */
    Duration attachMargin = PartitionDdl.DDL_MARGIN;
    /** Replaces {@link #ATTACH_SCAN_RATE}, for tests. */
    long attachScanRate = ATTACH_SCAN_RATE;
    /** Why the server cut this instance's last attach back of the tail (57014), else null. */
    @Nullable
    private volatile String attachBackCut;
    /** Times its {@link #ATTACH_SCAN_RATE} time the next attach back gets: doubled by each cut, 1 after a success. */
    private volatile int attachSlowdown = 1;

    public XidPartitions(TableNames names, OutboxConfig.PartitionsConfig cfg) {
        this(names, cfg, LONG_TRANSACTION, false);
    }

    /** {@code longTransaction} replaces {@link #LONG_TRANSACTION}, for tests. */
    XidPartitions(TableNames names, OutboxConfig.PartitionsConfig cfg, Duration longTransaction) {
        this(names, cfg, longTransaction, false);
    }

    /**
     * For bootstrap: a lock holder whose age is hidden defers the extension to maintenance at once, rather than after a
     * second sample, which would hold the schema lock, and every other instance's startup, for the threshold; and a
     * hot tail is left to maintenance, as turning it into a range scans it ({@link #heal}).
     */
    static XidPartitions forBootstrap(TableNames names, OutboxConfig.PartitionsConfig cfg) {
        return new XidPartitions(names, cfg, LONG_TRANSACTION, true);
    }

    private XidPartitions(
            TableNames names, OutboxConfig.PartitionsConfig cfg, Duration longTransaction, boolean bootstrap) {
        this.names = names;
        this.markDropping = "insert into " + names.dropping() + " (table_name) values (?) on conflict do nothing";
        this.unmarkDropping = "delete from " + names.dropping() + " where table_name = ?";
        this.deleteStaleMarkers = "delete from " + names.dropping() + " d where not exists (select 1 from pg_class c"
                + " where c.relname = d.table_name"
                + " and c.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?)))";
        // in the database, so a restart or another instance counts from the fleet's first sighting, not its own
        this.markOversized = "update " + names.maintenance() + " set tail_oversized_since"
                + " = coalesce(tail_oversized_since, ?) returning tail_oversized_since";
        this.clearOversized = "update " + names.maintenance() + " set tail_oversized_since = null"
                + " where tail_oversized_since is not null";
        this.size = cfg.xidsPerPartition();
        this.premake = cfg.premake();
        this.swapLockTimeout = cfg.swapLockTimeout();
        this.maxTailScan = cfg.maxTailScan();
        this.window = Window.parse(cfg.tailConversion().window());
        this.maxStall = cfg.tailConversion().maxStall();
        this.zone = ZoneId.of(cfg.tailConversion().zone());
        this.clock = Clock.system(zone);
        this.quiesce = "select pg_advisory_lock(" + names.internalNamespace() + ", " + TableNames.LOCK_SWAP + ")";
        // only when held, so a quiesce that timed out costs no "you don't own a lock" WARNING in the server log
        this.unquiesce = "select pg_advisory_unlock(" + names.internalNamespace() + ", " + TableNames.LOCK_SWAP + ")"
                + " from pg_locks where locktype = 'advisory' and pid = pg_backend_pid() and mode = 'ExclusiveLock'"
                + " and classid::bigint = " + Integer.toUnsignedLong(names.internalNamespace()) + " and objid = "
                + TableNames.LOCK_SWAP + " and objsubid = 2";
        this.longTransaction = longTransaction;
        this.bootstrap = bootstrap;
    }

    /**
     * How long the library's other waits for a table lock that hold key locks meanwhile (a dead-letter redrive's or an
     * orphan restore's insert) may last, so how long the writers of those keys may wait: {@code swapLockTimeout}, or
     * 200 ms when it is 0, past the swap's hold that the insert may meet.
     */
    public static Duration lockWait(OutboxConfig.PartitionsConfig cfg) {
        return cfg.swapLockTimeout().isZero() ? DEFAULT_LOCK_WAIT : cfg.swapLockTimeout();
    }

    /** One INFO with the tail conversion window, its zone and maxStall, or that there is none; once per start. */
    void logTailConversion() {
        if (window == null) {
            log.info("outbox partition tail conversion window of {} is off (outbox.partitions.tailConversion.window"
                    + " empty): a hot tail over maxTailScan {} MB stays hot", names.base(), mb(maxTailScan));
        } else {
            log.info("outbox partition tail conversion window {} in zone {} (now {} there), maxStall {} ms: a hot tail"
                    + " of {} over maxTailScan {} MB is turned into a range only then", window, zone,
                    LocalTime.now(clock).withNano(0), maxStall.toMillis(), names.base(), mb(maxTailScan));
        }
    }

    /**
     * Extends once half of the premade ranges are used up ({@link #premadeAhead} ≤ {@code premake / 2}), until {@code
     * premake} whole ranges lie above the one holding the current xid, by swapping the attached tail for the first
     * {@link #RANGES_PER_SWAP} of those ranges and a new tail above them in one short statement ({@link #swap}), so the
     * tail is never left detach-pending; the runs after go on while the next range waits made ahead of the tail.
     * First, under no lock that writers wait for, it creates the new ranges as standalone tables (each skipped when it
     * exists: a failed swap leaves them for the next attempt, and {@link #dropOrphans} leaves them be). Not while
     * ({@link #swapBlocker}) a transaction open for 20 s holds a lock on the parent, or a lock on the parent is
     * awaited: the swap would not get its lock. The run then leaves the extension to the next one with a WARN; if the
     * margin runs out first, rows go to the attached tail. Otherwise it makes {@code <base>_tail_next} anew and waits
     * up to {@code quiesceTimeout} ({@code batchTimeout} + 5 s from maintenance) for relay steps in flight, which hold
     * {@code LOCK_SWAP} shared, by taking it exclusively ({@code pg_advisory_lock(internalNs, LOCK_SWAP)}, released
     * after the swap). Writers go on meanwhile, but every shard pauses: no step starts while the request is queued.
     * That is no bound on a step (its retry pass may run about 2 × {@code batchTimeout} and its main pass's call
     * another {@code batchTimeout}): a step running longer times the wait out, and the swap is left to the next run.
     * The swap takes its lock without waiting, so writers wait only for the milliseconds it holds it (and for a
     * configured {@code swapLockTimeout}). A hot tail (it holds rows, which sets {@link #overflow()}, or the current
     * xid is within one range of its lower bound) becomes a range itself, rows and all, scanned under the swap's lock
     * while within {@code maxTailScan} ({@link #heal}); bootstrap leaves that to maintenance. A due extension that did
     * not swap, deferred or without its lock, counts towards the hourly ERROR ({@link #missed}). A tail left detached,
     * detach-pending (an operator's {@code DETACH CONCURRENTLY}) or missing is finished first: dropped when empty,
     * attached back when it holds rows; ranges and a new tail are then attached without a swap, nothing being attached
     * above the last range. With the tail attached, a hole first: ranges between the current xid's and the tail's
     * lower bound that are missing, detached or dropped by hand, are attached back or made anew ({@link #fillHole}).
     * Lock timeouts surface as {@link SQLException} (SQLState 55P03), the tail still attached; the next run retries.
     */
    public void ensurePremade(Connection autocommit) throws SQLException {
        ensurePremade(autocommit, PartitionDdl.DETACH_LOCK_TIMEOUT, PartitionDdl.DETACH_LOCK_TIMEOUT);
    }

    void ensurePremade(Connection c, Duration lockTimeout, Duration quiesceTimeout) throws SQLException {
        var s = state(c);
        long floor = Math.floorDiv(s.current, size) * size; // lower bound of the range holding the current xid
        long target = floor + (premake + 1) * size; // premake whole ranges above the current xid's
        if (s.tail == Attachment.ATTACHED) {
            if (s.holeLower != null) {
                fillHole(c, s);
                s = state(c);
            }
            boolean due = s.ahead <= premake / 2;
            // an extension goes on, RANGES_PER_SWAP a run, while the next range waits made ahead of the tail
            if (!due && !(s.ahead < premake && PartitionDdl.attachment(c, names.base(),
                    names.partitionPrefix() + s.tailLower) == Attachment.STANDALONE)) {
                overflow = false;
                if (stuckAt != null) {
                    notOversized(c);
                }
                return;
            }
            boolean rows = PartitionDdl.hasRows(c, names.tail());
            if (rows) {
                overflowed();
            } else {
                overflow = false;
            }
            String blocker;
            try {
                if (!rows && s.current + size <= s.tailLower) {
                    notOversized(c);
                    blocker = extend(c, s, target, quiesceTimeout);
                } else if (!bootstrap) { // rows may reach the tail any moment: it becomes a range, holding them
                    blocker = heal(c, s, floor + 2 * size, quiesceTimeout);
                } else { // bootstrap leaves that to maintenance: the scan would hold the schema lock and others
                    return;
                }
            } catch (SQLException e) {
                var failed = e instanceof WritersHeldTheLock w ? writersHeldTheLock(c, w) : e;
                if (due) {
                    missed(s, "SQLState " + failed.getSQLState() + ": " + firstLine(failed.getMessage()),
                            failed instanceof RelayStepsInFlight);
                }
                throw failed;
            }
            if (blocker != null && due) {
                missed(s, blocker, false);
            }
            return;
        }
        if (s.tail != Attachment.MISSING && !dropOrAttachBack(c, s, lockTimeout)) {
            return;
        }
        overflow = false;
        notOversized(c);
        // the first ranges start at the oldest running transaction's: it holds an xid already, and may send once they
        // exist, as a request on an instance that started earlier would when another makes the table
        long from = s.upper == null ? Math.floorDiv(s.oldest, size) * size : s.upper;
        if (from < floor) { // ranges ended below the current xid (maintenance was down): bridge the gap in one range
            create(from, floor, c);
            from = floor;
        }
        for (; from < target; from += size) {
            create(from, from + size, c);
        }
        PartitionDdl.createAndAttach(
                c, names.base(), names.tail(), Long.toString(from), null, PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    /**
     * Closes the hole {@code [holeLower, holeUpper)} of {@code s} range by range, with an ERROR naming what it did: a
     * standalone empty {@code <base>_x<lower>} (detached by hand) is attached back, made anew first when its columns
     * are stale, and a missing one (dropped by hand) is made and attached. The attach takes SHARE UPDATE EXCLUSIVE on
     * the parent, which no writer waits for. One that holds rows is left standalone, for {@link #dropOrphans} to keep
     * and report as a foreign orphan: its rows may be ones the relay skipped while it was detached, so it is not
     * attached back with them, and inserts fail with 23514 once the xid counter reaches it until it is restored.
     */
    private void fillHole(Connection c, State s) throws SQLException {
        var attached = new ArrayList<String>();
        var kept = new ArrayList<String>();
        try {
            for (long lower = s.holeLower; lower < s.holeUpper; ) {
                long upper = Math.min((Math.floorDiv(lower, size) + 1) * size, s.holeUpper);
                var range = names.partitionPrefix() + lower;
                if (PartitionDdl.attachment(c, names.base(), range) == Attachment.STANDALONE
                        && PartitionDdl.hasRows(c, range)) {
                    kept.add(range);
                } else {
                    precreate(c, lower, upper); // the one range from lower, made anew when empty with stale columns
                    PartitionDdl.attach(c, names.base(), range, Long.toString(lower), Long.toString(upper),
                            PartitionDdl.CREATE_LOCK_TIMEOUT);
                    attached.add(range);
                }
                lower = upper;
            }
        } finally {
            log.error("outbox partitions of {} had a hole at xids [{}, {}), above the range of the current xid {} and"
                    + " below the tail: a range detached or dropped by hand (never do either). Inserts there fail with"
                    + " SQLState 23514 once the xid counter reaches it; attached again: {}{}", names.base(),
                    s.holeLower, s.holeUpper, s.current, attached, kept.isEmpty() ? ""
                            : "; kept standalone, as they hold rows: " + kept + ", restore or drop them"
                                    + " (OutboxAdmin.restoreOrphan), and the next run fills the hole");
        }
    }

    /**
     * The swap path of {@link #ensurePremade} for a tail that is not hot: ranges from its lower bound until target are
     * made, and the first {@link #RANGES_PER_SWAP} of them attached. Why it did not swap, else null.
     */
    @Nullable
    private String extend(Connection c, State s, long target, Duration quiesceTimeout) throws SQLException {
        long lower = s.tailLower;
        long top = Math.min(precreate(c, lower, target), lower + RANGES_PER_SWAP * size);
        if (top == lower) { // none to add, as after xidsPerPartition shrank: the swap would only replace the tail alike
            return null;
        }
        var blocker = deferred(c, s);
        if (blocker != null) {
            return blocker;
        }
        nextTail(c);
        var reason = quiesced(c, quiesceTimeout, () -> swap(c, lower, null, top, DAY_HOLD, 0));
        if (reason != null) {
            overflowed(); // rows reached the tail just before the swap
        }
        return reason;
    }

    /**
     * The hot path of {@link #ensurePremade}: the tail {@code [H, MAXVALUE)} becomes the range {@code [H, U)} with its
     * rows, and ranges from U and a new tail are attached as for an empty tail, all in one swap. No check ever goes on
     * the tail: one that spared the attach its scan would refuse every row routed to the tail at or above U from the
     * moment it was added, and a swap that then failed could leave it there. So Postgres scans the tail under the
     * swap's ACCESS EXCLUSIVE on the parent, and writers wait for the scan: while {@code pg_relation_size(tail)} is at
     * most {@code maxTailScan}, or, larger, inside {@code tailConversion.window}, the scan's statement then cut at
     * {@code tailConversion.maxStall}: a cut one rolls back with a WARN, and this instance tries no other until the
     * window ends ({@link #windowEnded}).
     * Otherwise the tail stays hot, writes going on into it, with an ERROR at most once an hour. U is {@code bound},
     * two ranges above the current xid's lower bound: room for the run until its swap. Standalone empty ranges a failed
     * swap pre-created below U are dropped first: {@code [H, U)} covers them, and the first would take its name. The
     * blocker that deferred it, else null.
     */
    @Nullable
    private String heal(Connection c, State s, long bound, Duration quiesceTimeout) throws SQLException {
        var now = ZonedDateTime.now(clock);
        windowEnded(now);
        long bytes = relationSize(c, names.tail());
        boolean oversized = bytes > maxTailScan;
        if (!oversized) {
            notOversized(c);
        } else { // the window it is first seen in may be mostly over: the next whole one
            var seen = oversizedSince(c, now);
            stuckAt = window == null ? seen
                    : window.endAfter(window.contains(seen.toLocalTime()) ? window.endAfter(seen) : seen);
        }
        if (oversized && !oversizedNow(now)) {
            if (cutUntil == null && due(largeLogged, STUCK_LOG_INTERVAL)) { // in a window that did not fit: said
                var until = window == null
                        ? "for good: outbox.partitions.tailConversion.window is empty, set one (maxStall "
                                + maxStall.toMillis() + " ms)"
                        : "until outbox.partitions.tailConversion.window " + window + " (" + zone + "), inside which"
                                + " it converts it with the scan cut at maxStall " + maxStall.toMillis() + " ms";
                log.error("outbox partition tail {} holds {} MB, over outbox.partitions.maxTailScan {} MB: turning it"
                        + " into a range scans it under ACCESS EXCLUSIVE on {}, writers waiting for the scan, so"
                        + " maintenance leaves it hot {}. Writes go on into it and nothing is lost; the disk grows"
                        + " until it is converted. Once it has gone a whole window unconverted (at once without one),"
                        + " maintenance runs count as failed, so outbox.maintenance.since_success grows. If conversions"
                        + " keep timing out (WARN \"did not fit\"), raise maxStall, keeping it below the writers' own"
                        + " lock_timeout and statement_timeout. This ERROR repeats at most once an hour", names.tail(),
                        mb(bytes), mb(maxTailScan), names.base(), until);
            }
            return null;
        }
        if (afterGate != null) {
            afterGate.run();
        }
        long lower = s.tailLower;
        for (long l = lower; l < bound; l += size) {
            var range = names.partitionPrefix() + l;
            if (PartitionDdl.attachment(c, names.base(), range) == Attachment.STANDALONE
                    && !PartitionDdl.hasRows(c, range)) {
                PartitionDdl.drop(c, range, PartitionDdl.CREATE_LOCK_TIMEOUT);
            }
        }
        // premake whole ranges above [H, U) made, the first RANGES_PER_SWAP attached with it
        long top = Math.min(precreate(c, bound, bound + premake * size), bound + RANGES_PER_SWAP * size);
        var blocker = deferred(c, s);
        if (blocker != null) {
            return blocker;
        }
        nextTail(c);
        try {
            var reason = quiesced(c, quiesceTimeout, () -> {
                // the quiesce may have waited past the window's end
                if (oversized && !oversizedNow(ZonedDateTime.now(clock))) {
                    return "";
                }
                if (oversized) {
                    oversizedAttempts.incrementAndGet();
                }
                // in the window, an oversized tail's hold is cut at maxStall: one try, then the next window
                return swap(c, lower, bound, top, oversized ? maxStall : DAY_HOLD,
                        oversized ? Long.MAX_VALUE : maxTailScan);
            });
            if (reason == null) {
                overflow = false;
                cutUntil = null;
                notOversized(c);
            }
            return reason == null || reason.isEmpty() ? null : reason;
        } catch (SQLException e) {
            if (!oversized || !QUERY_CANCELED.equals(e.getSQLState())) {
                throw e;
            }
            cutUntil = window.endAfter(now);
            cutBytes = bytes;
            log.warn("outbox partition tail {} of {} MB did not fit outbox.partitions.tailConversion.maxStall {} ms:"
                    + " the server cut the swap's statement and rolled it back, the tail stays hot, writes going on"
                    + " into it. This instance tries again in the next window, after {}", names.tail(), mb(bytes),
                    maxStall.toMillis(), cutUntil);
            return null;
        }
    }

    /** Whether an oversized tail may be converted now: inside the window, and none of ours was cut in it. */
    private boolean oversizedNow(ZonedDateTime now) {
        var cut = cutUntil;
        return window != null && window.contains(now.toLocalTime()) && (cut == null || !now.isBefore(cut));
    }

    /** The ERROR, once, when a window in which this instance's oversized conversion did not fit has ended. */
    private void windowEnded(ZonedDateTime now) {
        var cut = cutUntil;
        if (cut != null && !now.isBefore(cut)) {
            cutUntil = null;
            largeLogged.set(System.nanoTime()); // the hourly over-cap ERROR starts over: one ERROR this run
            log.error("outbox partition tail {} conversion window {} ({}) ended without a conversion: its scan of {} MB"
                    + " did not fit outbox.partitions.tailConversion.maxStall {} ms. The tail stays hot, writes going"
                    + " on into it, and the next window tries again. To fit, raise maxStall, keeping it below the"
                    + " writers' own lock_timeout and statement_timeout", names.tail(), window, zone, mb(cutBytes),
                    maxStall.toMillis());
        }
    }

    private static String firstLine(@Nullable String message) {
        return message == null ? "" : message.lines().findFirst().orElse("");
    }

    /** When any instance first saw the tail over maxTailScan, {@code now} when none had: recorded then. */
    private ZonedDateTime oversizedSince(Connection c, ZonedDateTime now) throws SQLException {
        try (var ps = c.prepareStatement(markOversized)) {
            ps.setObject(1, now.toOffsetDateTime());
            try (var rs = ps.executeQuery()) { // no row before bootstrap's: this instance's own sighting, as it was
                var seen = rs.next() ? rs.getObject(1, java.time.OffsetDateTime.class) : null;
                return seen == null ? now : seen.atZoneSameInstant(now.getZone());
            }
        }
    }

    /** The tail is not over maxTailScan, or no longer the one seen so: nothing stuck, for every instance. */
    private void notOversized(Connection c) throws SQLException {
        stuckAt = null;
        try (var st = c.createStatement()) {
            st.executeUpdate(clearOversized);
        }
    }

    static long relationSize(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select pg_relation_size(to_regclass(?))")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    static String mb(long bytes) {
        return String.format(java.util.Locale.ROOT, "%.1f", bytes / 1048576.0);
    }

    /**
     * A due extension did not swap: deferred by {@code cause}, or its swap did not get its lock. Once
     * {@link #STUCK_SWAPS} due runs in a row missed, or the ranges ahead fell below {@code premake / 2} (one used up
     * since the extension was due), an ERROR at most once an hour names the cause and the remedies: those for relay
     * steps that outlasted the quiesce when {@code relaySteps}, else those for writers' locks.
     */
    private void missed(State s, String cause, boolean relaySteps) {
        int missed = ++missedSwaps;
        if ((missed >= STUCK_SWAPS || s.ahead < premake / 2) && due(stuckLogged, STUCK_LOG_INTERVAL)) {
            if (relaySteps) {
                log.error("outbox partition tail swap of {} has not run on {} due runs in a row, {} ranges ahead of the"
                        + " current xid: {}. The swap waits for the relay steps in flight to end, up to"
                        + " outbox.relay.batchTimeout + 5 s, and pauses new ones meanwhile; a step whose handler call"
                        + " is slow (a slow or unreachable Kafka, a slow handler, a retry pass) can run about"
                        + " 3 × batchTimeout and time the wait out. Writers are not the cause. Writes go on and nothing"
                        + " is lost: once the ranges run out rows go to the tail, which becomes a range at the first"
                        + " swap that runs; the swap is tried again every run. Bring the handler's target back to"
                        + " health or make its calls end well within batchTimeout. This ERROR repeats at most once an"
                        + " hour", names.base(), missed, s.ahead, cause);
                return;
            }
            log.error("outbox partition tail swap of {} has not run on {} due runs in a row, {} ranges ahead of the"
                    + " current xid: {}. The swap needs a moment in which no transaction holds a lock on the outbox"
                    + " ({}); writer transactions that keep their lock long after"
                    + " their send and overlap, or a transaction left open, never leave one. Writes go on and nothing"
                    + " is lost: once the ranges run out rows go to the tail, which becomes a range at the first swap"
                    + " that runs. Send last in the transaction, end the holders by the pids above (an"
                    + " idle_in_transaction_session_timeout ends a leaked one). This ERROR repeats at most once an"
                    + " hour", names.base(), missed, s.ahead, cause, swapLockTimeout.isZero()
                    ? "NOWAIT tries for " + NOWAIT_WINDOW.toMillis() + " ms, outbox.partitions.swapLockTimeout 0"
                    : "NOWAIT tries for " + NOWAIT_WINDOW.toMillis() + " ms, then a wait of"
                            + " outbox.partitions.swapLockTimeout " + swapLockTimeout.toMillis() + " ms");
        }
    }

    /** The swap's lock wait timed out (55P03): the 55P03 again, naming who held a lock on its table, and their age. */
    private SQLException writersHeldTheLock(Connection c, WritersHeldTheLock e) {
        String holders;
        try (var ps = c.prepareStatement(LOCK_HOLDERS)) {
            ps.setString(1, e.table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                holders = rs.getLong(1) + " transactions hold a lock on " + e.table + " right after, the oldest: "
                        + rs.getString(2);
            }
        } catch (SQLException | RuntimeException read) {
            holders = "who held it could not be read: " + read;
        }
        // its first line: the rest is the DO block's context ("Where: PL/pgSQL function ..."), which splits log lines
        return new SQLException(firstLine(e.getMessage()) + "; " + holders, e.getSQLState(), e);
    }

    /** The quiesce timed out (55P03): the 55P03 again, naming the relay steps in flight that it waited for. */
    private RelayStepsInFlight relayStepsInFlight(Connection c, Duration quiesceTimeout, SQLException e) {
        String holders;
        try (var ps = c.prepareStatement(STEP_HOLDERS)) {
            ps.setLong(1, Integer.toUnsignedLong(names.internalNamespace())); // pg_locks shows the key as an oid
            ps.setLong(2, TableNames.LOCK_SWAP);
            try (var rs = ps.executeQuery()) {
                rs.next();
                holders = rs.getLong(1) + " sessions hold the swap lock right after, the oldest: " + rs.getString(2);
            }
        } catch (SQLException | RuntimeException read) {
            holders = "who held it could not be read: " + read;
        }
        return new RelayStepsInFlight(firstLine(e.getMessage()) + ": the tail swap waited " + quiesceTimeout.toMillis()
                + " ms for the relay steps in flight to end and is left to the next run; " + holders, e);
    }

    /** The quiesce's wait for relay steps in flight timed out (55P03): no writer held the swap back. */
    private static final class RelayStepsInFlight extends SQLException {
        RelayStepsInFlight(String message, SQLException e) {
            super(message, e.getSQLState(), e);
        }
    }

    /** A lock wait of the swap on writers' locks timed out (55P03) on {@code table}. */
    private static final class WritersHeldTheLock extends SQLException {
        final String table;

        WritersHeldTheLock(String table, SQLException e) {
            super(e.getMessage(), e.getSQLState(), e);
            this.table = table;
        }
    }

    /** Creates the ranges from {@code from} until {@code target} standalone, each skipped when it exists; their top. */
    private long precreate(Connection c, long from, long target) throws SQLException {
        for (; from < target; from += size) {
            // with the indexes: the swap's attach then builds none; one made before the parent changed is made again
            var range = names.partitionPrefix() + from;
            PartitionDdl.create(c, names.base(), range, PartitionDdl.CREATE_LOCK_TIMEOUT, true);
        }
        return from;
    }

    /** Makes {@code <base>_tail_next} anew: one a failed swap left may lack a column the parent gained since. */
    private void nextTail(Connection c) throws SQLException {
        PartitionDdl.execute(c, PartitionDdl.CREATE_LOCK_TIMEOUT, "drop table if exists " + names.tailNext());
        PartitionDdl.create(c, names.base(), names.tailNext(), PartitionDdl.CREATE_LOCK_TIMEOUT, true);
    }

    /** Who {@link #swapBlocker} finds would hold the swap back, with a WARN at most once a minute; else null. */
    @Nullable
    private String deferred(Connection c, State s) throws SQLException {
        var blocker = swapBlocker(c);
        if (blocker != null && due(deferLogged)) { // never forced: the swap would not get its lock
            log.warn("outbox partition extension of {} deferred to the next run, {} ranges ahead: {}, so the tail swap"
                    + " would not get its lock on the outbox while it lasts."
                    + " Long transactions on the outbox table (pg_dump of it, long reports or batch jobs) hold the"
                    + " extension back; outbox.partitions.premake must outlast the longest. This WARN repeats at most"
                    + " once a minute",
                    names.base(), s.ahead, blocker);
        }
        return blocker;
    }

    /**
     * Runs {@code action} holding {@code LOCK_SWAP} exclusively, taken within {@code quiesceTimeout}: relay steps in
     * flight, which hold it shared, have ended, and new ones skip from the request on (Postgres grants no shared lock
     * past a queued exclusive request) until it is released, whatever fails. A step in flight past
     * {@code quiesceTimeout} fails it with SQLState 55P03. The span runs under {@link #QUIESCE_IDLE}, set before the
     * quiesce and given back after its release: its statements follow each other within milliseconds, so a session
     * idle longer holding the lock belongs to a client that froze or lost its connection, and Postgres ends it before
     * it holds every relay step out for the 60 s of the maintenance session.
     */
    private <T> T quiesced(Connection c, Duration quiesceTimeout, SqlFunction0<T> action) throws SQLException {
        return SessionSettings.with(c, QUIESCE_IDLE, () -> quiesce(c, quiesceTimeout, action));
    }

    private <T> T quiesce(Connection c, Duration quiesceTimeout, SqlFunction0<T> action) throws SQLException {
        Throwable failure = null;
        try {
            // inside the try: whatever fails once the lock is granted, even an Error, releases it below; a session
            // lock left behind would go back to the pool and keep every relay step out
            try {
                PartitionDdl.execute(c, quiesceTimeout, quiesce); // under the session's own timeouts again afterwards
            } catch (SQLException e) {
                throw LOCK_NOT_AVAILABLE.equals(e.getSQLState()) ? relayStepsInFlight(c, quiesceTimeout, e) : e;
            }
            return action.apply();
        } catch (Throwable t) {
            failure = t;
            throw t;
        } finally {
            try {
                execute(c, unquiesce);
            } catch (SQLException e) { // a terminated session, its lock gone with it: keep the cause on top
                if (failure == null) {
                    throw e;
                }
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * The swap: one {@code DO} statement in autocommit, so the server commits or rolls it back at its end and no round
     * trip to this client falls inside the hold; its {@code statement_timeout} bounds the hold whatever this JVM does
     * meanwhile (a GC pause, a stopped process, a dead connection). Read committed whatever the session default (set
     * for the statement and given back), so each check takes its snapshot under the lock. The statement tries ACCESS
     * EXCLUSIVE on the parent once, {@code NOWAIT}, so no writer ever queues behind it (a try takes two transaction
     * ids, granted or not); it ends with nothing done if it missed, and repeats every millisecond for
     * {@link #NOWAIT_WINDOW}, then,
     * with {@code swapLockTimeout} above 0, one queued
     * wait of at most that follows, which writers do queue behind. None got: 55P03. Then every table it attaches,
     * {@code NOWAIT}: a session holding one (a stray reader, pg_dump) fails it at once (55P03). Holding the parent, no
     * writer's
     * transaction is open on the outbox, so a row routed to the tail before is committed or gone; every further lock
     * wait is cut at {@link #HOLD_LOCK_TIMEOUT}, and the hold at about {@code holdBound} (its
     * {@code statement_timeout}, {@link #TRY_MARGIN} longer for the try; 57014, all rolled back). It locks
     * the tail; a hot one ({@code bound} not null) bigger than {@code maxBytes} now, or with the current xid at or
     * above {@code bound}, and an empty one holding a row, end it with nothing done. Else it detaches the tail (not
     * concurrently: it waits for no snapshot and is never left pending): an empty tail is dropped, a hot one renamed
     * to {@code <base>_x<lower>} and attached as {@code [lower, bound)}, which Postgres does by scanning it, keeping
     * its indexes. Then the pre-created ranges up to {@code top}, empty and with their indexes made, are attached
     * without a build, and {@code <base>_tail_next} becomes the tail at {@code top}. Returns why it did nothing, or
     * null once it swapped, with one INFO saying how long the lock was tried for and held.
     */
    @Nullable
    String swap(Connection c, long lower, @Nullable Long bound, long top, Duration holdBound, long maxBytes)
            throws SQLException {
        String base = names.base();
        long first = bound == null ? lower : bound;
        if (beforeSwap != null) {
            beforeSwap.run();
        }
        var given = readCommitted(c);
        long start = System.nanoTime();
        String result;
        try {
            var tries = holdSql(false, lower, bound, first, top, maxBytes);
            result = PartitionDdl.withTimeouts(c, HOLD_LOCK_TIMEOUT, holdBound.plus(TRY_MARGIN), () ->
                    nowaitTries(start, () -> hold(c, tries)));
            if (result.equals("busy") && !swapLockTimeout.isZero()) {
                var queued = holdSql(true, lower, bound, first, top, maxBytes);
                result = PartitionDdl.withTimeouts(c, swapLockTimeout, swapLockTimeout.plus(holdBound), () ->
                        hold(c, queued));
            }
        } finally {
            giveBack(c, given);
        }
        if (result.equals("busy")) {
            throw new WritersHeldTheLock(base, new SQLException("could not take ACCESS EXCLUSIVE on " + base + " in "
                    + NOWAIT_WINDOW.toMillis() + " ms of NOWAIT tries: a transaction held a lock on it at every try",
                    LOCK_NOT_AVAILABLE));
        }
        if (result.startsWith("large ")) {
            return "the tail grew past outbox.partitions.maxTailScan under the swap's lock, to "
                    + mb(Long.parseLong(result.substring(6))) + " MB: not scanned";
        }
        if (result.equals("passed")) {
            return "the current xid passed " + bound + ", the range's upper bound, before the swap took its lock;"
                    + " the next run takes a higher one";
        }
        if (result.equals("rows")) {
            return "rows reached the tail just before its swap; the next run turns it into a range";
        }
        missedSwaps = 0;
        double held = Double.parseDouble(result.substring("swapped ".length()));
        double tried = Math.max(0, (System.nanoTime() - start) / 100_000 / 10.0 - held);
        String former = names.partitionPrefix() + lower;
        log.info("swapped the partition tail of {}: {}attached {} ranges [{}, {}) and the tail at {}; held ACCESS"
                + " EXCLUSIVE on {} for {} ms, after trying {} ms for it", base,
                bound == null ? "" : "attached it with its rows as " + former + " [" + lower + ", " + bound + "), ",
                (top - first) / size, first, top, top, base, held, Math.round(tried * 10) / 10.0);
        return null;
    }

    /**
     * {@code attempt}, one statement whose NOWAIT try missed when it returns "busy", again every millisecond while it
     * does, until {@link #NOWAIT_WINDOW} from {@code start}; its last result. A miss returns at once, having done
     * nothing, so no writer ever waits behind a try.
     */
    private static String nowaitTries(long start, SqlFunction0<String> attempt) throws SQLException {
        while (true) {
            var r = attempt.apply();
            if (!r.equals("busy") || System.nanoTime() - start >= NOWAIT_WINDOW.toNanos()) {
                return r;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return r;
            }
        }
    }

    /**
     * Drops {@code partition}, detach-pending, of {@code parent} without {@code FINALIZE}, which waits for every
     * snapshot in the database older than its own, a report on another table included: one autocommit {@code DO}
     * statement takes ACCESS EXCLUSIVE on the parent {@code NOWAIT}, as the swap does, and drops the partition. Any
     * query that could still see the partition holds a lock on the parent, so a try that gets it waits for no snapshot,
     * and one that misses leaves nothing behind. Tried every millisecond for {@link #NOWAIT_WINDOW}; the drop's own
     * lock waits are cut at {@link #HOLD_LOCK_TIMEOUT} and the hold at {@link #DAY_HOLD}. False when no try got the
     * lock, or the drop failed (a session holding the partition itself timed it out, 55P03; a dependent object, 2BP01;
     * another role's table, 42501): nothing done, the caller finalizes. A dead session (08xxx) throws.
     * Only for a partition whose rows may go: the caller has proven that.
     */
    static boolean dropPending(Connection c, String parent, String partition) throws SQLException {
        // a local commit, as the swap's: a wait for a synchronous standby ignores cancels and would hold the lock
        var sql = "do $$ begin perform set_config('synchronous_commit', 'local', true); begin lock table only " + parent
                + " in access exclusive mode nowait; exception when lock_not_available then perform set_config('"
                + DROP_RESULT + "', 'busy', false); return; end; drop table if exists " + partition + "; perform"
                + " set_config('" + DROP_RESULT + "', 'dropped', false); end $$";
        long start = System.nanoTime();
        String result;
        try {
            result = PartitionDdl.withTimeouts(c, HOLD_LOCK_TIMEOUT, DAY_HOLD.plus(TRY_MARGIN), () ->
                    nowaitTries(start, () -> {
                        execute(c, sql);
                        try (var st = c.createStatement();
                                var rs = st.executeQuery("select current_setting('" + DROP_RESULT + "', true)")) {
                            rs.next();
                            return rs.getString(1);
                        }
                    }));
        } catch (SQLException e) {
            if (e.getSQLState() != null && e.getSQLState().startsWith("08")) { // a dead session: nothing to finalize on
                throw e;
            }
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                // a dependent view, FK or function (2BP01), another role's table (42501): FINALIZE still clears the
                // pending state, which else blocks every other detach of the parent, and dropOrphans retries the drop
                log.warn("outbox could not drop partition {} of {}, left detach-pending (SQLState {}), so its detach is"
                        + " finalized instead: {}", partition, parent, e.getSQLState(), e.getMessage());
            }
            return false;
        }
        if (!result.equals("dropped")) {
            return false;
        }
        log.info("dropped partition {} of {}, left detach-pending, under ACCESS EXCLUSIVE on {} instead of finalizing"
                + " its detach", partition, parent, parent);
        return true;
    }

    /** The swap's one statement; {@code queued}: its parent lock waits in the queue instead of trying NOWAIT. */
    private String holdSql(boolean queued, long lower, @Nullable Long bound, long first, long top, long maxBytes) {
        String base = names.base();
        String tail = names.tail();
        // first, a local commit: Postgres stops the statement's timeout before the commit, and a wait for a synchronous
        // standby ignores cancels, so it would hold the lock unbounded. Safe: a writer's acknowledged commit waits for
        // an LSN past the swap's commit record, so it is replicated after it; a swap lost in a failover leaves the
        // previous catalog, consistent and attached as before
        var sql = new StringBuilder("do $$ declare locked timestamptz; begin perform set_config('synchronous_commit',"
                + " 'local', true); ");
        if (queued) {
            sql.append("lock table only ").append(base).append(" in access exclusive mode; ");
        } else {
            // a lock the try took in its block's subtransaction stays with the statement, and a miss logs nothing
            // (but takes two transaction ids: an ACCESS EXCLUSIVE request, granted or not, assigns one for its
            // standby record, in the block's subtransaction and its parent)
            sql.append("begin lock table only ")
                    .append(base).append(" in access exclusive mode nowait; exception when lock_not_available then"
                            + " perform set_config('" + SWAP_RESULT + "', 'busy', false); return; end; ");
        }
        // the tables it attaches, without waiting: a session holding one (a stray reader, pg_dump) fails the swap
        sql.append("lock table ").append(names.tailNext());
        for (long from = first; from < top; from += size) {
            sql.append(", ").append(names.partitionPrefix()).append(from);
        }
        sql.append(" in access exclusive mode nowait; ");
        sql.append("locked := clock_timestamp(); perform set_config('lock_timeout', '")
                .append(HOLD_LOCK_TIMEOUT.toMillis()).append("ms', true); lock table ").append(tail)
                .append(" in access exclusive mode; ");
        if (bound != null) {
            sql.append("if pg_relation_size('").append(tail).append("') > ").append(maxBytes)
                    .append(" then perform set_config('" + SWAP_RESULT + "', 'large ' || pg_relation_size('")
                    .append(tail).append("'), false); return; end if; ")
                    .append("if pg_snapshot_xmax(pg_current_snapshot()) >= '").append(bound)
                    .append("'::xid8 then perform set_config('" + SWAP_RESULT + "', 'passed', false); return;"
                            + " end if; ");
        } else {
            sql.append("if exists (select 1 from ").append(tail).append(") then perform set_config('" + SWAP_RESULT
                    + "', 'rows', false); return; end if; ");
        }
        sql.append("alter table ").append(base).append(" detach partition ").append(tail).append("; ");
        if (inHold != null) {
            sql.append(inHold).append(' ');
        }
        if (bound != null) {
            String former = names.partitionPrefix() + lower;
            sql.append("alter table ").append(tail).append(" rename to ").append(names.unqualified(former))
                    .append("; alter table ").append(base).append(" attach partition ").append(former).append(' ')
                    .append(PartitionDdl.bounds(Long.toString(lower), Long.toString(bound))).append("; ");
        } else {
            sql.append("drop table ").append(tail).append("; ");
        }
        for (long from = first; from < top; from += size) {
            sql.append("alter table ").append(base).append(" attach partition ").append(names.partitionPrefix())
                    .append(from).append(' ')
                    .append(PartitionDdl.bounds(Long.toString(from), Long.toString(from + size))).append("; ");
        }
        sql.append("alter table ").append(names.tailNext()).append(" rename to ")
                .append(names.unqualified(tail)).append("; alter table ").append(base).append(" attach partition ")
                .append(tail).append(' ').append(PartitionDdl.bounds(Long.toString(top), null))
                .append("; perform set_config('" + SWAP_RESULT + "', 'swapped ' || round((extract(epoch from"
                        + " clock_timestamp() - locked) * 1000)::numeric, 1), false); end $$");
        return sql.toString();
    }

    /** Runs one swap statement and reads what it did. */
    private String hold(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
            if (attachWarnings != null) {
                attachWarnings.accept(st.getWarnings());
            }
        }
        try (var st = c.createStatement();
                var rs = st.executeQuery("select current_setting('" + SWAP_RESULT + "', true)")) {
            rs.next();
            return rs.getString(1);
        }
    }

    /**
     * Makes the session's default isolation read committed for the swap's statement when it is not; what to give
     * back: null for nothing, "" for a reset, else the session's own value.
     */
    @Nullable
    private static String readCommitted(Connection c) throws SQLException {
        String setting;
        String source;
        try (var st = c.createStatement(); var rs = st.executeQuery("select setting, source from pg_settings"
                + " where name = 'default_transaction_isolation'")) {
            rs.next();
            setting = rs.getString(1);
            source = rs.getString(2);
        }
        if (setting.equals("read committed")) {
            return null;
        }
        execute(c, "set default_transaction_isolation = 'read committed'");
        return source.equals("session") ? setting : "";
    }

    private static void giveBack(Connection c, @Nullable String given) throws SQLException {
        if (given != null) {
            execute(c, given.isEmpty() ? "reset default_transaction_isolation"
                    : "set default_transaction_isolation = '" + given + "'");
        }
    }

    /**
     * Ranges lying entirely above the current xid (lower bound > current xid). Neither the range holding the current
     * xid nor the tail counts; right after an extension this equals {@code premake}.
     */
    public int premadeAhead(Connection c) throws SQLException {
        return state(c).ahead;
    }

    /** The oldest holders of an xid across the cluster ({@link #OLDEST_XID_HOLDERS}), for a log line. */
    static String oldestXidHolders(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(OLDEST_XID_HOLDERS)) {
            rs.next();
            var prepared = rs.getString(1);
            var session = rs.getString(2);
            if (prepared == null && session == null) {
                return "none now: no prepared transaction and no session holds an xid, so the one that did has ended"
                        + " since the latest tick, and the next tick moves past it (no tick is made while no relay runs)";
            }
            return prepared == null ? session : session == null ? prepared : prepared + "; " + session;
        }
    }

    /**
     * True while the tail holds rows, until a swap turns it into a range: informational, they are relayed as any, and
     * the next extension heals it within maxTailScan ({@link #heal}). Also set while an unattached tail holds rows,
     * which are invisible to the relay until it is attached back.
     */
    public boolean overflow() {
        return overflow;
    }

    /**
     * True once the hot tail over maxTailScan has gone a whole {@code tailConversion.window} without a conversion, at
     * once without a window: nothing will convert it, so maintenance counts the run as failed, which makes
     * {@code outbox.maintenance.since_success} grow.
     */
    boolean tailStuck() {
        var at = stuckAt;
        return at != null && !ZonedDateTime.now(clock).isBefore(at);
    }

    /**
     * The drop rule: detaches concurrently and drops, lowest first, every range ending at or below
     * {@code minCursorXmin}, the {@code pg_snapshot_xmin} of the slowest shard cursor's tick. Every transaction below
     * it had finished by that tick and the committed ones were visible to it, so every shard has relayed their rows.
     * The tail never qualifies, nor does the range holding the current xid (xmin is at most the current xid). Each
     * range is recorded in {@code <base>_dropping} before its detach and removed from it after its drop, so a run that
     * dies in between leaves {@link #dropOrphans} the proof that the library detached it as relayed. A range already
     * detach-pending is left to {@link #finishPendingDetach} and {@link #dropOrphans}: whoever started that detach
     * may have hidden its rows from the relay before they were delivered. A lock timeout
     * (SQLState 55P03, e.g. a detach waiting for a long transaction that uses the table) surfaces as
     * {@link SQLException} and leaves the rest to the next run. Returns the number of ranges dropped.
     */
    public int dropProcessed(Connection autocommit, long minCursorXmin) throws SQLException {
        return dropProcessed(autocommit, minCursorXmin, PartitionDdl.DETACH_LOCK_TIMEOUT);
    }

    int dropProcessed(Connection c, long minCursorXmin, Duration lockTimeout) throws SQLException {
        // not one already detach-pending: someone else's detach may have hidden its rows from the relay
        var pending = PartitionDdl.pendingDetach(c, names.base());
        var processed = PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> b.to() != null && Long.parseLong(b.to()) <= minCursorXmin && !b.name().equals(pending))
                .sorted(Comparator.comparingLong(b -> Long.parseLong(b.from())))
                .toList();
        for (var b : processed) {
            var range = names.unqualified(b.name());
            // committed before the detach: what lets dropOrphans finish it if this session dies before the drop
            marker(c, markDropping, range);
            PartitionDdl.detachConcurrentlyAndDrop(c, names.base(), b.name(), lockTimeout);
            marker(c, unmarkDropping, range);
        }
        return processed.size();
    }

    /**
     * Drops the ranges left standalone ({@code <base>_x<lower>} in no partition tree, which {@link #dropProcessed} no
     * longer lists), as a run that died, or whose DROP failed, after its detach leaves them. One recorded in
     * {@code <base>_dropping} was detached by {@link #dropProcessed} as relayed: dropped when no row is at or above
     * {@code minCursorXmin}, checked on the data. Any other was detached by someone else, perhaps before the relay
     * reached its rows: dropped only when empty (as a create whose attach never ran leaves one), as is a range the
     * xid-regression recovery set aside ({@code <base>_o<digits>}), but not one lying above the current xid: detached
     * by hand, it would leave a hole, so {@link #ensurePremade} attaches it back ({@link #fillHole}). Records whose
     * table is gone are deleted first.
     * Ranges at or above the tail's lower bound are not looked at: only the extension makes them there, standalone
     * until its swap, and a swap that failed or was deferred leaves them for the next attempt; set-aside ones are
     * looked at whatever their digits. Returns the ones kept.
     */
    List<Orphan> dropOrphans(Connection c, long minCursorXmin, Duration lockTimeout) throws SQLException {
        try (var ps = c.prepareStatement(deleteStaleMarkers)) { // their table is gone
            ps.setString(1, names.base());
            ps.executeUpdate();
        }
        var kept = new ArrayList<Orphan>();
        long current = state(c).current();
        int prefix = names.unqualified(names.partitionPrefix()).length();
        for (var orphan : orphans(c)) {
            var table = orphan.table();
            if (!orphan.ours() && !orphan.setAside()
                    && xidOf(names.unqualified(table).substring(prefix)) > current && !PartitionDdl.hasRows(c, table)) {
                continue; // a hole: ensurePremade attaches it back
            }
            // anyone else's detach may have come before the relay reached its rows: only an empty one goes
            boolean dropped = orphan.ours()
                    ? PartitionDdl.dropUnlessFound(c, table, ROW_AT_OR_ABOVE, Long.toString(minCursorXmin), lockTimeout)
                    : PartitionDdl.dropUnlessFound(c, table, "select 1 from {table} limit 1", null, lockTimeout);
            if (!dropped) {
                kept.add(orphan);
            } else if (orphan.ours()) {
                marker(c, unmarkDropping, names.unqualified(table));
            }
        }
        return kept;
    }

    /**
     * The standalone ranges {@link #dropOrphans} looks at, read only: {@code <base>_x<lower>} in no partition tree but
     * those at or above the tail's lower bound, which the extension makes for its next swap, and set-aside
     * {@code <base>_o<digits>}; {@code ours} when recorded in {@code <base>_dropping}. Sorted by their digits as a
     * number, not by name ({@code _x900} before {@code _x1000}), so orphans of ours are restored in xid order. A name
     * whose digits fit no long is no table of the library's: left out (so never dropped or restored), with a WARN at
     * most once a minute.
     */
    List<Orphan> orphans(Connection c) throws SQLException {
        var marked = new HashSet<String>();
        try (var st = c.createStatement(); var rs = st.executeQuery("select table_name from " + names.dropping())) {
            while (rs.next()) {
                marked.add(rs.getString(1));
            }
        }
        long tailLower = PartitionDdl.listRanges(c, names.base()).stream()
                .filter(b -> b.to() == null)
                .mapToLong(b -> Long.parseLong(b.from()))
                .findFirst()
                .orElse(Long.MAX_VALUE);
        int prefix = names.unqualified(names.partitionPrefix()).length();
        var setAsidePrefix = names.unqualified(names.setAsidePrefix());
        var orphans = new ArrayList<Orphan>();
        var strangers = new ArrayList<String>();
        for (var table : PartitionDdl.standalone(c, names.base(), "_[xo][0-9]+")) {
            var range = names.unqualified(table);
            var lower = xidOf(range.substring(prefix));
            if (lower == null) {
                strangers.add(table);
                continue;
            }
            boolean setAside = range.startsWith(setAsidePrefix);
            if (!setAside && lower >= tailLower) {
                continue;
            }
            orphans.add(new Orphan(table, !setAside && marked.contains(range), setAside));
        }
        if (!strangers.isEmpty() && due(strangersLogged)) {
            log.warn("outbox tables {} are named like ranges of {}, but their digits fit no bigint, so no table of the"
                    + " library's: left alone, neither dropped nor restorable. This WARN repeats at most once a minute",
                    strangers, names.base());
        }
        orphans.sort(Comparator.comparingLong(o -> xidOf(names.unqualified(o.table()).substring(prefix))));
        return orphans;
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static void marker(Connection c, String sql, String range) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            ps.setString(1, range);
            ps.executeUpdate();
        }
    }

    /** {@link PartitionDdl#reown}: the tail, the next tail, the ranges and the ranges set aside. */
    void reown(Connection c) throws SQLException {
        PartitionDdl.reown(c, names.base(), "_(tail|tail_next|[xo][0-9]+)");
    }

    /**
     * Finishes a detach that an interrupted run left pending. Postgres allows one pending detach per parent and fails
     * every other {@code DETACH ... CONCURRENTLY} meanwhile (SQLState 55000), so this goes before any other partition
     * DDL. A pending range recorded in {@code <base>_dropping} is one {@link #dropProcessed} detached as relayed (its
     * phase 2 timed out waiting for a lock holder, a relay step or a writer): no row can reach it since, so it is
     * dropped as that run would have, by {@link #dropPending} under the quiesce ({@code quiesceTimeout}, relay steps
     * in flight hold a lock on the parent too). When that misses, and for anyone else's pending range, it is
     * finalized, which leaves it standalone, and {@link #dropOrphans} decides on its drop: one recorded as relayed
     * goes, anyone else's is kept unless empty. A pending tail takes the extension's path: dropped when empty,
     * attached back when it holds rows.
     */
    void finishPendingDetach(Connection c, Duration lockTimeout, Duration quiesceTimeout) throws SQLException {
        var pending = PartitionDdl.pendingDetach(c, names.base());
        if (pending == null) {
            return;
        }
        if (PartitionDdl.attachment(c, names.base(), names.tail()) == Attachment.DETACH_PENDING) {
            dropOrAttachBack(c, state(c), lockTimeout);
            return;
        }
        var range = names.unqualified(pending);
        if (marked(c, range) && droppedQuiesced(c, pending, quiesceTimeout)) {
            marker(c, unmarkDropping, range);
            return;
        }
        PartitionDdl.detach(c, names.base(), pending, lockTimeout); // FINALIZE; dropOrphans decides on the drop
    }

    private boolean marked(Connection c, String range) throws SQLException {
        try (var ps = c.prepareStatement("select 1 from " + names.dropping() + " where table_name = ?")) {
            ps.setString(1, range);
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** {@link #dropPending} under the quiesce; false when a relay step in flight outlasted {@code quiesceTimeout}. */
    private boolean droppedQuiesced(Connection c, String pending, Duration quiesceTimeout) throws SQLException {
        try {
            return quiesced(c, quiesceTimeout, () -> dropPending(c, names.base(), pending));
        } catch (SQLException e) {
            if (!LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                throw e;
            }
            return false;
        }
    }

    /**
     * Xids left before an insert finds no partition (SQLState 23514), or null while the tail is attached. A tail that
     * is detach-pending, detached or missing takes no rows, so inserts fail once the current xid reaches the top of the
     * highest range.
     */
    @Nullable
    Long xidsLeft(Connection c) throws SQLException {
        var s = state(c);
        return s.tail == Attachment.ATTACHED ? null : s.upper == null ? 0 : Math.max(0, s.upper - s.current);
    }

    /** Whether an insert at the current xid finds a partition: the tail is attached or a range covers the xid. */
    boolean acceptsInserts(Connection c) throws SQLException {
        var s = state(c);
        return s.tail == Attachment.ATTACHED || (s.upper != null && s.current < s.upper);
    }

    /** Finishes a tail that is no longer attached; false when it held rows and was attached back. */
    private boolean dropOrAttachBack(Connection c, State s, Duration lockTimeout) throws SQLException {
        boolean rows = PartitionDdl.hasRows(c, names.tail());
        if (rows) {
            overflow = true; // stays set until the tail is attached again, even if the steps below time out
        }
        PartitionDdl.detach(c, names.base(), names.tail(), lockTimeout); // FINALIZE when pending, else no-op
        // a standalone tail has no bound left in the catalog; a tail always starts where the last range ends
        long lower = s.tailLower != null ? s.tailLower
                : s.upper != null ? s.upper
                : Math.floorDiv(s.current, size) * size;
        String tail = names.tail();
        long bytes = relationSize(c, tail);
        if (bootstrap && rows && bytes > maxTailScan) { // its scan would hold the schema lock and others' startup
            return false;
        }
        // the attach scans the whole tail to check its bounds: time for that at ATTACH_SCAN_RATE, blocking no writer,
        // times the slowdown earlier cuts showed, so a disk slower than the rate still gets it back
        int slowdown = attachSlowdown;
        var bound = lockTimeout.plus(attachMargin).plusMillis(bytes * 1000 / attachScanRate * slowdown);
        try {
            boolean dropped = PartitionDdl.dropIfEmptyElseAttach(
                    c, names.base(), tail, Long.toString(lower), null, lockTimeout, bound);
            attachBackCut = null;
            attachSlowdown = 1;
            if (dropped) {
                return true;
            }
        } catch (SQLException e) {
            boolean cut = QUERY_CANCELED.equals(e.getSQLState());
            if (cut) {
                attachSlowdown = Math.min(ATTACH_SLOWDOWN_CAP, slowdown * 2);
            }
            attachBackCut = !cut ? null
                    : "attaching it back scans all its " + mb(bytes) + " MB to check its bounds, and the server cut"
                            + " that at the " + bound.toMillis() / 1000.0 + " s it was given ("
                            + firstLine(e.getMessage()) + "). " + (slowdown < ATTACH_SLOWDOWN_CAP
                                    ? "The next run gives its scan twice that time, up to " + ATTACH_SLOWDOWN_CAP
                                            + " times the first; or run it"
                                    : "Its scan already gets " + ATTACH_SLOWDOWN_CAP + " times the first time; run it")
                            + " by hand in a session without a statement_timeout;"
                            + " it blocks no writer: alter table " + names.base() + " attach partition " + tail + " "
                            + PartitionDdl.bounds(Long.toString(lower), null);
            throw e;
        }
        overflowed();
        return false;
    }

    /** Why the server cut this instance's last attach back of a detached tail, its scan too long, else null. */
    @Nullable
    String attachBackCut() {
        return attachBackCut;
    }

    /**
     * WARNs when this role cannot see how long the transactions of sessions logged in as another role have run (no
     * {@code pg_read_all_stats}), which changes the check before a tail swap ({@link #swapBlocker}).
     */
    void warnIfActivityHidden(Connection c) throws SQLException {
        try (var st = c.createStatement();
                var rs = st.executeQuery("select current_user, pg_has_role('pg_read_all_stats', 'USAGE')")) {
            rs.next();
            if (!rs.getBoolean(2)) {
                log.warn("outbox role {} is not a member of pg_read_all_stats, so Postgres hides how long the"
                        + " transactions of sessions logged in as another role have run. That includes this app's own"
                        + " sessions when they log in as a role other than {} (SET ROLE, options=-c role=...,"
                        + " per-instance credentials). Before swapping the partition tail of {}, maintenance then"
                        + " samples such sessions' locks on it twice, 20 s apart, and defers the extension when one"
                        + " transaction holds a lock across both. Grant pg_read_all_stats (or pg_monitor) to {} to"
                        + " read their age directly, without the 20 s wait",
                        rs.getString(1), rs.getString(1), names.base(), rs.getString(1));
            }
        }
    }

    /**
     * Who would hold the tail swap back, described, or null ({@link #SWAP_BLOCKERS}). A lock holder whose age is
     * hidden counts only when the same transaction still holds a lock on the parent a threshold later: this sleeps for
     * it, between two autocommit statements (no transaction open, only the caller's session locks held), and only
     * when the first sample finds such a holder. A short transaction of the app's own (under another login role)
     * has ended by then.
     */
    @Nullable
    String swapBlocker(Connection c) throws SQLException {
        var first = sample(c);
        if (first.blocker() != null || first.hidden().isEmpty()) {
            return first.blocker();
        }
        if (bootstrap) {
            return "a session whose age is hidden from this role, which lacks pg_read_all_stats, holds a lock on "
                    + names.base() + "; bootstrap leaves the extension to maintenance";
        }
        try {
            Thread.sleep(longTransaction.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "maintenance was interrupted while it waited to sample the locks on " + names.base() + " again";
        }
        // everything again, not only the hidden holders: a pg_dump or a waiter may have come meanwhile
        var second = sample(c);
        if (second.blocker() != null) {
            return second.blocker();
        }
        for (int i = 0; i < second.hidden().size(); i++) {
            if (first.hidden().contains(second.hidden().get(i))) {
                return second.holders().get(i) + " for longer than " + longTransaction.toSeconds() + " s, its age"
                        + " hidden from this role; granting pg_read_all_stats to the outbox role reads it directly,"
                        + " without this second look " + longTransaction.toSeconds() + " s later";
            }
        }
        return null;
    }

    private Sample sample(Connection c) throws SQLException {
        double seconds = longTransaction.toMillis() / 1e3;
        try (var ps = c.prepareStatement(SWAP_BLOCKERS)) {
            ps.setString(1, names.base());
            ps.setLong(2, Integer.toUnsignedLong(names.internalNamespace())); // pg_locks shows the key as an oid
            ps.setLong(3, TableNames.LOCK_SWAP);
            ps.setString(4, names.base());
            ps.setDouble(5, seconds);
            ps.setString(6, names.base());
            try (var rs = ps.executeQuery()) {
                rs.next();
                return new Sample(rs.getString(1), strings(rs.getArray(2)), strings(rs.getArray(3)));
            }
        }
    }

    private static List<String> strings(@Nullable java.sql.Array array) throws SQLException {
        return array == null ? List.of() : List.of((String[]) array.getArray());
    }

    private void create(long from, long to, Connection c) throws SQLException {
        PartitionDdl.createAndAttach(c, names.base(), names.partitionPrefix() + from, Long.toString(from),
                Long.toString(to), PartitionDdl.CREATE_LOCK_TIMEOUT);
    }

    private void overflowed() {
        overflow = true;
        if (due(overflowLogged)) {
            log.warn("outbox rows went to the partition tail {}: maintenance fell behind; the next extension turns it"
                    + " into a range while it is within outbox.partitions.maxTailScan, a larger one only inside"
                    + " tailConversion.window (gauge outbox.partitions.overflow). This WARN repeats at most once a"
                    + " minute", names.tail());
        }
    }

    /**
     * The xid the digits of a range name give, or null when they fit no long: no table of the library's, as xids stay
     * far below 2^63.
     */
    @Nullable
    static Long xidOf(String digits) {
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** True at most once per {@link #LOG_INTERVAL} for {@code logged}. */
    private static boolean due(AtomicLong logged) {
        return due(logged, LOG_INTERVAL);
    }

    private static boolean due(AtomicLong logged, long interval) {
        long now = System.nanoTime();
        long last = logged.get();
        return now - last >= interval && logged.compareAndSet(last, now);
    }

    /** The current xid, the premade ranges, {@code retained} ones and the tail: one snapshot and one catalog read. */
    State state(Connection c) throws SQLException {
        long current;
        long oldest;
        try (var st = c.createStatement();
                var rs = st.executeQuery("select pg_snapshot_xmax(pg_current_snapshot())::text::bigint,"
                        + " pg_snapshot_xmin(pg_current_snapshot())::text::bigint")) {
            rs.next();
            current = rs.getLong(1);
            oldest = rs.getLong(2);
        }
        Long upper = null;
        Long tailLower = null;
        int ahead = 0;
        var retainedTo = new ArrayList<Long>();
        var bounded = new ArrayList<long[]>();
        for (var b : PartitionDdl.listRanges(c, names.base())) {
            if (b.to() == null) {
                tailLower = Long.parseLong(b.from());
                continue;
            }
            if (Long.parseLong(b.from()) > current) {
                ahead++;
            }
            long to = Long.parseLong(b.to());
            if (to <= current) {
                retainedTo.add(to);
            }
            upper = upper == null ? to : Math.max(upper, to);
            bounded.add(new long[] {Long.parseLong(b.from()), to});
        }
        // the first xids above the current one that no range covers below the tail: a range detached or dropped by
        // hand, as the library leaves none. Not when no range covers the current xid (its rows a foreign orphan, or
        // an xid regression): inserts fail at once then, and the recovery or the orphan's restore has to come first
        bounded.sort(Comparator.comparingLong(r -> r[0]));
        long reach = current;
        boolean covered = false;
        long[] hole = null;
        for (var r : bounded) {
            if (r[1] <= reach) {
                continue;
            }
            if (r[0] > reach) {
                hole = covered ? new long[] {reach, r[0]} : null;
                break;
            }
            reach = r[1];
            covered = true;
        }
        if (hole == null && covered && tailLower != null && reach < tailLower) {
            hole = new long[] {reach, tailLower};
        }
        return new State(current, oldest, upper, ahead, retainedTo.size(),
                retainedTo.stream().mapToLong(Long::longValue).toArray(),
                PartitionDdl.attachment(c, names.base(), names.tail()), tailLower,
                hole == null ? null : hole[0], hole == null ? null : hole[1]);
    }

    /** {@code tailConversion.window}: from (inclusive) to (exclusive), across midnight when to comes first. */
    record Window(LocalTime from, LocalTime to) {

        private static final java.util.regex.Pattern FORMAT =
                java.util.regex.Pattern.compile("((?:[01]\\d|2[0-3]):[0-5]\\d)-((?:[01]\\d|2[0-3]):[0-5]\\d)");

        /** The window {@code text} gives, null for an empty one (never). */
        @Nullable
        static Window parse(String text) {
            if (text.isEmpty()) {
                return null;
            }
            var m = FORMAT.matcher(text);
            if (!m.matches() || m.group(1).equals(m.group(2))) {
                throw new IllegalArgumentException("outbox.partitions.tailConversion.window must be HH:mm-HH:mm, two"
                        + " different times of day (crossing midnight when the second comes first), or empty: "
                        + text);
            }
            return new Window(LocalTime.parse(m.group(1)), LocalTime.parse(m.group(2)));
        }

        /** The first end of the window after {@code now}: inside it, this occurrence's end. */
        ZonedDateTime endAfter(ZonedDateTime now) {
            var end = now.toLocalDate().atTime(to).atZone(now.getZone());
            return end.isAfter(now) ? end : end.plusDays(1);
        }

        boolean contains(LocalTime time) {
            return from.isBefore(to)
                    ? !time.isBefore(from) && time.isBefore(to)
                    : !time.isBefore(from) || time.isBefore(to);
        }

        @Override
        public String toString() {
            return from + "-" + to;
        }
    }

    /**
     * {@code ahead}: ranges above the current xid ({@link #premadeAhead}); {@code retained}: attached ranges lying
     * entirely below the range holding it, which {@link #dropProcessed} has not dropped yet, and {@code retainedTo}
     * their upper bounds; {@code [holeLower, holeUpper)}: the first xids above the current one that no range covers
     * below the tail, null when none.
     */
    record State(long current, long oldest, @Nullable Long upper, int ahead, int retained, long[] retainedTo,
            Attachment tail, @Nullable Long tailLower, @Nullable Long holeLower, @Nullable Long holeUpper) {

        /** Retained ranges with an upper bound above {@code xmin}: kept even with every shard at a tick of it. */
        int retainedAbove(long xmin) {
            int n = 0;
            for (long to : retainedTo) {
                if (to > xmin) {
                    n++;
                }
            }
            return n;
        }
    }
}
