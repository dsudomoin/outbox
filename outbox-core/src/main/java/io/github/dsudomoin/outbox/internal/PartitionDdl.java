package io.github.dsudomoin.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Range-partition DDL shared by the outbox (xid8 bounds) and the inbox (uuid bounds). Every method needs an autocommit
 * connection ({@code DETACH ... CONCURRENTLY} cannot run in a transaction block, the drop-or-attach methods commit),
 * except the reads, {@link #create} and {@link #attach}, which {@link XidRecovery} also runs in its transaction. Nothing
 * is set for the session, so behind a transaction-pooling proxy (pgbouncer, Odyssey), where each autocommit statement
 * may run in another server session, nothing outlives a statement into another client's session: on an autocommit
 * connection each DDL statement runs in a transaction of its own ({@link #transaction}) under {@code SET LOCAL}
 * timeouts, its own {@code lock_timeout} and a {@code statement_timeout} {@link #DDL_MARGIN} longer, so a shorter role
 * or database {@code statement_timeout} cannot end the lock wait first (57014); inside the caller's transaction it
 * runs under the same {@code SET LOCAL}s, the transaction's own values given back after it. A statement Postgres runs
 * only alone outside a transaction block, a concurrent detach, runs as {@link #executeConcurrently} says. Bound values
 * are unquoted text ({@code "1000"}); a {@code null} upper bound is {@code MAXVALUE}. Names are interpolated, so they
 * and the values are checked against a strict charset.
 */
public final class PartitionDdl {

    public static final Duration CREATE_LOCK_TIMEOUT = Duration.ofSeconds(2);
    public static final Duration DETACH_LOCK_TIMEOUT = Duration.ofSeconds(30);
    private static final Pattern NEEDLESS_QUOTES = Pattern.compile("\"([a-z_][a-z0-9_]*)\"");
    /** A DDL statement's {@code statement_timeout} past its {@code lock_timeout}: room for its own work. */
    static final Duration DDL_MARGIN = Duration.ofSeconds(60);
    /**
     * The timeouts as this transaction set them ({@code SET LOCAL} shows as the session's), null where it did not.
     */
    private static final String SESSION_TIMEOUTS = "select max(setting) filter (where name = 'lock_timeout'),"
            + " max(setting) filter (where name = 'statement_timeout'),"
            + " max(setting) filter (where name = 'transaction_timeout') from pg_settings"
            + " where name in ('lock_timeout', 'statement_timeout', 'transaction_timeout') and source = 'session'";
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    /**
     * The {@code idle_in_transaction_session_timeout} of a work transaction that waits up to {@code lockTimeout}:
     * {@code lockTimeout} + 30 s, kept short of a frozen lock holder's end ({@link Maintenance#HOLDER_IDLE}, less two
     * keepalives), so a frozen instance's work transaction ends before its holder does and another instance's run never
     * meets it.
     */
    static Duration workIdle(Duration lockTimeout) {
        var cap = Maintenance.HOLDER_IDLE.minus(LockHolder.KEEPALIVE.multipliedBy(2));
        var idle = lockTimeout.plusSeconds(30);
        return idle.compareTo(cap) < 0 ? idle : cap;
    }
    private static final String QUERY_CANCELED = "57014";

    /**
     * The sessions of this database holding a snapshot, oldest first: how many, and the first five with their
     * transaction's age; a session of another role shows none to a role without {@code pg_read_all_stats}.
     */
    private static final String OLDEST_SNAPSHOTS = """
            with s as (select pid, application_name, state, xact_start, backend_xmin from pg_stat_activity
                        where datname = current_database() and backend_xmin is not null and pid <> pg_backend_pid())
            select (select count(*) from s),
                   (select string_agg(format('pid %s (%s, %s) in a transaction open %s', pid, application_name, state,
                                             coalesce(round(extract(epoch from now() - xact_start)::numeric, 1)
                                                      || ' s', 'for a time hidden from this role')), ', ')
                      from (select * from s order by age(backend_xmin) desc limit 5) o)""";

    private static final Logger log = LoggerFactory.getLogger(PartitionDdl.class);
    /** Where {@link #reown}'s statement leaves what it did. */
    private static final String REOWN_RESULT = "kora_outbox.reown";
    private static final long HANDOFF_WARN_INTERVAL = TimeUnit.HOURS.toNanos(1);
    /** By parent: when {@link #foreignOwner} last warned that its partitions cannot go to its owner. */
    private static final Map<String, AtomicLong> handoffWarned = new ConcurrentHashMap<>();
    private static final Pattern NAME = Pattern.compile("(?:[A-Za-z_][A-Za-z0-9_]*\\.)?[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern VALUE = Pattern.compile("[0-9A-Za-z:.+ -]+");
    private static final Pattern BOUNDS = Pattern.compile("FOR VALUES FROM \\((.+)\\) TO \\((.+)\\)");

    /** One partition; {@code name} is usable in DDL as is, {@code to == null} is {@code MAXVALUE}. */
    public record Bound(String name, String from, @Nullable String to) {}

    /** How a table relates to a partitioned parent. */
    enum Attachment {
        ATTACHED,
        /** {@code DETACH CONCURRENTLY} was interrupted: no new rows can be routed to it and its rows are invisible. */
        DETACH_PENDING,
        /** The table exists but is not a partition of the parent. */
        STANDALONE,
        MISSING
    }

    private PartitionDdl() {}

    public static void createAndAttach(
            Connection c, String parent, String partition, String from, @Nullable String to, Duration lockTimeout)
            throws SQLException {
        create(c, parent, partition, lockTimeout);
        attach(c, parent, partition, from, to, lockTimeout);
    }

    /**
     * Creates {@code table} standalone, {@code like parent} with its defaults, CHECK constraints and generated columns
     * (an attach requires the last two), unless it exists (an earlier run that failed before its attach left it). One
     * left empty and standalone before the parent changed ({@link #sameShape}) would fail every attach: dropped and
     * made again. {@code LIKE} takes ACCESS SHARE on the parent, which no writer waits for.
     */
    static void create(Connection c, String parent, String table, Duration lockTimeout) throws SQLException {
        create(c, parent, table, lockTimeout, false);
    }

    /**
     * {@link #create}; with {@code indexes}, also the parent's indexes, which an attach to it then takes over instead
     * of building them under its lock.
     */
    static void create(Connection c, String parent, String table, Duration lockTimeout, boolean indexes)
            throws SQLException {
        if (exists(c, table) && attachment(c, parent, table) == Attachment.STANDALONE && !sameShape(c, parent, table)
                && !hasRows(c, table)) {
            drop(c, table, lockTimeout);
        }
        if (!exists(c, table)) {
            var owner = foreignOwner(c, parent, table);
            // one statement batch, one implicit transaction: no table is left behind under the creating role
            execute(c, lockTimeout, "create table " + name(table) + " (like " + name(parent) + " including defaults"
                    + " including constraints including generated" + (indexes ? " including indexes)" : ")")
                    + (owner == null ? "" : "; alter table " + name(table) + " owner to " + owner));
            log.info("created table {} like {}", table, parent);
        }
    }

    /**
     * {@code parent}'s owner, quoted, when it is not the current role, else null. A table the library makes belongs to
     * it, so any other member of that role (per-instance credentials) can later lock, rename, attach or drop it. When
     * the current role cannot give {@code table} to it (on Postgres 16+ membership needs the SET option, and the owner
     * needs {@code CREATE} on the schema), null as well: the table stays the current role's, which is all a single DDL
     * role needs, with a WARN naming the grant at most once an hour per parent.
     */
    @Nullable
    private static String foreignOwner(Connection c, String parent, String table) throws SQLException {
        var problem = ownerProblem(c, parent, table);
        if (problem != null) {
            if (LogLimit.due(handoffWarned.computeIfAbsent(parent,
                    p -> new AtomicLong(System.nanoTime() - HANDOFF_WARN_INTERVAL)), HANDOFF_WARN_INTERVAL)) {
                log.warn("outbox {}; this WARN repeats at most once an hour", problem);
            }
            return null;
        }
        try (var ps = c.prepareStatement("select quote_ident(pg_get_userbyid(relowner)) from pg_class"
                + " where oid = to_regclass(?) and pg_get_userbyid(relowner) <> current_user")) {
            ps.setString(1, parent);
            try (var rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /**
     * Gives {@code parent}'s owner the tables in its schema named {@code <parent><suffix>} ({@code suffix} a regex),
     * attached or not, that the current role owns while the parent's owner is another role: what a member made before
     * {@link #create} gave each new table to the owner. Only a table's owner may give it away, so each instance's role
     * hands over its own; when it cannot ({@link #ownerProblem}), they stay, with {@link #foreignOwner}'s WARN. The
     * {@code OWNER TO} takes ACCESS EXCLUSIVE on the table, which a queued wait would hold every writer routed to it
     * and every reader of the parent behind: one short transaction each takes it {@code NOWAIT}, as
     * {@link XidPartitions#dropPending} does ({@link XidPartitions#nowaitPipeline}), tried every millisecond for
     * {@link XidPartitions#NOWAIT_WINDOW}, other lock waits cut at {@link XidPartitions#HOLD_LOCK_TIMEOUT} and the hold
     * at {@link XidPartitions#DAY_HOLD}, the commit sent with it. A table
     * no try got (behind a long reader), or whose handover failed, is logged and left to the next run.
     */
    static void reown(Connection c, String parent, String suffix) throws SQLException {
        var tables = new ArrayList<String>();
        try (var ps = c.prepareStatement("select t.oid::regclass::text from pg_class p join pg_class t on"
                + " t.relnamespace = p.relnamespace and t.relowner <> p.relowner where p.oid = to_regclass(?)"
                + " and t.relkind = 'r' and t.relname ~ ? and pg_get_userbyid(t.relowner) = current_user")) {
            ps.setString(1, parent);
            ps.setString(2, rangePattern(parent, suffix));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (!dropped(rs.getString(1))) {
                        tables.add(unquoted(rs.getString(1)));
                    }
                }
            }
        }
        var owner = tables.isEmpty() ? null : foreignOwner(c, parent, "the tables of " + parent);
        if (owner == null) {
            return;
        }
        for (var table : tables) {
            var sql = XidPartitions.nowaitPipeline("table " + name(table), Duration.ZERO, "alter table " + name(table)
                    + " owner to " + owner + "; perform set_config('" + REOWN_RESULT + "', 'given', true);",
                    XidPartitions.DAY_HOLD, REOWN_RESULT);
            try {
                var result = XidPartitions.pipelined(c, sql, null);
                if (!result.equals("given")) {
                    log.warn("outbox could not give table {} to {}, the owner of {}: a transaction held a lock on it at"
                            + " every try for {} ms; the next maintenance run retries", table, owner, parent,
                            XidPartitions.NOWAIT_WINDOW.toMillis());
                    continue;
                }
                log.info("gave table {} to {}, the owner of {}", table, owner, parent);
            } catch (SQLException e) {
                if (LockHolder.lost(e)) {
                    throw e; // the run's lock holder is gone: no more tables
                }
                log.warn("outbox could not give table {} to {}, the owner of {} (SQLState {}); the next maintenance"
                        + " run retries: {}", table, owner, parent, e.getSQLState(), e.getMessage());
            }
        }
    }

    /**
     * Why the current role cannot give a new partition {@code table} of {@code parent} to {@code parent}'s owner, with
     * the statement to run; null when it can, or when the owner is the current role or the parent is missing.
     */
    @Nullable
    static String ownerProblem(Connection c, String parent, String table) throws SQLException {
        // before 16 membership is enough to SET ROLE, and pg_has_role knows no 'SET'
        var set = c.getMetaData().getDatabaseMajorVersion() >= 16 ? "SET" : "MEMBER";
        try (var ps = c.prepareStatement("select pg_get_userbyid(p.relowner), n.nspname, r.rolsuper"
                + " or pg_has_role(p.relowner, '" + set + "'), r.rolsuper or has_schema_privilege(p.relowner,"
                + " p.relnamespace, 'CREATE') from pg_class p join pg_namespace n on n.oid = p.relnamespace,"
                + " pg_roles r where p.oid = to_regclass(?) and r.rolname = current_user"
                + " and pg_get_userbyid(p.relowner) <> current_user")) {
            ps.setString(1, parent);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                var owner = rs.getString(1);
                var schema = rs.getString(2);
                var role = role(c);
                var cannot = "role " + role + " cannot make " + table + " belong to " + owner + ", the owner of "
                        + parent + "; until it can, the tables it creates stay its own, and the owner's other members"
                        + " (per-instance credentials) cannot swap or drop them: ";
                if (!rs.getBoolean(3)) {
                    return cannot + "run GRANT " + owner + " TO " + role
                            + (set.equals("SET") ? " WITH SET TRUE" : "");
                }
                if (!rs.getBoolean(4)) {
                    return cannot + owner + " lacks CREATE on schema " + schema + "; run GRANT CREATE ON SCHEMA "
                            + schema + " TO " + owner;
                }
                return null;
            }
        }
    }

    /** The regex of the tables in {@code parent}'s schema named {@code <parent><suffix>}. */
    private static String rangePattern(String parent, String suffix) {
        return "^" + parent.substring(parent.indexOf('.') + 1) + suffix + "$";
    }

    static String role(Connection c) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select current_user")) {
            rs.next();
            return rs.getString(1);
        }
    }

    /** Attaches the standalone {@code table} to {@code parent} {@code FOR VALUES FROM (from) TO (to)}. */
    static void attach(Connection c, String parent, String table, String from, @Nullable String to,
            Duration lockTimeout) throws SQLException {
        String bounds = bounds(from, to);
        execute(c, lockTimeout, "alter table " + name(parent) + " attach partition " + name(table) + " " + bounds);
        log.info("attached partition {} to {} {}", table, parent, bounds);
    }

    public static void detachConcurrentlyAndDrop(Connection c, String parent, String partition, Duration lockTimeout)
            throws SQLException {
        detach(c, parent, partition, lockTimeout);
        drop(c, partition, lockTimeout);
    }

    /**
     * Whether {@code table} has what an attach to {@code parent} requires of it: the same column names, types, NOT
     * NULLs and generation expressions in the same order, and the same CHECK constraints by name and expression: one
     * the parent has NOT VALID is copied validated, which an attach accepts.
     */
    static boolean sameShape(Connection c, String parent, String table) throws SQLException {
        String shape = "(select array_agg(attname::text || ' ' || atttypid::regtype::text || ' ' || attnotnull::text"
                + " || ' ' || attgenerated::text || ' ' || coalesce(pg_get_expr(d.adbin, d.adrelid), '')"
                + " order by attnum)"
                + " from pg_attribute left join pg_attrdef d on d.adrelid = attrelid and d.adnum = attnum"
                + " and attgenerated <> '' where attrelid = to_regclass(?) and attnum > 0 and not attisdropped)"
                + " || (select array_agg(conname::text || ' ' || pg_get_expr(conbin, conrelid) order by conname)"
                + " from pg_constraint where conrelid = to_regclass(?) and contype = 'c')";
        try (var ps = c.prepareStatement("select " + shape + " is not distinct from " + shape)) {
            ps.setString(1, parent);
            ps.setString(2, parent);
            ps.setString(3, table);
            ps.setString(4, table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /**
     * Partitions of {@code parent} in no particular order, including ones whose detach is still pending, but not one
     * another session dropped after this query's snapshot ({@link #dropped}).
     */
    public static List<Bound> listRanges(Connection c, String parent) throws SQLException {
        var bounds = new ArrayList<Bound>();
        try (var ps = c.prepareStatement("select c.oid::regclass::text, pg_get_expr(c.relpartbound, c.oid)"
                + " from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = to_regclass(?)")) {
            ps.setString(1, parent);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var name = unquoted(rs.getString(1));
                    if (dropped(name) || rs.getString(2) == null) { // pg_get_expr: null once dropped
                        continue;
                    }
                    var m = BOUNDS.matcher(rs.getString(2));
                    if (!m.matches()) {
                        throw new IllegalStateException("not a range partition: " + name);
                    }
                    bounds.add(new Bound(name, value(m.group(1)), value(m.group(2))));
                }
            }
        }
        return bounds;
    }

    /** The partition of {@code parent} whose detach is pending, or null: Postgres allows at most one per parent. */
    @Nullable
    static String pendingDetach(Connection c, String parent) throws SQLException {
        try (var ps = c.prepareStatement("select inhrelid::regclass::text from pg_inherits"
                + " where inhparent = to_regclass(?) and inhdetachpending")) {
            ps.setString(1, parent);
            try (var rs = ps.executeQuery()) {
                return rs.next() && !dropped(rs.getString(1)) ? unquoted(rs.getString(1)) : null;
            }
        }
    }

    static Attachment attachment(Connection c, String parent, String table) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?) is not null, (select inhdetachpending from pg_inherits"
                + " where inhrelid = to_regclass(?) and inhparent = to_regclass(?))")) {
            ps.setString(1, table);
            ps.setString(2, table);
            ps.setString(3, parent);
            try (var rs = ps.executeQuery()) {
                rs.next();
                if (!rs.getBoolean(1)) {
                    return Attachment.MISSING;
                }
                boolean pending = rs.getBoolean(2);
                return rs.wasNull() ? Attachment.STANDALONE
                        : pending ? Attachment.DETACH_PENDING : Attachment.ATTACHED;
            }
        }
    }

    /**
     * {@code DETACH ... CONCURRENTLY} ({@link #executeConcurrently}, its wait cut at {@code lockTimeout}), or
     * {@code FINALIZE} when an earlier detach was interrupted (lock timeout, crash, a role's {@code statement_timeout})
     * and left the partition pending; no-op when the partition is not attached. A {@code FINALIZE} that timed
     * out (55P03) waited for the sessions of this database holding an older snapshot, whatever they read: the 55P03
     * again, naming the oldest of them ({@link #OLDEST_SNAPSHOTS}).
     */
    static void detach(Connection c, String parent, String partition, Duration lockTimeout) throws SQLException {
        var attachment = attachment(c, parent, partition);
        if (attachment == Attachment.ATTACHED || attachment == Attachment.DETACH_PENDING) {
            try {
                var detach = "alter table " + name(parent) + " detach partition " + name(partition);
                if (attachment == Attachment.ATTACHED) {
                    executeConcurrently(c, lockTimeout, detach + " concurrently", null);
                } else {
                    execute(c, lockTimeout, detach + " finalize"); // in a transaction block too, unlike CONCURRENTLY
                }
            } catch (SQLException e) {
                if (attachment != Attachment.DETACH_PENDING || !LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                    throw e;
                }
                throw new SQLException(e.getMessage() + "; FINALIZE of " + partition + " waits for every older"
                        + " snapshot in this database, of any table: " + oldestSnapshots(c), e.getSQLState(), e);
            }
            log.info("detached partition {} from {}", partition, parent);
        }
    }

    private static String oldestSnapshots(Connection c) {
        try (var st = c.createStatement(); var rs = st.executeQuery(OLDEST_SNAPSHOTS)) {
            rs.next();
            return rs.getLong(1) + " sessions hold one right after, the oldest: " + rs.getString(2);
        } catch (SQLException | RuntimeException read) {
            return "who holds them could not be read: " + read;
        }
    }

    static void drop(Connection c, String table, Duration lockTimeout) throws SQLException {
        if (exists(c, table)) {
            execute(c, lockTimeout, "drop table " + name(table));
            log.info("dropped partition {}", table);
        }
    }

    /**
     * For a table that is no longer a partition: in one transaction under {@code ACCESS EXCLUSIVE}, drops it when
     * empty, otherwise attaches it back {@code FOR VALUES FROM (from) TO (to)}. The lock waits for a writer that was
     * routed to the table before its detach completed, so a row committed by that writer is seen, never dropped.
     * Each statement runs under {@code statementTimeout}, the caller's: the attach scans the whole table to check its
     * bounds. Returns true when the table was dropped.
     */
    static boolean dropIfEmptyElseAttach(Connection c, String parent, String table, String from, @Nullable String to,
            Duration lockTimeout, Duration statementTimeout) throws SQLException {
        return SocketBound.atLeast(c, statementTimeout,
                () -> dropIfEmptyElseAttachBounded(c, parent, table, from, to, lockTimeout, statementTimeout));
    }

    private static boolean dropIfEmptyElseAttachBounded(Connection c, String parent, String table, String from,
            @Nullable String to, Duration lockTimeout, Duration statementTimeout) throws SQLException {
        boolean empty = transaction(c, workIdle(lockTimeout), () -> {
            try (var st = c.createStatement()) {
                setLocalTimeouts(c, lockTimeout, statementTimeout);
                st.execute("lock table " + name(table) + " in access exclusive mode");
                boolean none;
                try (var rs = st.executeQuery("select 1 from " + name(table) + " limit 1")) {
                    none = !rs.next();
                }
                st.execute(none
                        ? "drop table " + name(table)
                        : "alter table " + name(parent) + " attach partition " + name(table) + " " + bounds(from, to));
                return none;
            }
        });
        if (empty) {
            log.info("dropped partition {}", table);
        } else {
            log.info("attached {} back to {} {}: it holds rows", table, parent, bounds(from, to));
        }
        return empty;
    }

    /**
     * Tables in {@code parent}'s schema named {@code <parent><suffix>} ({@code suffix} a regex) that are no partition:
     * what a run leaves when it dies, or its DROP fails, after its detach went through. Usable in DDL as is; not one
     * another session dropped after this query's snapshot ({@link #dropped}).
     */
    static List<String> standalone(Connection c, String parent, String suffix) throws SQLException {
        var tables = new ArrayList<String>();
        try (var ps = c.prepareStatement("select c.oid::regclass::text from pg_class c where c.relkind = 'r'"
                + " and c.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?))"
                + " and c.relname ~ ? and not exists (select 1 from pg_inherits i where i.inhrelid = c.oid)"
                + " order by c.relname")) {
            ps.setString(1, parent);
            ps.setString(2, rangePattern(parent, suffix));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (!dropped(rs.getString(1))) {
                        tables.add(unquoted(rs.getString(1)));
                    }
                }
            }
        }
        return tables;
    }

    /**
     * {@code regclass}, an {@code oid::regclass::text}, as a session prints it without {@code quote_all_identifiers}:
     * with it, the library's own names read {@code "schema"."table_x0"}, which they are never written as.
     */
    static String unquoted(String regclass) {
        return NEEDLESS_QUOTES.matcher(regclass).replaceAll("$1");
    }

    /**
     * Whether {@code regclass}, an {@code oid::regclass::text} of a catalog row this query's snapshot still shows,
     * names a relation another session has dropped since: {@code regclass} reads the current catalog and prints such
     * an oid bare, while a live relation's name never is digits alone (it would be quoted).
     */
    static boolean dropped(String regclass) {
        return !regclass.isEmpty() && regclass.chars().allMatch(Character::isDigit);
    }

    /**
     * Drops {@code table} unless {@code find} ({@code {table}} for the table, one bind parameter: {@code bound}, none
     * when null) returns a row; the lock and statement timeouts of every other DDL statement. {@code find} runs first
     * under {@code ACCESS SHARE}, so a table that stays (someone may be reading it) is never locked exclusively; only
     * a table to drop is, and {@code find} runs again under that lock in the same transaction, so nothing written
     * between the two is lost: the transaction is read committed whatever the session default, so that second read
     * takes a new snapshot. Taking the exclusive lock over the shared one can deadlock with someone who reads the
     * table and then alters or drops it in one transaction; Postgres then aborts one side (40P01), and a failed
     * maintenance step is retried next run. Returns true when dropped.
     */
    static boolean dropUnlessFound(
            Connection c, String table, String find, @Nullable String bound, Duration lockTimeout)
            throws SQLException {
        return SocketBound.atLeast(c, lockTimeout.plus(DDL_MARGIN),
                () -> dropUnlessFoundBounded(c, table, find, bound, lockTimeout));
    }

    private static boolean dropUnlessFoundBounded(
            Connection c, String table, String find, @Nullable String bound, Duration lockTimeout)
            throws SQLException {
        // read committed (transaction): a repeatable-read default would fix the snapshot at the first read, and the
        // second would miss rows
        boolean found = transaction(c, workIdle(lockTimeout), () -> {
            try (var st = c.createStatement()) {
                setLocalTimeouts(c, lockTimeout, lockTimeout.plus(DDL_MARGIN));
                if (finds(c, table, find, bound)) {
                    return true;
                }
                st.execute("lock table " + name(table) + " in access exclusive mode");
                if (finds(c, table, find, bound)) {
                    return true;
                }
                st.execute("drop table " + name(table));
                return false;
            }
        });
        if (!found) {
            log.info("dropped partition {}, detached earlier but never dropped", table);
        }
        return !found;
    }

    private static boolean finds(Connection c, String table, String find, @Nullable String bound) throws SQLException {
        try (var ps = c.prepareStatement(find.replace("{table}", name(table)))) {
            if (bound != null) {
                ps.setString(1, bound);
            }
            try (var rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    static boolean exists(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?) is not null")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    static boolean hasRows(Connection c, String table) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery("select 1 from " + name(table) + " limit 1")) {
            return rs.next();
        }
    }

    /**
     * The first minor of {@code major} with the fix for crashes and "could not open relation" errors of queries
     * (prepared ones above all) on a partitioned table concurrent with {@code DETACH PARTITION … CONCURRENTLY} and an
     * immediate drop of the partition, which {@link #detachConcurrentlyAndDrop} does; null when {@code major.minor}
     * has it. 17.0 shipped with it.
     */
    @Nullable
    static String detachDropFix(int major, int minor) {
        int fixed = switch (major) {
            case 14 -> 14;
            case 15 -> 9;
            case 16 -> 5;
            default -> 0;
        };
        return minor < fixed ? major + "." + fixed : null;
    }

    /** One WARN for {@code what} when the server of {@code c} lacks {@link #detachDropFix}; it does not refuse. */
    static void warnIfDetachDropUnfixed(Connection c, String what) throws SQLException {
        var meta = c.getMetaData(); // pgjdbc parses server_version: no query
        var fix = detachDropFix(meta.getDatabaseMajorVersion(), meta.getDatabaseMinorVersion());
        if (fix != null) {
            log.warn("{}: Postgres {}.{} can crash a backend (which restarts every session of the server) or fail a"
                    + " query with \"could not open relation\" when a query on a partitioned table runs concurrently"
                    + " with DETACH PARTITION CONCURRENTLY and an immediate drop of the partition, which outbox"
                    + " maintenance does for every expired range; {} fixes it: upgrade to {} or newer", what,
                    meta.getDatabaseMajorVersion(), meta.getDatabaseMinorVersion(), fix, fix);
        }
    }

    /**
     * Runs {@code sql} under the class's per-statement timeouts, all {@code SET LOCAL}: on an autocommit connection in a
     * transaction of its own ({@link #transaction}), inside the caller's transaction with the transaction's own values
     * given back after it. On Postgres 17+ also with {@code transaction_timeout} off, which a role or database default
     * would otherwise apply to the transaction too.
     */
    static void execute(Connection c, Duration lockTimeout, String sql) throws SQLException {
        execute(c, lockTimeout, lockTimeout.plus(DDL_MARGIN), sql);
    }

    /** {@link #execute(Connection, Duration, String)} with its own {@code statementTimeout}, zero for none. */
    static void execute(Connection c, Duration lockTimeout, Duration statementTimeout, String sql)
            throws SQLException {
        withTimeouts(c, lockTimeout, statementTimeout, () -> {
            Sql.execute(c, sql);
            return null;
        });
    }

    /**
     * {@code body} with {@code lock_timeout} and {@code statement_timeout} set (and, on Postgres 17+,
     * {@code transaction_timeout} off) for each of its statements by {@code SET LOCAL}: on an autocommit connection in
     * a transaction of its own ({@link #transaction}), which commits after it; inside the caller's transaction with
     * the transaction's own values given back afterwards.
     */
    static <T> T withTimeouts(Connection c, Duration lockTimeout, Duration statementTimeout,
            Sql.SqlFunction0<T> body) throws SQLException {
        return SocketBound.atLeast(c, statementTimeout, () -> c.getAutoCommit()
                ? transaction(c, Maintenance.ABANDONED_AFTER, () -> {
                    setLocalTimeouts(c, lockTimeout, statementTimeout);
                    return body.apply();
                })
                : withLocalTimeouts(c, lockTimeout, statementTimeout, body));
    }

    /**
     * {@code body} in a transaction of its own on the autocommit connection {@code c}, given back in autocommit: read
     * committed whatever the session's default (so each statement takes its snapshot, a check under a lock included),
     * {@code idle_in_transaction_session_timeout} {@code idle} (a client silent that long inside it froze or lost its
     * connection, and Postgres ends the session, freeing its locks) and, on Postgres 17+, {@code transaction_timeout}
     * off, all {@code SET LOCAL}; fenced by the holder bound to this thread ({@link LockHolder#fenceCurrent}). Commits
     * after {@code body}, which may have committed itself in its last statement (a pipeline ending in {@code COMMIT},
     * so the server ends a hold without waiting for this client: the commit here then sends nothing); rolls back when
     * it throws.
     */
    static <T> T transaction(Connection c, Duration idle, Sql.SqlFunction0<T> body) throws SQLException {
        return Sql.inTransaction(c, () -> {
            // first: isolation must precede every query of the transaction
            Sql.execute(c, "set transaction isolation level read committed; set local"
                    + " idle_in_transaction_session_timeout = " + idle.toMillis() + Ticks.noTransactionTimeout(c));
            LockHolder.fenceCurrent(c);
            return body.apply();
        });
    }

    /**
     * {@code sql}, a statement Postgres runs only alone and outside a transaction block (a concurrent detach or index
     * build), as one autocommit statement with nothing set for the session, so behind a transaction-pooling proxy
     * nothing outlives it into another client's session. So the role's and database's {@code lock_timeout},
     * {@code statement_timeout} and {@code transaction_timeout} apply, which the library cannot lift for it there: a
     * statement they cut fails as the server says, and its caller's next run retries it. Its own bound is a cancel
     * from this client once it has run {@code waitBound} (zero: none): a JDBC query timeout, which a proxy forwards to
     * the server session running it, surfaced as SQLState 55P03, as a {@code lock_timeout} would be. Fenced first by
     * the holder bound to this thread, which cancels it once lost ({@link LockHolder#running}); {@code started}, if
     * given, gets the statement before it runs, for a cancel of its own.
     */
    static void executeConcurrently(Connection c, Duration waitBound, String sql,
            @Nullable Sql.SqlConsumer<java.sql.Statement> started) throws SQLException {
        LockHolder.fenceCurrent(c);
        SocketBound.atLeast(c, waitBound, () -> {
            try (var st = c.createStatement()) {
                if (!waitBound.isZero()) {
                    st.setQueryTimeout((int) Math.max(1, (waitBound.toMillis() + 999) / 1000));
                }
                if (started != null) {
                    started.accept(st);
                }
                LockHolder.running(st);
                long start = System.nanoTime();
                try {
                    st.execute(sql);
                } catch (SQLException e) {
                    if (QUERY_CANCELED.equals(e.getSQLState()) && !waitBound.isZero()
                            && System.nanoTime() - start >= waitBound.toNanos()) {
                        throw new SQLException(e.getMessage() + " (a lock timeout: the library cut it after waiting "
                                + waitBound.toMillis() + " ms): " + sql, LOCK_NOT_AVAILABLE, e);
                    }
                    throw e;
                } finally {
                    LockHolder.running(null);
                }
            }
            return null;
        });
    }

    /** {@code SET LOCAL} of both timeouts, in one statement. */
    static void setLocalTimeouts(Connection c, Duration lock, Duration statement) throws SQLException {
        Sql.execute(c, "set local lock_timeout = " + lock.toMillis() + "; set local statement_timeout = "
                + statement.toMillis());
    }

    private static <T> T withLocalTimeouts(Connection c, Duration lockTimeout, Duration statementTimeout,
            Sql.SqlFunction0<T> body) throws SQLException {
        String lock;
        String statement;
        String transaction;
        try (var st = c.createStatement(); var rs = st.executeQuery(SESSION_TIMEOUTS)) {
            rs.next();
            lock = rs.getString(1);
            statement = rs.getString(2);
            transaction = rs.getString(3);
        }
        boolean pg17 = c.getMetaData().getDatabaseMajorVersion() >= 17; // before it, no such setting
        T result;
        try (var st = c.createStatement()) {
            st.execute("set local lock_timeout = " + lockTimeout.toMillis());
            st.execute("set local statement_timeout = " + statementTimeout.toMillis());
            if (pg17) {
                st.execute("set local transaction_timeout = 0");
            }
            result = body.apply();
        } catch (SQLException | RuntimeException e) {
            try {
                restore(c, lock, statement, transaction, pg17);
            } catch (SQLException cleanup) { // a terminated session: keep its cause on top
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        restore(c, lock, statement, transaction, pg17);
        return result;
    }

    private static void restore(Connection c, @Nullable String lock, @Nullable String statement,
            @Nullable String transaction, boolean pg17) throws SQLException {
        restore(c, "lock_timeout", lock);
        restore(c, "statement_timeout", statement);
        if (pg17) {
            restore(c, "transaction_timeout", transaction);
        }
    }

    /** {@code SET LOCAL} back to what the transaction had: its own {@code SET LOCAL}, else the session's default. */
    private static void restore(Connection c, String setting, @Nullable String sessionValue) throws SQLException {
        if (sessionValue == null) {
            Sql.execute(c, "set local " + setting + " to default");
            return;
        }
        try (var ps = c.prepareStatement("select set_config(?, ?, true)")) {
            ps.setString(1, setting);
            ps.setString(2, sessionValue);
            ps.execute();
        }
    }

    static String bounds(String from, @Nullable String to) {
        return "for values from (" + literal(from) + ") to (" + (to == null ? "maxvalue" : literal(to)) + ")";
    }

    /** {@code 'text'} → {@code text}; {@code MINVALUE}/{@code MAXVALUE} → null. */
    @Nullable
    private static String value(String expr) {
        return expr.startsWith("'") ? expr.substring(1, expr.length() - 1) : null;
    }

    private static String literal(String value) {
        if (!VALUE.matcher(value).matches()) {
            throw new IllegalArgumentException("unsafe partition bound: " + value);
        }
        return "'" + value + "'";
    }

    private static String name(String name) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("unsafe table name: " + name);
        }
        return name;
    }
}
