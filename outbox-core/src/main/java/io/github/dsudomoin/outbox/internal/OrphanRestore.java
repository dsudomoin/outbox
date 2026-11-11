package io.github.dsudomoin.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;
import io.github.dsudomoin.outbox.internal.Sql.SqlConsumer;

/**
 * Moves the rows of a standalone outbox range back into the outbox, then drops the range. Moved rows are new messages
 * ({@link Resender#insertFrom}: new id and xid, the same {@code message_id} and headers, the shard recomputed, through
 * the key locks {@code send} takes), so they are delivered after later messages of their keys. Their payloads and
 * headers are copied inside the database and never come into Java.
 */
public final class OrphanRestore {

    /** Rows, so key locks, per chunk transaction: far below {@code max_locks_per_transaction} × connections. */
    public static final int CHUNK = 1000;
    /** Keys held through a whole call whose lock the call then waits for, one transaction each. */
    static final int HELD_KEYS_WAITED = 8;

    private final TableNames names;
    private final Resender resender;
    private final Duration keyLockTimeout;
    /**
     * {@code <base>_x<digits>}, or {@code <base>_o<digits>} as the xid-regression recovery sets one aside, unqualified
     * (as listed on the search_path) or qualified as the outbox is named.
     */
    private final Pattern range;
    private final String unmarkDropping;
    /** For tests: runs after each chunk's commit, to end the session there. */
    @Nullable
    SqlConsumer<Connection> afterChunk;
    /** Replaces {@link PartitionDdl#DDL_MARGIN} in the statement bounds, for tests. */
    Duration margin = PartitionDdl.DDL_MARGIN;
    /** Replaces {@link XidPartitions#ATTACH_SCAN_RATE}, for tests. */
    long scanRate = XidPartitions.ATTACH_SCAN_RATE;

    /**
     * {@code keyLockTimeout} bounds each lock wait of a chunk's insert (key locks are only tried, never waited for), so
     * a chunk stalls a writer of a key it holds no longer than that: pass {@link XidPartitions#lockWait}.
     */
    public OrphanRestore(TableNames names, Resender resender, Duration keyLockTimeout) {
        this.names = names;
        this.resender = resender;
        this.keyLockTimeout = keyLockTimeout;
        var base = names.base();
        var unqualified = names.unqualified(base);
        this.range = Pattern.compile("(?:" + Pattern.quote(base.substring(0, base.length() - unqualified.length()))
                + ")?" + Pattern.quote(unqualified) + "_[xo][0-9]+");
        this.unmarkDropping = "delete from " + names.dropping() + " where table_name = ?";
    }

    /**
     * Moves the rows of {@code table} with an xid at or above {@code fromXid} back into the outbox, then drops it:
     * <b>its rows below {@code fromXid} go with the table</b>. First {@code create index if not exists <table>_id},
     * which, as each chunk's read before its key locks, may take {@code lockTimeout} + 60 s + a second per
     * {@link XidPartitions#ATTACH_SCAN_RATE} bytes of the table (a cut one fails the call with 57014 and says what to
     * run by hand); then chunks of {@link #CHUNK} rows in id order, each one read-committed transaction that holds
     * ACCESS EXCLUSIVE on {@code table} alone, never on the outbox, waited for at most {@code lockTimeout}, idle in it
     * at most
     * {@code lockTimeout} + 30 s ({@code idle_in_transaction_session_timeout}): it inserts the rows and
     * deletes them from {@code table} together, so a restore that dies loses and repeats nothing, and the next call
     * resumes it. Its key locks are tried without waiting ({@link Resender#tryKeyLocks}): the rows of a key another
     * transaction holds (an open {@code send} of it) stay, and so do that key's rows in the later chunks of the call,
     * so its rows still go back in id order, behind that send; the other rows of the chunk go on. Any other lock the
     * insert waits for is waited for at most {@code keyLockTimeout}. The chunk that finds fewer rows, the last, also
     * drops {@code table} and deletes its {@code <base>_dropping} marker, unless rows stayed. Then, so that a key held
     * almost all the time still gets through, the first {@link #HELD_KEYS_WAITED} keys that stayed are waited for,
     * one transaction each: its key lock waited for at most {@code keyLockTimeout} (only that key's writers wait
     * meanwhile, at most one such wait), then up to {@link #CHUNK} of its rows moved by the ids the chunks kept,
     * through {@code <table>_id}, so the key is held for its rows alone, not for a scan of the table; the table
     * dropped with the last of them. Rows still left fail the call with SQLState 55P03, and the next run or call
     * moves them. A lock timeout (55P03) on the table or on another lock rolls back the chunk it hits, rows and locks.
     * Returns the rows moved.
     *
     * @throws IllegalArgumentException before any SQL that names it, unless {@code table} is {@code <base>_x<digits>}
     *     or {@code <base>_o<digits>}, unqualified or qualified as the outbox is named, and a table in the outbox's
     *     schema that is no partition, or before any SQL at all for a negative {@code fromXid}
     */
    public long restore(Connection autocommit, String table, long fromXid, Duration lockTimeout) throws SQLException {
        if (fromXid < 0) {
            // xid8 parses a negative number as one above every real xid: the cut would move nothing and drop every row
            throw new IllegalArgumentException("fromXid " + fromXid + " is below 0: xids are never negative, and 0"
                    + " moves every row");
        }
        return SocketBound.atLeast(autocommit, lockTimeout.plus(margin),
                () -> restoreBounded(autocommit, table, fromXid, lockTimeout));
    }

    private long restoreBounded(Connection autocommit, String table, long fromXid, Duration lockTimeout)
            throws SQLException {
        if (!range.matcher(table).matches() || !standalone(autocommit, table)) {
            throw new IllegalArgumentException(table + " is not a standalone range of the outbox " + names.base()
                    + ": a table " + names.partitionPrefix() + "<digits> or " + names.setAsidePrefix() + "<digits> in"
                    + " its schema and in no partition tree");
        }
        long bytes = XidPartitions.relationSize(autocommit, table);
        // the index build and the first chunk's walk past the rows below fromXid each read the whole table: time for
        // that at scanRate. Neither holds a writer back (ACCESS EXCLUSIVE on the standalone table alone, no key lock)
        var scan = lockTimeout.plus(margin).plusMillis(bytes * 1000 / scanRate);
        String index = "create index if not exists " + names.unqualified(table) + "_id on " + table + " (id)";
        try {
            return SocketBound.atLeast(autocommit, scan,
                    () -> move(autocommit, table, fromXid, lockTimeout, scan, index));
        } catch (SQLException e) {
            if (!"57014".equals(e.getSQLState())) {
                throw e;
            }
            throw new SQLException("restoring " + table + " reads all its " + XidPartitions.mb(bytes) + " MB to"
                    + " build its id index and to find the rows at or above xid " + fromXid + ", and the server cut a"
                    + " statement of it at the " + scan.toMillis() / 1000.0 + " s it was given (" + e.getMessage()
                    + "): every call is given the same, so calling again is cut the same way. Rows moved so far stay"
                    + " moved. In a session without a statement_timeout, which blocks no writer, run " + index
                    + ", then call again; if it is still cut, move the rest by hand: insert into " + names.base()
                    + " (shard, message_id, topic, key, payload, headers) select shard, message_id, topic, key,"
                    + " payload, headers from " + table + " where xid >= '" + fromXid + "'::xid8, then drop table "
                    + table, "57014", e);
        }
    }

    private long move(Connection autocommit, String table, long fromXid, Duration lockTimeout, Duration scan,
            String index) throws SQLException {
        PartitionDdl.execute(autocommit, lockTimeout, scan, index);
        String chunk = "select id, topic, key from " + table
                + " where xid >= ?::text::xid8 and id > ? order by id limit " + CHUNK;
        long moved = 0;
        long after = Long.MIN_VALUE;
        boolean last = false;
        var busy = new HashSet<Integer>(); // key hashes another transaction held: theirs stay, in every later chunk
        // the first HELD_KEYS_WAITED busy hashes, in the order first met: the ids of up to CHUNK of their rows, by id
        var held = new LinkedHashMap<Integer, ArrayList<Long>>();
        long left = 0;
        var c = autocommit;
        while (!last) {
            var ids = new ArrayList<Long>(CHUNK);
            var rows = new ArrayList<Resender.Source>(CHUNK);
            int read;
            c.setAutoCommit(false);
            try (var st = c.createStatement()) {
                // first in the transaction: read committed, so the read takes its snapshot after the lock
                st.execute("set transaction isolation level read committed");
                st.execute("set local lock_timeout = " + lockTimeout.toMillis());
                // as relay steps do: Postgres ends a frozen holder's session, and with it the table's lock and a
                // session lock it holds (the maintenance lock of a restoreOrphan call)
                st.execute("set local idle_in_transaction_session_timeout = "
                        + lockTimeout.plusSeconds(30).toMillis());
                // the first chunk's read walks the rows below fromXid
                st.execute("set local statement_timeout = " + scan.toMillis() + Ticks.noTransactionTimeout(c));
                st.execute("lock table " + table + " in access exclusive mode");
                try (var ps = c.prepareStatement(chunk)) {
                    ps.setString(1, Long.toString(fromXid));
                    ps.setLong(2, after);
                    try (var rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getLong(1));
                            rows.add(new Resender.Source(rs.getLong(1), rs.getString(2), rs.getBytes(3)));
                        }
                    }
                }
                read = rows.size();
                if (!ids.isEmpty()) {
                    after = ids.get(ids.size() - 1);
                }
                // from here on, writers may queue behind this chunk's key locks: wait no longer than the swap may
                st.execute("set local lock_timeout = " + keyLockTimeout.toMillis());
                st.execute("set local statement_timeout = " + lockTimeout.plus(margin).toMillis());
                var hashes = rows.stream().map(r -> r.key() == null ? null : Hashing.keyHash(r.topic(), r.key()))
                        .toList();
                busy.addAll(resender.tryKeyLocks(c,
                        hashes.stream().filter(h -> h != null && !busy.contains(h)).toList()));
                for (int i = 0; i < rows.size(); i++) {
                    var hash = hashes.get(i);
                    if (hash != null && busy.contains(hash)) {
                        var kept = held.get(hash);
                        if (kept == null && held.size() < HELD_KEYS_WAITED) {
                            held.put(hash, kept = new ArrayList<>());
                        }
                        if (kept != null && kept.size() < CHUNK) {
                            kept.add(ids.get(i));
                        }
                    }
                }
                for (int i = rows.size() - 1; i >= 0; i--) {
                    if (hashes.get(i) != null && busy.contains(hashes.get(i))) {
                        rows.remove(i);
                        ids.remove(i);
                        left++;
                    }
                }
                resender.insertFrom(c, table, rows);
                try (var ps = c.prepareStatement("delete from " + table + " where id = any(?)")) {
                    ps.setArray(1, c.createArrayOf("int8", ids.toArray()));
                    ps.executeUpdate();
                }
                last = read < CHUNK;
                // under the lock still: no row can reach the table between this read and the drop
                if (last && left == 0) {
                    st.execute("drop table " + table);
                    try (var ps = c.prepareStatement(unmarkDropping)) {
                        ps.setString(1, names.unqualified(table));
                        ps.executeUpdate();
                    }
                }
                c.commit();
            } catch (Throwable e) { // an Error too: setAutoCommit(true) would commit the chunk so far
                try {
                    c.rollback();
                    c.setAutoCommit(true);
                } catch (SQLException cleanup) { // a terminated session, its transaction gone with it
                    e.addSuppressed(cleanup);
                }
                throw e;
            }
            c.setAutoCommit(true);
            moved += rows.size();
            if (afterChunk != null) {
                afterChunk.accept(c);
            }
        }
        boolean dropped = left == 0;
        for (var e : held.entrySet()) {
            if (dropped) {
                break;
            }
            long m = moveHeld(c, table, lockTimeout, e.getKey(), e.getValue(), left);
            moved += m;
            left -= m;
            dropped = left == 0;
        }
        if (!dropped) {
            throw new SQLException("moved " + moved + " rows of " + table + " back into the outbox; "
                    + Math.max(left, 1) + " rows of keys another transaction held (an open send of the key) stay in"
                    + " it, so that each key's rows go back in order behind that send: the next run or call moves"
                    + " them", "55P03");
        }
        return moved;
    }

    /**
     * The rows {@code ids} of the key hash {@code hash}, which the chunks left, in id order, in one transaction as a
     * chunk's, once its key lock is had within {@code keyLockTimeout}; none when it is not. Read by id through
     * {@code <table>_id}, so the key is held for its rows alone, not for a scan of the table. Drops the table when they
     * are the {@code left} rows the chunks left. Returns the rows moved.
     */
    private long moveHeld(Connection c, String table, Duration lockTimeout, int hash, ArrayList<Long> ids, long left)
            throws SQLException {
        c.setAutoCommit(false);
        try (var st = c.createStatement()) {
            st.execute("set transaction isolation level read committed");
            st.execute("set local lock_timeout = " + lockTimeout.toMillis());
            st.execute("set local idle_in_transaction_session_timeout = " + lockTimeout.plusSeconds(30).toMillis());
            st.execute("set local statement_timeout = " + lockTimeout.plus(margin).toMillis()
                    + Ticks.noTransactionTimeout(c));
            st.execute("lock table " + table + " in access exclusive mode");
            st.execute("set local lock_timeout = " + keyLockTimeout.toMillis());
            try (var ps = c.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
                ps.setInt(1, names.keyNamespace());
                ps.setInt(2, hash);
                ps.execute();
            } catch (SQLException e) {
                if (!"55P03".equals(e.getSQLState())) {
                    throw e;
                }
                c.rollback();
                c.setAutoCommit(true);
                return 0;
            }
            var rows = new ArrayList<Resender.Source>();
            try (var ps = c.prepareStatement("select id, topic, key from " + table
                    + " where id = any(?) order by id")) {
                ps.setArray(1, c.createArrayOf("int8", ids.toArray()));
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(new Resender.Source(rs.getLong(1), rs.getString(2), rs.getBytes(3)));
                    }
                }
            }
            resender.insertFrom(c, table, rows); // the key's lock is ours: taken again without waiting
            try (var ps = c.prepareStatement("delete from " + table + " where id = any(?)")) {
                ps.setArray(1, c.createArrayOf("int8", ids.toArray()));
                ps.executeUpdate();
            }
            // nothing writes into a standalone range: the chunks counted every row they left, as the last chunk's drop
            // relies on, so no scan of the table under the key lock
            boolean empty = rows.size() == left;
            if (empty) {
                st.execute("drop table " + table);
                try (var ps = c.prepareStatement(unmarkDropping)) {
                    ps.setString(1, names.unqualified(table));
                    ps.executeUpdate();
                }
            }
            c.commit();
            c.setAutoCommit(true);
            return rows.size();
        } catch (Throwable e) { // an Error too: setAutoCommit(true) would commit the work so far
            try {
                c.rollback();
                c.setAutoCommit(true);
            } catch (SQLException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private boolean standalone(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select exists (select 1 from pg_class t where t.oid = to_regclass(?)"
                + " and t.relkind = 'r'"
                + " and t.relnamespace = (select p.relnamespace from pg_class p where p.oid = to_regclass(?))"
                + " and not exists (select 1 from pg_inherits i where i.inhrelid = t.oid))")) {
            ps.setString(1, table);
            ps.setString(2, names.base());
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }
}
