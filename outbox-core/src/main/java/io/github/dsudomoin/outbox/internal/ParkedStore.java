package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.internal.RetryStore.COLUMNS;
import static io.github.dsudomoin.outbox.internal.RetryStore.RECORD_COLUMNS;
import static io.github.dsudomoin.outbox.internal.RetryStore.errorText;
import static io.github.dsudomoin.outbox.internal.RetryStore.requireOneRow;
import static io.github.dsudomoin.outbox.internal.RetryStore.timestamp;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Action;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Dead;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Head;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Hold;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Keep;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Park;
import io.github.dsudomoin.outbox.internal.BatchClassifier.Unpark;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Requeued records in {@code <base>_parked}: unlike retry rows they block nothing; each comes back right before the
 * next record of its (topic, key) or at its {@code due_at}, or that of a newer parked row of its key that fits a call
 * ({@link #dueWithOlder}). The state is {@code due_at}, and in memory what limits a pull ({@link #pullNext}). Every method runs
 * on the caller's connection, so inside the shard step's transaction a rolled-back step leaves each record where it
 * was.
 */
public final class ParkedStore {

    private static final int PULLED_KEYS = 1024;

    private final Duration delay;
    private final int batch;
    private final String insert;
    private final String repark;
    private final String keep;
    private final String delete;
    private final String toRetry;
    private final String toDead;
    private final String fromRetry;
    private final String selectDue;
    private final String selectOfKeys;
    /**
     * When each key last had such a pull ({@link #pullNext}), newest {@link #PULLED_KEYS} keys, per instance: a lost
     * entry, or a pull on another instance, costs one more offer of the key's lowest rows and nothing else.
     */
    private final Map<Object, Instant> pulledOffTheBottom = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Object, Instant> eldest) {
            return size() > PULLED_KEYS;
        }
    };
    private final String fitting;
    private final String wholeKeys;
    private final String pullNext;
    private final String countOfKeys;
    private final String lowestOfKeys;
    private final String minDue;
    private final String size;

    /** For reads and moves of rows: nothing here that answers a handler. */
    public ParkedStore(TableNames names, RetryStore retries) {
        this(names, retries, Duration.ZERO, Integer.MAX_VALUE);
    }

    /** {@code delay}, {@code batch}: {@code relay.requeue.delay} and {@code relay.batchSize}. */
    public ParkedStore(TableNames names, RetryStore retries, Duration delay, int batch) {
        this.delay = delay;
        this.batch = batch;
        var parked = names.parked();
        var retry = names.retry();
        insert = "insert into " + parked + " (" + COLUMNS + ", attempts, requeues, due_at)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        repark = "update " + parked + " set requeues = ?, due_at = ? where id = ?";
        keep = "update " + parked + " set due_at = greatest(due_at, ?) where id = ?";
        delete = "delete from " + parked + " where id = any(?)";
        // every part sees the tables as before the statement; key = null matches nothing, so an unkeyed row moves alone
        toRetry = "with moved as (delete from " + parked + " where id = ? or (shard = ? and topic = ? and key = ?"
                + " and id > ?) returning " + COLUMNS + ", attempts, requeues),"
                + " demoted as (update " + retry + " set next_attempt_at = null, waiting_since = null where shard = ?"
                + " and topic = ? and key = ? and id > ? and next_attempt_at is not null),"
                + " head as (insert into " + retry + " (" + COLUMNS + ", attempts, next_attempt_at, last_error,"
                + " requeues, waiting_since) select " + COLUMNS + ", ?, ?, ?, requeues, ?::timestamptz from moved"
                + " where id = ? returning id),"
                + " followers as (insert into " + retry + " (" + COLUMNS + ", attempts, requeues)"
                + " select " + COLUMNS + ", attempts, requeues from moved where id <> ?)"
                + " select count(*) from head";
        toDead = retries.toDead("delete from " + parked + " where id = ? returning " + COLUMNS, "?", "?");
        fromRetry = "with moved as (delete from " + retry + " where id = ? returning " + COLUMNS + ", attempts)"
                + " insert into " + parked + " (" + COLUMNS + ", attempts, requeues, due_at)"
                + " select " + COLUMNS + ", attempts, ?, ? from moved";
        // in the order rows came due, by the index on it, so a row far along a key is reached however slow the handler
        selectDue = "select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and due_at <= ? order by due_at, id limit ?";
        // at most limit rows read per key: a key with thousands of parked rows costs no more than any other
        selectOfKeys = "select " + RECORD_COLUMNS + " from (select distinct t, b from unnest(?::text[], ?::bytea[])"
                + " u(t, b)) k cross join lateral (select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and topic = k.t and key = k.b order by id limit ?) p order by id limit ?";
        // each key's rows, up to the limit: past it the key is larger than a call, and how much larger does not matter
        fitting = "select k.t, k.b, n.c from (select distinct t, b from unnest(?::text[], ?::bytea[]) u(t, b)) k"
                + " cross join lateral (select count(*) c from (select 1 from " + parked
                + " where shard = ? and topic = k.t and key = k.b limit ?) l) n";
        // of each key that fits, all but its rows above the highest handed that are not due
        wholeKeys = "select " + RECORD_COLUMNS + " from unnest(?::text[], ?::bytea[], ?::int8[]) k(t, b, above)"
                + " cross join lateral (select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and topic = k.t and key = k.b and (id <= k.above or due_at <= ?)) p order by id";
        // ahead of every row of the shard, its earliest due time, so the rows of a key whose block ended go in their order
        // and no chunk of it already overdue passes them again
        pullNext = "with t as (select least(?::timestamptz, min(due_at)) ahead from " + parked + " where shard = ?)"
                + " update " + parked + " p set due_at = t.ahead from t where p.id in (select q.id from (select distinct"
                + " u.t, u.b from unnest(?::text[], ?::bytea[]) u(t, b)) k cross join lateral (select id, due_at from "
                + parked + " where shard = ? and topic = k.t and key = k.b order by id limit ?) q where q.due_at >"
                + " (select ahead from t))";
        lowestOfKeys = "select k.t, k.b, l.id from (select distinct t, b from unnest(?::text[], ?::bytea[])"
                + " u(t, b)) k cross join lateral (select id from " + parked + " where shard = ? and topic = k.t"
                + " and key = k.b order by id limit 1) l";
        countOfKeys = "select k.t, k.b, count(*) from (select distinct t, b from unnest(?::text[], ?::bytea[]) u(t, b)) k"
                + " join " + parked + " p on p.shard = ? and p.topic = k.t and p.key = k.b group by k.t, k.b";
        minDue = "select min(due_at) from " + parked + " where shard = ?";
        size = "select count(*) from " + parked;
    }

    /**
     * Writes the actions of one handler call, routed by origin ({@code parkedIds}: rows handed from this table).
     * {@link Park}: a main-stream record is inserted, a parked one gets its new {@code requeues} and due time.
     * {@link Hold}: a main-stream record is inserted as it is.
     * {@link Keep}: the due time moves later, never earlier. {@link Unpark}: {@link #unpark}. A parked {@link Head}:
     * {@link #toRetry}, a waiting one waiting since {@code now}; a parked {@link Dead}: {@link #toDead}. A parked
     * follower: nothing, as a parked head before it in the call blocks its key, and its {@link #toRetry} carries it.
     * Other actions of main-stream rows belong to {@link RetryStore#apply}.
     *
     * <p>Precondition: {@code actions} are in the call's order, where every parked row precedes the newer main-stream
     * rows of its key (the relay builds its calls that way). A parked head turns the later rows of its key into
     * followers, so no main-stream {@link Park} of its key comes after it: moving the head at once, before the batched
     * inserts, leaves behind no row it should have carried.
     */
    public void apply(Connection inTx, int shard, List<Action> actions, Set<Long> parkedIds, Instant now)
            throws SQLException {
        var inserts = new ArrayList<Park>();
        var holds = new ArrayList<Hold>();
        var reparks = new ArrayList<Park>();
        var aKeysTurn = comesRound(inTx, shard, actions, parkedIds, now);
        var keeps = new ArrayList<Keep>();
        var unparked = new ArrayList<OutboxRecord>();
        for (var action : actions) {
            var r = action.r();
            if (!parkedIds.contains(r.id())) {
                if (action instanceof Park p) {
                    inserts.add(p);
                } else if (action instanceof Hold h) {
                    holds.add(h);
                }
            } else if (action instanceof Park p) {
                reparks.add(p);
            } else if (action instanceof Keep k) {
                keeps.add(k);
            } else if (action instanceof Unpark) {
                unparked.add(r);
            } else if (action instanceof Head h) {
                toRetry(inTx, shard, r, h.attempts(), h.nextAttemptAt(), h.error(), h.waiting() == null ? null : now);
            } else if (action instanceof Dead d) {
                toDead(inTx, r, d.attempts(), d.error());
            }
        }
        if (!inserts.isEmpty() || !holds.isEmpty()) {
            try (var ps = inTx.prepareStatement(insert)) {
                for (var p : inserts) {
                    bindInsert(ps, shard, p.r(), p.requeues(), later(p, aKeysTurn));
                }
                for (var h : holds) {
                    bindInsert(ps, shard, h.r(), h.r().requeues(), h.dueAt());
                }
                ps.executeBatch();
            }
        }
        if (!reparks.isEmpty()) {
            try (var ps = inTx.prepareStatement(repark)) {
                for (var p : reparks) {
                    ps.setInt(1, p.requeues());
                    ps.setObject(2, timestamp(later(p, aKeysTurn)));
                    ps.setLong(3, p.r().id());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        if (!keeps.isEmpty()) {
            try (var ps = inTx.prepareStatement(keep)) {
                for (var k : keeps) {
                    ps.setObject(1, timestamp(k.dueAt()));
                    ps.setLong(2, k.r().id());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        unpark(inTx, unparked);
    }

    /**
     * When the {@link Park}s of each key in {@code actions} next come round, where that is past the delay: a key with
     * more parked rows than a batch is offered a batch per delay, so a row that requeues comes back after
     * {@code delay × ceil(rows / batch)}, which keeps its writes to about a batch per delay however long the key.
     */
    private Map<Object, Instant> comesRound(Connection c, int shard, List<Action> actions, Set<Long> parkedIds, Instant now)
            throws SQLException {
        var parks = new ArrayList<OutboxRecord>();
        var added = new HashMap<Object, Integer>();
        for (var action : actions) {
            if (action instanceof Park p && p.r().key() != null) {
                parks.add(p.r());
                if (!parkedIds.contains(p.r().id())) {
                    added.merge(BatchClassifier.orderingKey(p.r()), 1, Integer::sum);
                }
            }
        }
        if (parks.isEmpty() || batch == Integer.MAX_VALUE) {
            return Map.of();
        }
        var turns = new HashMap<Object, Instant>();
        try (var ps = c.prepareStatement(countOfKeys)) {
            ps.setArray(1, c.createArrayOf("text", parks.stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(2, c.createArrayOf("bytea", parks.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(3, (short) shard);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var key = List.of(rs.getString(1), ByteBuffer.wrap(rs.getBytes(2)));
                    long rows = rs.getLong(3) + added.getOrDefault(key, 0);
                    if (rows > batch) {
                        turns.put(key, now.plus(delay.multipliedBy((rows + batch - 1) / batch)));
                    }
                }
            }
        }
        added.forEach((key, rows) -> {
            if (rows > batch) { // none parked before them
                turns.putIfAbsent(key, now.plus(delay.multipliedBy((rows + batch - 1) / batch)));
            }
        });
        return turns;
    }

    private static void bindInsert(PreparedStatement ps, int shard, OutboxRecord r, int requeues, Instant due)
            throws SQLException {
        RetryStore.bindRow(ps, shard, r);
        ps.setInt(9, r.attempts());
        ps.setInt(10, requeues);
        ps.setObject(11, timestamp(due));
        ps.addBatch();
    }

    /** {@code p}'s due time, or its key's turn when that is later. */
    private static Instant later(Park p, Map<Object, Instant> aKeysTurn) {
        var turn = aKeysTurn.get(BatchClassifier.orderingKey(p.r()));
        return turn != null && turn.isAfter(p.dueAt()) ? turn : p.dueAt();
    }

    /** Deletes delivered rows; a row that is not in the table is a bug, so it fails and the step rolls back. */
    public void unpark(Connection c, List<OutboxRecord> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        try (var ps = c.prepareStatement(delete)) {
            ps.setArray(1, RetryStore.ids(c, rows));
            int deleted = ps.executeUpdate();
            if (deleted != rows.size()) {
                throw new IllegalStateException("parked rows " + rows.stream().map(OutboxRecord::id).toList()
                        + " not all found (" + deleted + " deleted)");
            }
        }
    }

    /**
     * Makes {@code row} the retry head of its (topic, key), due at {@code nextAttemptAt}, keeping its
     * {@code requeues} and waiting since {@code waitingSince} (null: not waiting): the key's retry heads with a higher
     * id become followers, which do not wait, and its parked rows with a higher id follow it, keeping their
     * {@code attempts} and {@code requeues}, so nothing newer of the key overtakes it. An unkeyed row moves alone.
     */
    public void toRetry(Connection c, int shard, OutboxRecord row, int attempts, Instant nextAttemptAt, String error,
            @Nullable Instant waitingSince) throws SQLException {
        try (var ps = c.prepareStatement(toRetry)) {
            ps.setLong(1, row.id());
            bindNewerOfKey(ps, 2, shard, row); // parked rows
            bindNewerOfKey(ps, 6, shard, row); // retry heads
            ps.setInt(10, attempts);
            ps.setObject(11, timestamp(nextAttemptAt));
            ps.setString(12, errorText(error));
            ps.setObject(13, waitingSince == null ? null : timestamp(waitingSince));
            ps.setLong(14, row.id());
            ps.setLong(15, row.id());
            try (var rs = ps.executeQuery()) {
                rs.next();
                requireOneRow(rs.getInt(1), "parked", row);
            }
        }
    }

    /** Moves a parked row to the dead table, as {@link RetryStore#toDead} writes every dead row. */
    public void toDead(Connection c, OutboxRecord row, int attempts, String error) throws SQLException {
        try (var ps = c.prepareStatement(toDead)) {
            ps.setLong(1, row.id());
            ps.setInt(2, attempts);
            ps.setString(3, errorText(error));
            requireOneRow(ps.executeUpdate(), "parked", row);
        }
    }

    /** Parks a retry row, keeping its {@code attempts}; the caller promotes the next row of its key. */
    public void fromRetry(Connection c, OutboxRecord row, int requeues, Instant dueAt) throws SQLException {
        try (var ps = c.prepareStatement(fromRetry)) {
            ps.setLong(1, row.id());
            ps.setInt(2, requeues);
            ps.setObject(3, timestamp(dueAt));
            requireOneRow(ps.executeUpdate(), "retry", row);
        }
    }

    /** Rows of {@code shard} due at {@code now}, in id order. */
    public List<OutboxRecord> due(Connection c, int shard, Instant now, int limit) throws SQLException {
        try (var ps = c.prepareStatement(selectDue)) {
            ps.setShort(1, (short) shard);
            ps.setObject(2, timestamp(now));
            ps.setInt(3, limit);
            return records(ps.executeQuery());
        }
    }

    /**
     * What a due pass hands, at most {@code most} rows: the oldest {@code limit} rows due at {@code now} by due time,
     * key by key in the order they came due. A key that fits in a batch comes whole, its older rows due or not and its
     * other due rows, so its rows come back in their order whatever their due times. A key larger than that comes only
     * with the rows due: it would otherwise bring its oldest rows on every step of a sweep, spending their requeues
     * without a delay, and a row of it may go ahead of older ones, each of which was offered and answered requeue. A
     * key that does not fit in what is left of the call stays due for the next step; in a call with room for a batch
     * the first always fits, so a step moves rows. An unkeyed row comes alone.
     */
    public List<OutboxRecord> dueWithOlder(Connection c, int shard, Instant now, int limit, long most)
            throws SQLException {
        var due = due(c, shard, now, limit);
        var ofKey = new LinkedHashMap<Object, List<OutboxRecord>>();
        for (var row : due) {
            ofKey.computeIfAbsent(BatchClassifier.orderingKey(row), k -> new ArrayList<>()).add(row);
        }
        var rowsOfKey = rowsOfKeys(c, shard, due);
        var handed = new TreeMap<Long, OutboxRecord>();
        var whole = new ArrayList<OutboxRecord>(); // the highest row due of each key that fits
        long room = most;
        for (var e : ofKey.entrySet()) {
            var rows = e.getValue();
            long ofIt = rowsOfKey.getOrDefault(e.getKey(), 0L);
            boolean fits = fits(ofIt);
            long size = fits ? ofIt : rows.size();
            if (size > room) {
                break;
            }
            room -= size;
            if (fits) {
                whole.add(rows.stream().max(Comparator.comparingLong(OutboxRecord::id)).orElseThrow());
            } else {
                rows.forEach(row -> handed.put(row.id(), row));
            }
        }
        if (!whole.isEmpty()) {
            try (var ps = c.prepareStatement(wholeKeys)) {
                bindKeys(c, ps, 1, whole);
                ps.setShort(4, (short) shard);
                ps.setObject(5, timestamp(now));
                records(ps.executeQuery()).forEach(row -> handed.put(row.id(), row));
            }
        }
        return List.copyOf(handed.values());
    }

    /**
     * How many parked rows each key of {@code keyed} holds, those with none absent, up to a batch and one more: above a
     * batch, the key does not fit in a call, and how far above does not matter.
     */
    public Map<Object, Long> rowsOfKeys(Connection c, int shard, Collection<OutboxRecord> keyed) throws SQLException {
        var withKey = keyed.stream().filter(row -> row.key() != null).toList();
        var rows = new HashMap<Object, Long>();
        if (withKey.isEmpty()) {
            return rows;
        }
        try (var ps = c.prepareStatement(fitting)) {
            ps.setArray(1, c.createArrayOf("text", withKey.stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(2, c.createArrayOf("bytea", withKey.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(3, (short) shard);
            ps.setInt(4, batch == Integer.MAX_VALUE ? batch : batch + 1);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (rs.getLong(3) > 0) {
                        rows.put(List.of(rs.getString(1), ByteBuffer.wrap(rs.getBytes(2))), rs.getLong(3));
                    }
                }
            }
        }
        return rows;
    }

    /** Whether a key with these {@link #rowsOfKeys} fits in a call. */
    public boolean fits(long rows) {
        return rows > 0 && rows <= batch;
    }

    /**
     * For each key of {@code answered} none of whose rows stays ({@code writes}: its {@link Park} or {@link Keep}), its
     * lowest {@code batch} rows are due ahead of every other row of the shard, so its rows go on at once and in their
     * order, whenever they would have come round, and at most what was read when its block ended has passed them; a
     * row's own {@code notBefore} is cut short by that. The key takes the next steps until its rows stop leaving. When
     * the rows that left were not the key's lowest, the lowest may be what just answered {@code requeue}, and offering
     * them again would spend their requeues at full speed: such a pull happens at most once per {@code delay} for a key,
     * so at most one portion passes them once their block ends. Returns whether it wrote a row.
     *
     * <p>What it costs its neighbours: no key has a minimum share, so while a key of N rows goes on, the due parked rows
     * of the shard's other keys wait behind it, about {@code N / batch} steps. Retry heads and the main stream do not
     * wait: a step takes the heads first and reads its chunk whatever the due pass took.
     */
    public boolean pullNext(Connection c, int shard, List<Action> writes, List<OutboxRecord> answered, Instant now)
            throws SQLException {
        var gone = new LinkedHashMap<Object, OutboxRecord>(); // the lowest of each key
        for (var row : answered) {
            if (row.key() != null) {
                gone.merge(BatchClassifier.orderingKey(row), row, (a, b) -> a.id() < b.id() ? a : b);
            }
        }
        for (var write : writes) {
            if (write instanceof Park || write instanceof Keep) {
                gone.remove(BatchClassifier.orderingKey(write.r()));
            }
        }
        if (gone.isEmpty()) {
            return false;
        }
        var eligible = new ArrayList<OutboxRecord>();
        try (var ps = c.prepareStatement(lowestOfKeys)) {
            ps.setArray(1, c.createArrayOf("text", gone.values().stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(2, c.createArrayOf("bytea", gone.values().stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(3, (short) shard);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var key = List.of(rs.getString(1), ByteBuffer.wrap(rs.getBytes(2)));
                    var left = gone.get(key);
                    if (rs.getLong(3) > left.id() || offTheBottom(key, now)) {
                        eligible.add(left);
                    }
                }
            }
        }
        if (eligible.isEmpty()) {
            return false;
        }
        try (var ps = c.prepareStatement(pullNext)) {
            ps.setObject(1, timestamp(now));
            ps.setShort(2, (short) shard);
            ps.setArray(3, c.createArrayOf("text", eligible.stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(4, c.createArrayOf("bytea", eligible.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(5, (short) shard);
            ps.setInt(6, batch);
            return ps.executeUpdate() > 0;
        }
    }

    /** Whether a pull of {@code key}'s lowest rows when rows above them left is due: none in the last {@code delay}. */
    private boolean offTheBottom(Object key, Instant now) {
        synchronized (pulledOffTheBottom) {
            var last = pulledOffTheBottom.get(key);
            if (last != null && !last.isAfter(now) && last.plus(delay).isAfter(now)) { // a clock stepped back: gone
                return false;
            }
            pulledOffTheBottom.put(key, now);
            return true;
        }
    }

    /** Parameters {@code first..first+2} of a statement per (topic, key) and an id: each of {@code rows}'. */
    private static void bindKeys(Connection c, PreparedStatement ps, int first, Collection<OutboxRecord> rows)
            throws SQLException {
        ps.setArray(first, c.createArrayOf("text", rows.stream().map(OutboxRecord::topic).toArray()));
        ps.setArray(first + 1, c.createArrayOf("bytea", rows.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
        ps.setArray(first + 2, c.createArrayOf("int8", rows.stream().map(OutboxRecord::id).toArray()));
    }

    /**
     * The parked rows of the (topic, key) pairs of {@code keyed}: the oldest {@code perKey} of each, in id order, so a
     * key with many rows takes none of another's. An unkeyed record matches nothing.
     */
    public List<OutboxRecord> ofEachKey(Connection c, int shard, Collection<OutboxRecord> keyed, int perKey)
            throws SQLException {
        if (keyed.isEmpty()) {
            return List.of();
        }
        try (var ps = c.prepareStatement(selectOfKeys)) {
            ps.setArray(1, c.createArrayOf("text", keyed.stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(2, c.createArrayOf("bytea", keyed.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(3, (short) shard);
            ps.setInt(4, perKey);
            ps.setInt(5, (int) Math.min((long) perKey * keyed.size(), Integer.MAX_VALUE));
            return records(ps.executeQuery());
        }
    }

    /** Earliest {@code due_at} of the parked rows of {@code shard}; null without any. */
    public @Nullable Instant minDue(Connection c, int shard) throws SQLException {
        return RetryStore.minTimestamp(c, minDue, shard);
    }

    public long size(Connection c) throws SQLException {
        return RetryStore.count(c, size);
    }

    /** Parameters {@code first..first+3} of a "newer rows of the key" condition: shard, topic, key, id. */
    private static void bindNewerOfKey(PreparedStatement ps, int first, int shard, OutboxRecord row)
            throws SQLException {
        ps.setShort(first, (short) shard);
        ps.setString(first + 1, row.topic());
        ps.setBytes(first + 2, row.key());
        ps.setLong(first + 3, row.id());
    }

    private static List<OutboxRecord> records(ResultSet resultSet) throws SQLException {
        try (var rs = resultSet) {
            var records = new ArrayList<OutboxRecord>();
            while (rs.next()) {
                records.add(RetryStore.record(rs, 1));
            }
            return records;
        }
    }
}
