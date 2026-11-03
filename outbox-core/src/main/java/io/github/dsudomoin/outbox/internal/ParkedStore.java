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
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Requeued records in {@code <base>_parked}: unlike retry rows they block nothing; each comes back right before the
 * next record of its (topic, key) or at its {@code due_at}, or that of a newer parked row of its key. Every method runs
 * on the caller's connection, so inside the shard step's transaction a rolled-back step leaves each record where it
 * was.
 */
public final class ParkedStore {

    /** A due time on a whole second marks a row's turn, written only by {@link #settle}: see {@link #dueWithOlder}. */
    private static final String ON_A_TURN = "extract(microseconds from due_at)::bigint % 1000000 = 0";
    /** The shortest gap between a key's turns: due times on whole seconds cannot be closer. */
    private static final Duration SHORTEST_TURN = Duration.ofSeconds(1);

    private final String insert;
    private final String repark;
    private final String keep;
    private final String delete;
    private final String toRetry;
    private final String toDead;
    private final String fromRetry;
    private final String selectDue;
    private final String selectDueTurns;
    private final String selectOfKeys;
    private final String selectOlder;
    private final String putOff;
    private final String postponeNewer;
    private final String pullNext;
    private final String countOfKeys;
    private final String minDue;
    private final String size;

    public ParkedStore(TableNames names, RetryStore retries) {
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
        selectDue = "select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and due_at <= ? order by id limit ?";
        selectDueTurns = "select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and due_at <= ? and " + ON_A_TURN + " order by id limit ?";
        // at most limit rows read per key: a key with thousands of parked rows costs no more than any other
        selectOfKeys = "select " + RECORD_COLUMNS + " from (select distinct t, b from unnest(?::text[], ?::bytea[])"
                + " u(t, b)) k cross join lateral (select " + RECORD_COLUMNS + " from " + parked
                + " where shard = ? and topic = k.t and key = k.b order by id limit ?) p order by id limit ?";
        // of a key waiting in retry, its lowest row not due; of any other, its rows not due below the due one (those
        // due came with it: the due rows are the shard's lowest), the oldest first, after those, so that those never
        // cut the others to fewer than the limit. The head by the key's lowest id, one index entry and one row, however
        // many rows it holds: a follower below its head, from a long transaction, reads as no due time, and the head
        // then brings the rows put off anyway
        selectOlder = "select " + RECORD_COLUMNS + ", due_at, coalesce(in_retry, false) in_retry, head_due"
                + " from unnest(?::text[], ?::bytea[], ?::int8[]) u(t, b, below)"
                + " left join lateral (select true in_retry, r.next_attempt_at head_due from " + retry
                + " r where r.shard = ? and r.topic = u.t and r.key = u.b order by r.id limit 1) k on true"
                + " cross join lateral (select " + RECORD_COLUMNS + ", due_at from " + parked
                + " where shard = ? and topic = u.t and key = u.b and id < u.below and due_at > ?"
                + " order by id limit case when k.in_retry then 1 else ? end) p"
                + " order by coalesce(in_retry, false) desc, id limit ?";
        putOff = "update " + parked + " set due_at = ? where id = any(?)";
        // each key's due rows above the handed ones get turns, a batch per delay
        postponeNewer = "with t as (select p.id, (row_number() over (partition by k.t, k.b order by p.id) - 1) / ? turn"
                + " from unnest(?::text[], ?::bytea[], ?::int8[]) k(t, b, above) join " + parked + " p on p.shard = ?"
                + " and p.topic = k.t and p.key = k.b and p.id > k.above and p.due_at <= ?)"
                + " update " + parked + " p set due_at = date_trunc('second', ?::timestamptz + (t.turn + 1)"
                + " * make_interval(secs => ?)) + interval '1 second' from t where p.id = t.id";
        pullNext = "update " + parked + " p set due_at = ? where p.id in (select q.id from unnest(?::text[], ?::bytea[],"
                + " ?::int8[]) k(t, b, above) cross join lateral (select id from " + parked + " where shard = ?"
                + " and topic = k.t and key = k.b and id > k.above and due_at > ? and " + ON_A_TURN
                + " order by id limit ?) q)";
        countOfKeys = "select k.t, k.b, count(*) from unnest(?::text[], ?::bytea[], ?::int8[]) k(t, b, above) join "
                + parked + " p on p.shard = ? and p.topic = k.t and p.key = k.b group by k.t, k.b";
        minDue = "select min(due_at) from " + parked + " where shard = ?";
        size = "select count(*) from " + parked;
    }

    /**
     * Writes the actions of one handler call, routed by origin ({@code parkedIds}: rows handed from this table).
     * {@link Park}: a main-stream record is inserted, a parked one gets its new {@code requeues} and due time.
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
        var reparks = new ArrayList<Park>();
        var keeps = new ArrayList<Keep>();
        var unparked = new ArrayList<OutboxRecord>();
        for (var action : actions) {
            var r = action.r();
            if (!parkedIds.contains(r.id())) {
                if (action instanceof Park p) {
                    inserts.add(p);
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
        if (!inserts.isEmpty()) {
            try (var ps = inTx.prepareStatement(insert)) {
                for (var p : inserts) {
                    RetryStore.bindRow(ps, shard, p.r());
                    ps.setInt(9, p.r().attempts());
                    ps.setInt(10, p.requeues());
                    ps.setObject(11, timestamp(p.dueAt()));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        if (!reparks.isEmpty()) {
            try (var ps = inTx.prepareStatement(repark)) {
                for (var p : reparks) {
                    ps.setInt(1, p.requeues());
                    ps.setObject(2, timestamp(p.dueAt()));
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

    /** What a due pass hands, and whether it moved the due time of rows it did not hand: {@link #minDue} changed. */
    public record DueRows(List<OutboxRecord> rows, boolean putOff, Set<Long> turns) {

        public static final DueRows NONE = new DueRows(List.of(), false, Set.of());
    }

    /**
     * What a due pass hands: the rows of {@code shard} due at {@code now} and, for each keyed one, the older parked
     * rows of its (topic, key), due or not, so a key's rows come back in their order whatever their due times; the
     * oldest {@code limit} of all these, in id order, which keeps each key's a prefix of its rows; and the rows on their
     * turn ({@link #settle}), {@code turns}, wherever they lie: such a row goes behind the key's oldest rows in the call,
     * ahead of the rows between, each of which had its turn and answered {@code requeue}. A key waiting in the
     * retry table is blocked, so its rows are not offered ahead of time: its due rows come only up to its lowest row
     * not due, and those above that stay parked, no requeue spent, due with it or when the key's retry head comes up,
     * whichever is sooner, as the key may leave the retry table then, and at least {@code requeueDelay} from now, so
     * the shard is not stepped for them in the meantime: at most one more step per retry attempt. An unkeyed row comes
     * alone.
     */
    /** The statement {@link #dueWithOlder} reads the older rows with, for a test of its plan. */
    String selectOlder() {
        return selectOlder;
    }

    public DueRows dueWithOlder(Connection c, int shard, Instant now, int limit, Duration requeueDelay)
            throws SQLException {
        var due = new ArrayList<>(due(c, shard, now, limit));
        var turns = new HashSet<Long>();
        if (!due.isEmpty()) { // also those on their turn beyond the limit: none of the oldest may keep them out
            var seen = new HashSet<Long>();
            due.forEach(row -> seen.add(row.id()));
            try (var ps = c.prepareStatement(selectDueTurns)) {
                ps.setShort(1, (short) shard);
                ps.setObject(2, timestamp(now));
                ps.setInt(3, limit);
                for (var row : records(ps.executeQuery())) {
                    turns.add(row.id());
                    if (seen.add(row.id())) {
                        due.add(row);
                    }
                }
            }
        }
        var newest = new LinkedHashMap<Object, OutboxRecord>();
        for (var row : due) { // in id order: the last of a key stays
            if (row.key() != null) {
                newest.put(BatchClassifier.orderingKey(row), row);
            }
        }
        if (newest.isEmpty()) {
            return new DueRows(due, false, turns);
        }
        var rows = new TreeMap<Long, OutboxRecord>();
        due.forEach(row -> rows.put(row.id(), row));
        var notDue = new HashMap<Object, OutboxRecord>(); // of a key waiting in retry
        var lookAgainAt = new HashMap<Object, OffsetDateTime>();
        try (var ps = c.prepareStatement(selectOlder)) {
            bindKeys(c, ps, 1, newest.values());
            ps.setShort(4, (short) shard);
            ps.setShort(5, (short) shard);
            ps.setObject(6, timestamp(now));
            ps.setInt(7, limit);
            ps.setInt(8, limit + newest.size());
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    var row = RetryStore.record(rs, 1);
                    if (rs.getBoolean("in_retry")) {
                        notDue.put(BatchClassifier.orderingKey(row), row);
                        var at = rs.getObject("due_at", OffsetDateTime.class);
                        var head = rs.getObject("head_due", OffsetDateTime.class);
                        lookAgainAt.put(BatchClassifier.orderingKey(row),
                                head != null && head.isBefore(at) ? head : at);
                    } else {
                        rows.put(row.id(), row);
                    }
                }
            }
        }
        if (notDue.isEmpty()) {
            return new DueRows(withTurns(rows, limit, due, turns), false, turns);
        }
        var above = new HashMap<Object, List<Long>>();
        for (var row : due) {
            var key = BatchClassifier.orderingKey(row);
            var older = notDue.get(key);
            if (older != null && row.id() > older.id() && !turns.contains(row.id())) {
                rows.remove(row.id());
                above.computeIfAbsent(key, k -> new ArrayList<>()).add(row.id());
            }
        }
        var earliest = timestamp(now.plus(requeueDelay));
        try (var ps = c.prepareStatement(putOff)) {
            for (var key : notDue.keySet()) {
                var at = lookAgainAt.get(key);
                ps.setObject(1, at.isAfter(earliest) ? at : earliest);
                ps.setArray(2, c.createArrayOf("int8", above.getOrDefault(key, List.of()).toArray()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return new DueRows(withTurns(rows, limit, due, turns), true, turns);
    }

    /** The oldest {@code limit} of {@code rows} and those on their turn. */
    private static List<OutboxRecord> withTurns(TreeMap<Long, OutboxRecord> rows, int limit, List<OutboxRecord> due,
            Set<Long> turns) {
        var handed = new TreeMap<Long, OutboxRecord>();
        rows.values().stream().limit(limit).forEach(row -> handed.put(row.id(), row));
        due.stream().filter(row -> turns.contains(row.id())).forEach(row -> handed.put(row.id(), row));
        return List.copyOf(handed.values());
    }

    /**
     * Writes the parked rows of one call of the due pass ({@code writes}, the rows handed being {@code handed}) and gives
     * the rows behind a key's oldest their turns, so every parked row is offered again in a bounded time however long
     * the oldest keep requeueing, as a later row may carry what they wait for, at a cost per turn that does not grow with
     * the key's rows ({@link #turns}). A key none of whose rows but those on their turn stays gets the next {@code batch}
     * rows waiting for their turn above them due now, one statement per step, so its rows go on at once without a
     * rewrite of the rest. Returns whether it wrote a row.
     */
    public boolean settle(Connection c, int shard, List<Action> writes, List<OutboxRecord> handed, Set<Long> onTurn,
            Instant now, Duration delay, int batch, boolean cut) throws SQLException {
        var ofKey = new LinkedHashMap<Object, List<OutboxRecord>>();
        for (var row : handed) {
            if (row.key() != null) {
                ofKey.computeIfAbsent(BatchClassifier.orderingKey(row), k -> new ArrayList<>()).add(row);
            }
        }
        var turned = turns(c, shard, writes, ofKey, onTurn, now, delay, batch, cut);
        var ids = new HashSet<Long>();
        turned.forEach(write -> ids.add(write.r().id()));
        apply(c, shard, turned, ids, now);
        return !turned.isEmpty() | pullNext(c, shard, writes, ofKey, onTurn, now, batch);
    }

    /** {@link #settle} of a main-pass call, whose rows are written already: only the rows of a key whose oldest left. */
    public boolean pullNext(Connection c, int shard, List<Action> writes, List<OutboxRecord> handed, Instant now,
            int batch) throws SQLException {
        var ofKey = new LinkedHashMap<Object, List<OutboxRecord>>();
        for (var row : handed) {
            if (row.key() != null) {
                ofKey.computeIfAbsent(BatchClassifier.orderingKey(row), k -> new ArrayList<>()).add(row);
            }
        }
        return pullNext(c, shard, writes, ofKey, Set.of(), now, batch);
    }

    /** When a turn comes, {@code turns} turns from {@code now}: on a whole second, which is what marks it. */
    private static Instant turn(Instant now, Duration delay, long turns) {
        var gap = delay.compareTo(SHORTEST_TURN) < 0 ? SHORTEST_TURN : delay;
        return now.plus(gap.multipliedBy(turns)).truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
    }

    private static Set<Long> staying(List<Action> writes) {
        var stay = new HashSet<Long>();
        for (var write : writes) {
            if (write instanceof Park || write instanceof Keep) {
                stay.add(write.r().id());
            }
        }
        return stay;
    }

    /**
     * Where a key has rows outside a call and some of the call stays, in a call of half a {@code batch} at least or with
     * a row on its turn: its rows that are not on their turn are due in 2 turns, so they come along with the next turn,
     * one call per turn; the rows on their turn that requeued go to the back, after as many turns as the key's other
     * rows make batches; and with {@code cut} its due rows above the call get turns, a batch per turn (at least a second
     * apart), written once. So every row's turn comes every (rows / batch) turns while the writes of a turn stay at about
     * 2 batches, however many rows the key holds. A row with a {@code notBefore} of its own keeps it.
     */
    private List<Action> turns(Connection c, int shard, List<Action> writes, Map<Object, List<OutboxRecord>> ofKey,
            Set<Long> onTurn, Instant now, Duration delay, int batch, boolean cut) throws SQLException {
        var stay = staying(writes);
        var full = new LinkedHashMap<Object, OutboxRecord>();
        for (var e : ofKey.entrySet()) {
            var rows = e.getValue();
            if ((rows.size() >= Math.max(1, batch / 2) || rows.stream().anyMatch(row -> onTurn.contains(row.id())))
                    && rows.stream().anyMatch(row -> stay.contains(row.id()))) {
                full.put(e.getKey(), rows.get(rows.size() - 1));
            }
        }
        if (full.isEmpty()) {
            return writes;
        }
        var others = new HashMap<Object, Long>();
        try (var ps = c.prepareStatement(countOfKeys)) {
            bindKeys(c, ps, 1, full.values());
            ps.setShort(4, (short) shard);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    others.put(List.of(rs.getString(1), ByteBuffer.wrap(rs.getBytes(2))), rs.getLong(3));
                }
            }
        }
        var normal = now.plus(delay);
        var soon = now.plus((delay.compareTo(SHORTEST_TURN) < 0 ? SHORTEST_TURN : delay).multipliedBy(2));
        var turned = new ArrayList<Action>(writes.size());
        for (var write : writes) {
            var key = BatchClassifier.orderingKey(write.r());
            var rows = ofKey.get(key);
            long left = rows == null ? 0 : others.getOrDefault(key, 0L) - rows.size();
            if (left > 0 && write instanceof Park p && p.dueAt().equals(normal)) {
                var due = onTurn.contains(p.r().id()) ? turn(now, delay, 1 + (left + batch - 1) / batch) : soon;
                turned.add(new Park(p.r(), p.requeues(), due));
            } else {
                turned.add(write);
            }
        }
        if (cut) {
            try (var ps = c.prepareStatement(postponeNewer)) {
                ps.setInt(1, batch);
                bindKeys(c, ps, 2, full.values());
                ps.setShort(5, (short) shard);
                ps.setObject(6, timestamp(now));
                ps.setObject(7, timestamp(now));
                ps.setDouble(8, (delay.compareTo(SHORTEST_TURN) < 0 ? SHORTEST_TURN : delay).toNanos() / 1e9);
                ps.executeUpdate();
            }
        }
        return turned;
    }

    /** Rows waiting for their turn, of the keys none of whose handed rows but those on their turn stays, come at once. */
    private boolean pullNext(Connection c, int shard, List<Action> writes, Map<Object, List<OutboxRecord>> ofKey,
            Set<Long> onTurn, Instant now, int batch) throws SQLException {
        var stay = staying(writes);
        var gone = new ArrayList<OutboxRecord>();
        for (var rows : ofKey.values()) {
            if (rows.stream().noneMatch(row -> !onTurn.contains(row.id()) && stay.contains(row.id()))) {
                gone.add(rows.stream().filter(row -> !onTurn.contains(row.id())).reduce((a, b) -> b)
                        .orElse(rows.get(0)));
            }
        }
        if (gone.isEmpty()) {
            return false;
        }
        try (var ps = c.prepareStatement(pullNext)) {
            ps.setObject(1, timestamp(now));
            bindKeys(c, ps, 2, gone);
            ps.setShort(5, (short) shard);
            ps.setObject(6, timestamp(now));
            ps.setInt(7, batch);
            return ps.executeUpdate() > 0;
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
     * The parked rows of the (topic, key) pairs of {@code keyed}: the oldest {@code limit} of them, in id order. An
     * unkeyed record matches nothing.
     */
    public List<OutboxRecord> ofKeys(Connection c, int shard, Collection<OutboxRecord> keyed, int limit)
            throws SQLException {
        if (keyed.isEmpty()) {
            return List.of();
        }
        try (var ps = c.prepareStatement(selectOfKeys)) {
            ps.setArray(1, c.createArrayOf("text", keyed.stream().map(OutboxRecord::topic).toArray()));
            ps.setArray(2, c.createArrayOf("bytea", keyed.stream().map(OutboxRecord::key).toArray(byte[][]::new)));
            ps.setShort(3, (short) shard);
            ps.setInt(4, limit);
            ps.setInt(5, limit);
            return records(ps.executeQuery());
        }
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
