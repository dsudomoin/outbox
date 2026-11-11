package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxRecord;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The batch query: rows of one shard whose xid is visible in the target tick's snapshot and not in the
 * previous tick's, i.e. exactly the transactions that committed between the two ticks, in id order after
 * {@code lastId}. Candidates are "started at or after prev's xmax" ∪ "in flight at prev and no longer at target", so
 * the scan stays proportional to the batch even while a long transaction holds xmin back or keeps writing. An empty
 * candidate xip binds an empty array, which the planner prunes away with its branch.
 *
 * <p>Each statement comes twice: plain, bounded by rows only, and budgeted, bounded by bytes too ({@link RowBytes}):
 * there a running sum over the rows the plain statement orders and limits stops the read at the row that crosses the
 * budget, which comes back only as a marker without its payload, key and headers, so the caller knows it was cut
 * there; the rows after it are never fetched or sent. The window costs about a third of the plain statement's time
 * with small rows, so the caller picks the budgeted one only when the rows may be large ({@link ShardProcessor}).
 */
public final class BatchReader {

    /**
     * A main-stream row ({@code attempts} = 0); {@code blocked} when its (topic, key) has rows in the retry table;
     * {@code bytes} as {@link RowBytes} counts them, from the arrays read.
     */
    public record BatchRow(OutboxRecord record, boolean blocked, int bytes) {}

    /** One shard's part of {@link #readMany}: the arguments of {@link #read}. */
    public record ShardRead(int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            boolean checkBlocked) {}

    /**
     * The rows of {@link #readMany}, per shard in id order, with an entry (maybe empty) for every shard read, in the
     * order of the reads. {@code cut}: the total limit was reached, so a shard's rows may be only a prefix of what
     * {@link #read} returns for it, and the shards after it may have none they would have had. {@code bytesCut} (a
     * budgeted read only): the byte budget stopped the read before a row it would have returned, always a cut so: the
     * shard of that row has a prefix of its rows (maybe none), the shards after it none. {@code bytes}: what the rows
     * hold, as {@link RowBytes} counts.
     */
    public record Batch(Map<Integer, List<BatchRow>> rows, boolean cut, boolean bytesCut, long bytes) {}

    /** A read's byte budget: {@code bytes}, the first row however large when {@code oneAtLeast}. */
    private record Budget(long bytes, boolean oneAtLeast) {}

    private static final int MAX_FETCH = 5000;
    private static final String BLOCKED = "exists (select 1 from %s r"
            + " where r.shard = o.shard and r.topic = o.topic and r.key = o.key)";
    /** A row past the budget (the cut's marker) comes without them. */
    private static final String FITTING_COLUMNS = "case when o.fit then o.key end, case when o.fit then o.payload end,"
            + " case when o.fit then o.headers end";
    /**
     * The rows that fit and the first that does not, its marker: the second row is that or fits, and a later one
     * starting past the budget comes after the marker. The budget is the statement's last parameter.
     */
    private static final String BUDGET_CUT = " where o.n <= 2 or o.run - o.b <= ?";

    private final String select;
    private final String selectUnchecked;
    private final String selectMany;
    private final String selectManyUnchecked;
    private final String budgeted;
    private final String budgetedUnchecked;
    private final String budgetedMany;
    private final String budgetedManyUnchecked;

    public BatchReader(TableNames names) {
        var blocked = BLOCKED.formatted(names.retry());
        this.select = sql(names, blocked);
        this.selectUnchecked = sql(names, "false");
        // the per-shard parameters arrive as rows of unnest, so the partition bounds are known only as each shard's
        // lateral runs: pruned at run time on every rescan (the xip branch through its lo/hi bounds, as xid = any of a
        // non-constant array prunes nothing). Run-time pruning still opens and locks every partition and index when
        // the statement starts, past the fast-path locks with a few dozen partitions (2.7 times read's time per row on
        // the stand), so both branches also carry the group's own constant xid bounds, which prune at plan time.
        // Each array is cast once, here, not per row
        this.selectMany = manySql(names, "case when u.chk then " + blocked + " else false end");
        // no read checks: the statement does not name the retry table at all
        this.selectManyUnchecked = manySql(names, "false");
        this.budgeted = budgetedSql(names, blocked);
        this.budgetedUnchecked = budgetedSql(names, "false");
        this.budgetedMany = budgetedManySql(names, "case when o.fit and o.chk then " + blocked + " else false end");
        this.budgetedManyUnchecked = budgetedManySql(names, "false");
    }

    private static String manySql(TableNames names, String blocked) {
        return "select u.i, o.id, o.message_id, o.topic, o.key, o.payload, o.headers, o.created_at, o.shard, " + blocked
                + " from unnest(?::int[], ?::text[]::xid8[],"
                + " ?::text[]::xid8[], ?::text[]::xid8[], ?::text[]::xid8[], ?::text[], ?::text[]::pg_snapshot[],"
                + " ?::int8[], ?::int[], ?::bool[]) with ordinality u(shard, pmax, tmax, lo, hi, xip, snap, last_id, lim,"
                + " chk, i)"
                + " cross join lateral (select o.* from ("
                + " select * from " + names.base() + " where shard = u.shard and xid >= u.pmax and xid < u.tmax"
                + " and xid >= ?::xid8 and xid < ?::xid8"
                + " union all"
                + " select * from " + names.base() + " where shard = u.shard and xid >= u.lo and xid <= u.hi"
                + " and xid = any(u.xip::xid8[]) and xid >= ?::xid8 and xid < ?::xid8"
                + ") o where pg_visible_in_snapshot(o.xid, u.snap) and o.id > u.last_id order by o.id limit u.lim) o"
                + " limit ?";
    }

    private static String sql(TableNames names, String blocked) {
        return "select o.id, o.message_id, o.topic, o.key, o.payload, o.headers, o.created_at, o.shard, " + blocked
                + " as blocked from ("
                + " select * from " + names.base() + " where shard = ? and xid >= ?::xid8 and xid < ?::xid8"
                + " union all"
                + " select * from " + names.base() + " where shard = ? and xid = any(?::xid8[])"
                + ") o where pg_visible_in_snapshot(o.xid, ?::pg_snapshot) and o.id > ? order by o.id limit ?";
    }

    /** {@link #manySql(TableNames, String)} within a byte budget: the parameters are its, after two and before one. */
    private static String budgetedManySql(TableNames names, String blocked) {
        // the window has no order: it takes the rows as the limit passes them, shard by shard as the lateral yields
        // them (see readMany), which is the order the byte budget cuts in; an order by would sort them all first
        return "select o.i, o.id, o.message_id, o.topic, " + FITTING_COLUMNS + ", o.created_at, o.shard, " + blocked
                + ", o.fit from (select w.*, w.run <= ? or w.n = 1 and ? as fit from (select l.*,"
                + " sum(l.b) over r as run, row_number() over r as n from (select u.i, u.chk, o.id, o.message_id,"
                + " o.topic, o.key, o.payload, o.headers, o.created_at, o.shard, " + RowBytes.sql("o") + " as b"
                + " from unnest(?::int[], ?::text[]::xid8[],"
                + " ?::text[]::xid8[], ?::text[]::xid8[], ?::text[]::xid8[], ?::text[], ?::text[]::pg_snapshot[],"
                + " ?::int8[], ?::int[], ?::bool[]) with ordinality u(shard, pmax, tmax, lo, hi, xip, snap, last_id, lim,"
                + " chk, i)"
                + " cross join lateral (select o.* from ("
                + " select * from " + names.base() + " where shard = u.shard and xid >= u.pmax and xid < u.tmax"
                + " and xid >= ?::xid8 and xid < ?::xid8"
                + " union all"
                + " select * from " + names.base() + " where shard = u.shard and xid >= u.lo and xid <= u.hi"
                + " and xid = any(u.xip::xid8[]) and xid >= ?::xid8 and xid < ?::xid8"
                + ") o where pg_visible_in_snapshot(o.xid, u.snap) and o.id > u.last_id order by o.id limit u.lim) o"
                + " limit ?) l window r as (rows between unbounded preceding and current row)) w) o" + BUDGET_CUT;
    }

    /** {@link #sql(TableNames, String)} within a byte budget: the parameters are its, after two and before one. */
    private static String budgetedSql(TableNames names, String blocked) {
        return "select o.id, o.message_id, o.topic, " + FITTING_COLUMNS + ", o.created_at, o.shard,"
                + " case when o.fit then " + blocked + " else false end as blocked, o.fit"
                + " from (select w.*, w.run <= ? or w.n = 1 and ? as fit from (select l.*, sum(l.b) over r as run,"
                + " row_number() over r as n from (select o.id, o.message_id, o.topic, o.key, o.payload, o.headers,"
                + " o.created_at, o.shard, " + RowBytes.sql("o") + " as b from ("
                + " select * from " + names.base() + " where shard = ? and xid >= ?::xid8 and xid < ?::xid8"
                + " union all"
                + " select * from " + names.base() + " where shard = ? and xid = any(?::xid8[])"
                + ") o where pg_visible_in_snapshot(o.xid, ?::pg_snapshot) and o.id > ? order by o.id limit ?"
                + ") l window r as (order by l.id rows between unbounded preceding and current row)) w) o" + BUDGET_CUT
                + " order by o.id";
    }

    /** The statement {@link #read} runs. */
    String sql(boolean checkBlocked) {
        return checkBlocked ? select : selectUnchecked;
    }

    /** The statement a budgeted read of one shard runs. */
    String budgetedSql(boolean checkBlocked) {
        return checkBlocked ? budgeted : budgetedUnchecked;
    }

    /** Binds the parameters of {@link #sql(boolean)}. */
    void bind(PreparedStatement ps, int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit)
            throws SQLException {
        bind(ps, 0, shard, prev, target, lastId, limit);
    }

    /** Binds the parameters of {@link #budgetedSql(boolean)}. */
    void bindBudgeted(PreparedStatement ps, int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            long bytes, boolean oneAtLeast) throws SQLException {
        ps.setLong(1, bytes);
        ps.setBoolean(2, oneAtLeast);
        bind(ps, 2, shard, prev, target, lastId, limit);
        ps.setLong(11, bytes);
    }

    /** The parameters of {@link #sql(boolean)}, each {@code at} places on. */
    private static void bind(PreparedStatement ps, int at, int shard, PgSnapshot prev, PgSnapshot target, long lastId,
            int limit) throws SQLException {
        ps.setShort(at + 1, (short) shard);
        ps.setString(at + 2, Long.toString(prev.xmax()));
        ps.setString(at + 3, Long.toString(target.xmax()));
        ps.setShort(at + 4, (short) shard);
        // the candidate xip as a text[] parameter: text[] -> xid8[] folds at plan time, while a varchar goes through
        // the stable array_in and leaves the xip branch scanning every partition
        var xip = Arrays.stream(candidateXip(prev, target)).mapToObj(Long::toString).toArray(String[]::new);
        ps.setArray(at + 5, ps.getConnection().createArrayOf("text", xip));
        ps.setString(at + 6, target.text());
        ps.setLong(at + 7, lastId);
        ps.setInt(at + 8, limit);
    }

    /**
     * The candidate xip of the batch {@code (prev, target]}: in flight at prev but not at target. A transaction in
     * target's xip is invisible in target, so its rows (a long writer's, maybe many) need no fetch.
     */
    private static long[] candidateXip(PgSnapshot prev, PgSnapshot target) {
        var running = Arrays.stream(target.xip()).boxed().collect(Collectors.toSet());
        return Arrays.stream(prev.xip()).filter(x -> !running.contains(x)).toArray();
    }

    /** The statement {@link #readMany} runs. */
    String manySql() {
        return selectMany;
    }

    /** The statement a budgeted {@link #readMany} runs. */
    String budgetedManySql(boolean checkBlocked) {
        return checkBlocked ? budgetedMany : budgetedManyUnchecked;
    }

    /** The statement {@link #readMany} runs without read checks. */
    String uncheckedManySql() {
        return selectManyUnchecked;
    }

    /** Binds the parameters of {@link #manySql()}. */
    void bindMany(PreparedStatement ps, List<ShardRead> reads, int totalLimit) throws SQLException {
        bindMany(ps, 0, reads, totalLimit);
    }

    /** Binds the parameters of {@link #budgetedManySql}. */
    void bindBudgetedMany(PreparedStatement ps, List<ShardRead> reads, int totalLimit, long totalBytes,
            boolean oneAtLeast) throws SQLException {
        ps.setLong(1, totalBytes);
        ps.setBoolean(2, oneAtLeast);
        bindMany(ps, 2, reads, totalLimit);
        ps.setLong(18, totalBytes);
    }

    /** The parameters of {@link #manySql()}, each {@code at} places on. */
    private static void bindMany(PreparedStatement ps, int at, List<ShardRead> reads, int totalLimit)
            throws SQLException {
        int n = reads.size();
        var shard = new Integer[n];
        var pmax = new String[n];
        var tmax = new String[n];
        var lo = new String[n];
        var hi = new String[n];
        var xip = new String[n];
        var snap = new String[n];
        var lastId = new Long[n];
        var limit = new Integer[n];
        var check = new Boolean[n];
        for (int i = 0; i < n; i++) {
            var r = reads.get(i);
            var candidates = candidateXip(r.prev(), r.target());
            shard[i] = r.shard();
            pmax[i] = Long.toString(r.prev().xmax());
            tmax[i] = Long.toString(r.target().xmax());
            // null bounds when none: the branch then matches nothing and prunes every partition
            lo[i] = candidates.length == 0 ? null : Long.toString(Arrays.stream(candidates).min().getAsLong());
            hi[i] = candidates.length == 0 ? null : Long.toString(Arrays.stream(candidates).max().getAsLong());
            xip[i] = Arrays.stream(candidates).mapToObj(Long::toString).collect(Collectors.joining(",", "{", "}"));
            snap[i] = r.target().text();
            lastId[i] = r.lastId();
            limit[i] = r.limit();
            check[i] = r.checkBlocked();
        }
        var c = ps.getConnection();
        ps.setArray(at + 1, c.createArrayOf("int4", shard));
        ps.setArray(at + 2, c.createArrayOf("text", pmax));
        ps.setArray(at + 3, c.createArrayOf("text", tmax));
        ps.setArray(at + 4, c.createArrayOf("text", lo));
        ps.setArray(at + 5, c.createArrayOf("text", hi));
        ps.setArray(at + 6, c.createArrayOf("text", xip));
        ps.setArray(at + 7, c.createArrayOf("text", snap));
        ps.setArray(at + 8, c.createArrayOf("int8", lastId));
        ps.setArray(at + 9, c.createArrayOf("int4", limit));
        ps.setArray(at + 10, c.createArrayOf("bool", check));
        // every shard's rows lie in [min(prev xmax, its lowest candidate xip), max(target xmax)): a candidate xip is
        // in prev's xip, so at or above prev's xmin and below prev's xmax, which is at most target's
        long low = Long.MAX_VALUE;
        long high = Long.MIN_VALUE;
        for (var r : reads) {
            low = Math.min(low, Math.min(r.prev().xmax(), Arrays.stream(candidateXip(r.prev(), r.target())).min()
                    .orElse(Long.MAX_VALUE)));
            high = Math.max(high, r.target().xmax());
        }
        for (int i : new int[] {at + 11, at + 13}) {
            ps.setString(i, Long.toString(low));
            ps.setString(i + 1, Long.toString(high));
        }
        ps.setInt(at + 15, totalLimit);
    }

    /**
     * {@link #read} of each of {@code reads} (distinct shards) in one statement, at most {@code totalLimit} rows in
     * all: per shard what {@link #read} returns, unless {@link Batch#cut() cut}, and then a prefix of it for each
     * shard (the statement stops in the middle of one shard's rows, in the order of the reads, with the shards after
     * it unread). The caller's transaction must have autocommit off; partitions are pruned at run time, per shard.
     */
    public Batch readMany(Connection inTx, List<ShardRead> reads, int totalLimit) throws SQLException {
        return readMany(inTx, reads, totalLimit, null);
    }

    /**
     * {@link #readMany(Connection, List, int)} by the budgeted statement: at most {@code totalBytes} bytes in all too,
     * the first row however large when {@code oneAtLeast}; the rows are a prefix in the same way when it is
     * {@link Batch#bytesCut() cut by bytes}.
     */
    public Batch readMany(Connection inTx, List<ShardRead> reads, int totalLimit, long totalBytes, boolean oneAtLeast)
            throws SQLException {
        return readMany(inTx, reads, totalLimit, new Budget(totalBytes, oneAtLeast));
    }

    private Batch readMany(Connection inTx, List<ShardRead> reads, int totalLimit, @Nullable Budget budget)
            throws SQLException {
        if (totalLimit <= 0) {
            throw new IllegalArgumentException("totalLimit " + totalLimit);
        }
        var rows = new LinkedHashMap<Integer, List<BatchRow>>();
        for (var r : reads) {
            if (rows.put(r.shard(), new ArrayList<>()) != null) {
                throw new IllegalArgumentException("shard " + r.shard() + " read twice");
            }
        }
        if (reads.isEmpty()) {
            return new Batch(rows, false, false, 0);
        }
        if (reads.size() == 1) { // most group steps under bulk: read's own statement, about 40 % less per row
            var r = reads.get(0);
            var one = readOne(inTx, r.shard(), r.prev(), r.target(), r.lastId(), Math.min(r.limit(), totalLimit),
                    r.checkBlocked(), budget);
            rows.get(r.shard()).addAll(one.rows().get(r.shard()));
            return new Batch(rows, rows.get(r.shard()).size() == totalLimit, one.bytesCut(), one.bytes());
        }
        record Positioned(long position, BatchRow row) {}
        var read = new ArrayList<Positioned>();
        boolean check = reads.stream().anyMatch(ShardRead::checkBlocked);
        boolean bytesCut = false;
        long bytes = 0;
        String sql = budget == null ? (check ? selectMany : selectManyUnchecked) : budgetedManySql(check);
        try (var ps = inTx.prepareStatement(sql)) {
            ps.setFetchSize(Math.min(totalLimit, MAX_FETCH));
            if (budget == null) {
                bindMany(ps, reads, totalLimit);
            } else {
                bindBudgetedMany(ps, reads, totalLimit, budget.bytes(), budget.oneAtLeast());
            }
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (budget != null && !rs.getBoolean(11)) { // the marker: the budget was passed at this row
                        bytesCut = true;
                        continue;
                    }
                    var row = row(rs, 2, 10);
                    bytes += row.bytes();
                    read.add(new Positioned(rs.getLong(1), row));
                }
            }
        }
        // Why each shard's rows are a prefix of its read when the outer limit cuts the statement: the lateral is
        // ordered by id and limited per shard, so it yields the shard's rows in id order; the planner can only run it
        // as a nested loop over unnest's rows, which come out in ordinality order, so the statement emits shard by
        // shard and the outer limit stops inside one shard, with the shards after it unread; and the statement runs
        // as a portal with a fetch size, which Postgres never gives a parallel plan (a Gather would interleave the
        // shards' rows and the limit could keep a later row of a shard while dropping an earlier one). A change to
        // any of the three (an outer order by, a hash or merge join, no fetch size) breaks the prefix rule silently.
        // The budgeted statement's window takes the rows in the same order, so its byte cut is a prefix too.
        // No order by in the statement: the rows are sorted here, by position and id, and nothing else relies on it
        read.sort(Comparator.comparingLong(Positioned::position).thenComparingLong(p -> p.row().record().id()));
        for (var p : read) {
            rows.get(reads.get((int) p.position() - 1).shard()).add(p.row());
        }
        return new Batch(rows, read.size() == totalLimit, bytesCut, bytes);
    }

    /**
     * Rows of {@code (prev, target]} with {@code id > lastId}, at most {@code limit}, streamed in chunks of up to 5000
     * rows (the caller's transaction must have autocommit off and {@code plan_cache_mode = force_custom_plan} for
     * plan-time partition pruning). {@code checkBlocked = false} skips the per-row retry-table probe: the caller passes
     * it when the shard has no retry rows, and every row then reads as not blocked.
     */
    public List<BatchRow> read(Connection inTx, int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            boolean checkBlocked) throws SQLException {
        return readOne(inTx, shard, prev, target, lastId, limit, checkBlocked, null).rows().get(shard);
    }

    /** {@link #read}, by the budgeted statement when {@code budget} is set, as a {@link Batch} ({@code cut} unset). */
    private Batch readOne(Connection inTx, int shard, PgSnapshot prev, PgSnapshot target, long lastId, int limit,
            boolean checkBlocked, @Nullable Budget budget) throws SQLException {
        try (var ps = inTx.prepareStatement(budget == null ? sql(checkBlocked) : budgetedSql(checkBlocked))) {
            ps.setFetchSize(Math.min(limit, MAX_FETCH));
            if (budget == null) {
                bind(ps, shard, prev, target, lastId, limit);
            } else {
                bindBudgeted(ps, shard, prev, target, lastId, limit, budget.bytes(), budget.oneAtLeast());
            }
            try (var rs = ps.executeQuery()) {
                var rows = new ArrayList<BatchRow>();
                boolean bytesCut = false;
                long bytes = 0;
                while (rs.next()) {
                    if (budget != null && !rs.getBoolean(10)) { // the marker: the budget was passed at this row
                        bytesCut = true;
                        continue;
                    }
                    var row = row(rs, 1, 9);
                    bytes += row.bytes();
                    rows.add(row);
                }
                return new Batch(Map.of(shard, rows), false, bytesCut, bytes);
            }
        }
    }

    /**
     * The row whose id is in column {@code from}, followed by message id, topic, key, payload, headers, created,
     * shard, and its blocked flag in column {@code blocked}.
     */
    private static BatchRow row(ResultSet rs, int from, int blocked) throws SQLException {
        var key = rs.getBytes(from + 3);
        var payload = rs.getBytes(from + 4);
        var headers = rs.getBytes(from + 5);
        var record = new OutboxRecord(
                rs.getLong(from),
                rs.getObject(from + 1, UUID.class),
                rs.getString(from + 2),
                key,
                payload,
                HeaderCodec.decode(headers),
                rs.getObject(from + 6, OffsetDateTime.class).toInstant(),
                0,
                0,
                rs.getInt(from + 7));
        int bytes = (key == null ? 0 : key.length) + (payload == null ? 0 : payload.length)
                + (headers == null ? 0 : headers.length);
        return new BatchRow(record, rs.getBoolean(blocked), bytes);
    }
}
