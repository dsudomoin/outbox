package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTelemetry.ShardQueue;
import io.github.dsudomoin.outbox.internal.Cursors.Position;
import io.github.dsudomoin.outbox.internal.Sql.SqlConsumer;
import io.github.dsudomoin.outbox.internal.Ticks.Tick;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Each shard's pending messages and the age of the oldest, for {@link OutboxTelemetry#queueState}, from maintenance's
 * cursor read. The exact count is one statement: per shard the main stream's rows not visible in the shard's
 * effective tick (the latest for a shard not busy, which holds no row of it in between) and, for a batch in progress,
 * not those of its target at or below its {@code last_id}, so the rows its steps still read ({@link BatchReader}),
 * plus its retry and parked rows. It runs under {@code set local statement_timeout} of the budget, in a savepoint: one
 * that runs over (SQLSTATE 57014) is rolled back to it and the shards are estimated from the ticks instead, the retry
 * and parked rows counted by a second statement of the same budget. Everything runs in one transaction that ends with
 * a rollback (its own, idling at most {@link Maintenance#ABANDONED_AFTER}, on an autocommit connection; a savepoint of
 * the caller's otherwise), which reads only and so gives the session's own {@code statement_timeout} back.
 */
class QueueState {

    /** How long each statement may take: the count took 1.1–3.4 s on 1M fresh rows of 16 shards, 0.24 s vacuumed. */
    static final Duration BUDGET = Duration.ofMillis(250);
    private static final String QUERY_CANCELED = "57014";
    private static final String AGE = "greatest(coalesce(extract(epoch from now() - min(q.oldest)), 0), 0)::float8";

    private final int shards;
    private final long budgetMillis;
    private final String exact;
    private final String sides;

    QueueState(TableNames names, int shards, Duration budget) {
        this.shards = shards;
        this.budgetMillis = Math.max(1, budget.toMillis()); // 0 would turn the timeout off
        var sideRows = "select shard, count(*) n, min(created_at) oldest from " + names.retry() + " group by shard"
                + " union all select shard, count(*), min(created_at) from " + names.parked() + " group by shard";
        // per shard the (shard, xid) index from the effective tick's xmin on; each snapshot array is cast once
        this.exact = "select q.shard, sum(q.n)::float8, " + AGE + " from (select u.shard, o.n, o.oldest"
                + " from unnest(?::int[], ?::text[]::pg_snapshot[], ?::text[]::pg_snapshot[], ?::int8[])"
                + " u(shard, e, t, last_id) cross join lateral (select count(*) n, min(o.created_at) oldest from "
                + names.base() + " o where o.shard = u.shard and o.xid >= pg_snapshot_xmin(u.e)"
                + " and not pg_visible_in_snapshot(o.xid, u.e)"
                + " and (u.t is null or o.id > u.last_id or not pg_visible_in_snapshot(o.xid, u.t))) o"
                + " union all " + sideRows + ") q group by q.shard";
        this.sides = "select q.shard, sum(q.n)::float8, " + AGE + " from (" + sideRows + ") q group by q.shard";
    }

    /** Every shard unknown, as without a cursor read. */
    static List<ShardQueue> unknown(int shards) {
        var entries = new ArrayList<ShardQueue>(shards);
        for (int s = 0; s < shards; s++) {
            entries.add(new ShardQueue(s, Double.NaN, Double.NaN, true));
        }
        return List.copyOf(entries);
    }

    /** {@code outbox.pending}: the sum of the known counts, NaN when none is. */
    static double total(List<ShardQueue> entries) {
        return entries.stream().mapToDouble(ShardQueue::pending).filter(v -> !Double.isNaN(v)).reduce(Double::sum)
                .orElse(Double.NaN);
    }

    /** {@code outbox.pending.oldest}: the largest known age, NaN when none is. */
    static double oldest(List<ShardQueue> entries) {
        return entries.stream().mapToDouble(ShardQueue::oldestSeconds).filter(v -> !Double.isNaN(v)).max()
                .orElse(Double.NaN);
    }

    /**
     * One entry per shard, in shard order, from the positions read at {@code latest} and the ticks read with them
     * ({@code ticks}, by id: the effective ticks of the busy shards and the targets of batches in progress).
     */
    List<ShardQueue> read(Connection c, Tick latest, List<Position> positions, Map<Long, Tick> ticks)
            throws SQLException {
        var from = new Tick[shards];
        var target = new Tick[shards];
        var lastId = new long[shards];
        for (var p : positions) {
            int s = p.cursor().shard();
            if (s < 0 || s >= shards) {
                continue;
            }
            // a shard not busy has no row in the ticks between its effective tick and the latest
            var e = !Cursors.busy(p) && p.effectiveTickId() <= latest.id() ? latest
                    : tick(p.effectiveTickId(), latest, ticks);
            var targetId = p.cursor().targetTickId();
            var t = targetId == null ? null : tick(targetId, latest, ticks);
            if (e != null && e.snapshot() != null && (targetId == null || t != null && t.snapshot() != null)) {
                from[s] = e;
                target[s] = t;
                lastId[s] = p.cursor().lastId();
            }
        }
        boolean own = c.getAutoCommit();
        Savepoint outer = null;
        try {
            if (own) {
                c.setAutoCommit(false);
                execute(c, "set local idle_in_transaction_session_timeout = " + Maintenance.ABANDONED_AFTER.toMillis());
            } else {
                outer = c.setSavepoint();
            }
            var entries = readIn(c, latest, from, target, lastId);
            end(c, own, outer);
            return entries;
        } catch (Throwable t) {
            try {
                end(c, own, outer);
            } catch (SQLException | RuntimeException cleanup) {
                t.addSuppressed(cleanup);
            }
            throw t;
        }
    }

    private List<ShardQueue> readIn(Connection c, Tick latest, Tick[] from, @Nullable Tick[] target, long[] lastId)
            throws SQLException {
        var known = new ArrayList<Integer>();
        var snapshots = new ArrayList<String>();
        var targets = new ArrayList<String>();
        var lastIds = new ArrayList<Long>();
        for (int s = 0; s < shards; s++) {
            if (from[s] != null) {
                known.add(s);
                snapshots.add(from[s].snapshot().text());
                targets.add(target[s] == null ? null : target[s].snapshot().text());
                lastIds.add(lastId[s]);
            }
        }
        var counted = budgeted(c, exact, ps -> {
            ps.setArray(1, c.createArrayOf("int4", known.toArray()));
            ps.setArray(2, c.createArrayOf("text", snapshots.toArray()));
            ps.setArray(3, c.createArrayOf("text", targets.toArray()));
            ps.setArray(4, c.createArrayOf("int8", lastIds.toArray()));
        });
        var entries = new ArrayList<ShardQueue>(shards);
        if (counted != null) {
            for (int s = 0; s < shards; s++) {
                var row = counted.get(s);
                entries.add(from[s] == null ? new ShardQueue(s, Double.NaN, Double.NaN, true)
                        : row == null ? new ShardQueue(s, 0, 0, false) : new ShardQueue(s, row[0], row[1], false));
            }
            return List.copyOf(entries);
        }
        Instant now;
        try (var st = c.createStatement(); var rs = st.executeQuery("select now()")) {
            rs.next();
            now = rs.getObject(1, OffsetDateTime.class).toInstant();
        }
        var sideCounts = budgeted(c, sides, ps -> {});
        for (int s = 0; s < shards; s++) {
            if (from[s] == null) {
                entries.add(new ShardQueue(s, Double.NaN, Double.NaN, true));
                continue;
            }
            double pending = 0;
            double age = 0;
            if (from[s] != latest) { // behind or mid-batch: the ids handed out since its effective tick, spread evenly
                var to = latest.maxId();
                var at = from[s].maxId();
                pending = to == null || at == null ? Double.NaN : Math.max(0, to - at) / (double) shards;
                age = Math.max(0, Duration.between(from[s].createdAt(), now).toNanos() / 1e9);
            }
            var side = sideCounts == null ? null : sideCounts.get(s);
            if (side != null) {
                pending += side[0];
                age = Math.max(age, side[1]);
            }
            entries.add(new ShardQueue(s, pending, age, true));
        }
        return List.copyOf(entries);
    }

    /**
     * The rows of {@code sql} by shard (count, age in seconds) under the budget, in a savepoint rolled back after it,
     * which gives the timeout back; null when it ran over.
     */
    @Nullable
    private Map<Integer, double[]> budgeted(Connection c, String sql, SqlConsumer<PreparedStatement> bind)
            throws SQLException {
        var savepoint = c.setSavepoint();
        Map<Integer, double[]> rows = new HashMap<>();
        try {
            execute(c, "set local statement_timeout = " + budgetMillis);
            try (var ps = c.prepareStatement(sql)) {
                bind.accept(ps);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.put(rs.getInt(1), new double[] {rs.getDouble(2), rs.getDouble(3)});
                    }
                }
            }
        } catch (SQLException e) {
            if (!QUERY_CANCELED.equals(e.getSQLState())) {
                throw e; // the caller's rollback ends the transaction or the savepoint around it
            }
            rows = null;
        }
        c.rollback(savepoint);
        c.releaseSavepoint(savepoint);
        return rows;
    }

    @Nullable
    private static Tick tick(long id, Tick latest, Map<Long, Tick> ticks) {
        return id == latest.id() ? latest : ticks.get(id);
    }

    /** Reads only: a rollback, which also gives the settings back. */
    private static void end(Connection c, boolean own, @Nullable Savepoint outer) throws SQLException {
        if (!own) {
            if (outer != null) {
                c.rollback(outer);
                c.releaseSavepoint(outer);
            }
            return;
        }
        try {
            c.rollback();
        } finally {
            c.setAutoCommit(true);
        }
    }

    private static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }
}
