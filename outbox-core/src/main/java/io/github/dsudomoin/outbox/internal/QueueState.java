package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.internal.Sql.execute;

import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTelemetry.ShardQueue;
import io.github.dsudomoin.outbox.internal.Sql.SqlConsumer;
import io.github.dsudomoin.outbox.internal.Ticks.Tick;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Each shard's pending messages and the age of the oldest, for {@link OutboxTelemetry#queueState}. One read-only
 * repeatable-read transaction on an autocommit connection, idling at most {@link Maintenance#ABANDONED_AFTER}: the
 * latest tick, the shards' positions and the ticks they stand on, then the count, all in one snapshot, so the rows a
 * relay step moved and the cursor it moved with them (one commit) are seen together. The exact count is one statement:
 * per shard the main stream's rows not visible in the shard's effective tick (the latest for a shard not busy, which
 * holds no row of it in between) and, for a batch in progress, not those of its target at or below its
 * {@code last_id}, so the rows its steps still read ({@link BatchReader}), plus its retry and parked rows. It runs
 * under {@code set local} {@code statement_timeout} and {@code lock_timeout} of the budget, in a savepoint: one that
 * runs over (SQLSTATE 57014 or 55P03) is rolled back to it and the shards are estimated from the ticks instead, the retry and
 * parked rows counted by a second statement of the same budget. The transaction ends with a rollback, which gives the
 * session's own settings back.
 */
class QueueState {

    /** How long each statement may take: the count took 1.1–3.4 s on 1M fresh rows of 16 shards, 0.24 s vacuumed. */
    static final Duration BUDGET = Duration.ofMillis(250);
    private static final String QUERY_CANCELED = "57014";
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String AGE = "greatest(coalesce(extract(epoch from now() - min(q.oldest)), 0), 0)::float8";

    private final int shards;
    private final Ticks ticks;
    private final Cursors cursors;
    private final long budgetMillis;
    private final String exact;
    private final String sides;

    QueueState(TableNames names, int shards, Duration budget) {
        this.shards = shards;
        this.ticks = new Ticks(names, shards);
        this.cursors = new Cursors(names, shards);
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

    /** One entry per shard, in shard order; {@code c} in autocommit, as it is left. */
    List<ShardQueue> read(Connection c) throws SQLException {
        if (!c.getAutoCommit()) {
            throw new IllegalStateException("the queue state reads in a transaction of its own");
        }
        c.setAutoCommit(false);
        try {
            execute(c, "set transaction isolation level repeatable read, read only"); // before the snapshot
            execute(c, "set local idle_in_transaction_session_timeout = " + Maintenance.ABANDONED_AFTER.toMillis());
            var entries = readIn(c);
            c.rollback();
            return entries;
        } catch (Throwable t) {
            try {
                c.rollback();
            } catch (SQLException | RuntimeException cleanup) {
                t.addSuppressed(cleanup);
            }
            throw t;
        } finally {
            c.setAutoCommit(true);
        }
    }

    private List<ShardQueue> readIn(Connection c) throws SQLException {
        var latest = ticks.latest(c);
        var positions = cursors.positions(c, latest.id());
        var found = ticks.get(c, Cursors.ticksToRead(positions, latest.id()));
        var from = new Tick[shards];
        var target = new Tick[shards];
        var lastId = new long[shards];
        for (var p : positions) {
            int s = p.cursor().shard(); // 0..shards-1: positions reads generate_series(0, shards - 1)
            // a shard not busy has no row in the ticks between its effective tick and the latest
            var e = !Cursors.checked(p, latest.id()) ? latest
                    : tick(p.effectiveTickId(), latest, found);
            var targetId = p.cursor().targetTickId();
            var t = targetId == null ? null : tick(targetId, latest, found);
            if (e != null && e.snapshot() != null && (targetId == null || t != null && t.snapshot() != null)) {
                from[s] = e;
                target[s] = t;
                lastId[s] = p.cursor().lastId();
            }
        }
        return count(c, latest, from, target, lastId);
    }

    private List<ShardQueue> count(Connection c, Tick latest, Tick[] from, @Nullable Tick[] target, long[] lastId)
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
            // a target above the latest cannot be in one snapshot (the latest is the highest tick id seen), but it
            // would bound the batch
            var upTo = target[s] != null && target[s].id() > latest.id() ? target[s] : latest;
            if (from[s] != upTo) { // behind or mid-batch: the ids handed out since its effective tick, spread evenly
                var to = upTo.maxId();
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
     * which gives the timeouts back; null when it ran over or waited a lock out.
     */
    @Nullable
    private Map<Integer, double[]> budgeted(Connection c, String sql, SqlConsumer<PreparedStatement> bind)
            throws SQLException {
        var savepoint = c.setSavepoint();
        Map<Integer, double[]> rows = new HashMap<>();
        try {
            execute(c, "set local statement_timeout = " + budgetMillis + "; set local lock_timeout = " + budgetMillis);
            try (var ps = c.prepareStatement(sql)) {
                bind.accept(ps);
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.put(rs.getInt(1), new double[] {rs.getDouble(2), rs.getDouble(3)});
                    }
                }
            }
        } catch (SQLException e) {
            if (!QUERY_CANCELED.equals(e.getSQLState()) && !LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
                throw e; // the rollback of the transaction ends it
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
}
