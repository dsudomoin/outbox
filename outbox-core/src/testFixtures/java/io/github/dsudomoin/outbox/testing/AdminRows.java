package io.github.dsudomoin.outbox.testing;

import static io.github.dsudomoin.outbox.testing.Utf8.bytes;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.internal.HeaderCodec;
import io.github.dsudomoin.outbox.internal.TableNames;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/** The rows the admin HTTP tests of both adapters seed and read in one outbox's tables. */
public final class AdminRows {

    public static final String API = "/kora-outbox/admin/v1";

    /** An admin route: its operation, method, path ({@code {orphan}} is the test's orphan table) and body. */
    public record Route(String operation, String method, String path, String body) {

        @Override
        public String toString() {
            return operation;
        }
    }

    /** All ten, in the contract's order, each acting on what {@link #seedEveryRoute} made when allowed. */
    public static final List<Route> ROUTES = List.of(
            new Route("getStatus", "GET", API + "/status", null),
            new Route("findDeadLetters", "GET", API + "/dead-letters?topic=t", null),
            new Route("countDeadLetters", "GET", API + "/dead-letters/count?topic=t", null),
            new Route("redriveDeadLetters", "POST", API + "/dead-letters/redrive", "{\"ids\":[1]}"),
            new Route("discardDeadLetters", "POST", API + "/dead-letters/discard", "{\"ids\":[1]}"),
            new Route("restoreOrphan", "POST", API + "/orphans/{orphan}/restore?fromXid=0", null),
            new Route("recoverStrandedShards", "POST", API + "/recovery/stranded-shards", null),
            new Route("recoverXidRegression", "POST", API + "/recovery/xid-regression", null),
            new Route("abandonKey", "POST", API + "/keys/abandon",
                    "{\"topic\":\"t\",\"key\":\"k\",\"reason\":\"test\"}"),
            new Route("abandonWaiting", "POST", API + "/waiting/abandon", "{\"topic\":\"t\",\"reason\":\"test\"}"));

    private final ConnectionSource connections;
    private final TableNames names;

    public AdminRows(ConnectionSource connections, TableNames names) {
        this.connections = connections;
        this.names = names;
    }

    /** What an allowed call of {@code route} on {@link #seedEveryRoute}'s rows adds to its audit line. */
    public static String allowedDetail(Route route) {
        return switch (route.operation()) {
            case "redriveDeadLetters", "discardDeadLetters" -> " with count \"1\", ids \"[1]\"";
            case "restoreOrphan" -> " with fromXid \"0\"";
            case "abandonWaiting" -> " with topic \"t\", key null, reason \"test\", dryRun \"false\"";
            case "abandonKey" -> " with topic \"t\", key \"k\", messageId null, reason \"test\", dryRun \"false\"";
            default -> "";
        };
    }

    /**
     * A dead letter as the relay writes one: payload p, headers h1=v and h2 without a value, created an hour before
     * {@code failedAt}; a pending or scheduled one is due an hour after it.
     */
    public long letter(long id, String topic, String key, String error, Instant failedAt, DeadLetter.Status status)
            throws SQLException {
        var headers = HeaderCodec.encode(List.of(new OutboxHeader("h1", bytes("v")), new OutboxHeader("h2", null)));
        try (var c = connections.newConnection(); var ps = c.prepareStatement("insert into " + names.dead()
                + " (id, shard, message_id, topic, key, payload, headers, created_at, attempts, failed_at, error,"
                + " status, due_at) values (?, 0, gen_random_uuid(), ?, ?, ?, ?, ?, 3, ?, ?, ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, topic);
            ps.setBytes(3, bytes(key));
            ps.setBytes(4, bytes("p"));
            ps.setBytes(5, headers);
            ps.setObject(6, failedAt.minusSeconds(3600).atOffset(ZoneOffset.UTC));
            ps.setObject(7, failedAt.atOffset(ZoneOffset.UTC));
            ps.setString(8, error);
            ps.setString(9, status.name().toLowerCase(Locale.ROOT));
            ps.setObject(10, status == DeadLetter.Status.KEPT ? null
                    : failedAt.plusSeconds(3600).atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        }
        return id;
    }

    /**
     * Topic t, shard 0: waiting head 10 of key k with follower 11 and parked row 5; unkeyed waiting head 20; waiting
     * head 30 of key ключ.
     */
    public void seedWaiting() throws SQLException {
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at, next_attempt_at,"
                + " waiting_since) values (10, 0, gen_random_uuid(), 't', 'k', now(), now(), now()),"
                + " (11, 0, gen_random_uuid(), 't', 'k', now(), null, null),"
                + " (20, 0, gen_random_uuid(), 't', null, now(), now(), now()),"
                + " (30, 0, gen_random_uuid(), 't', convert_to('ключ', 'UTF8'), now(), now(), now())");
        execute("insert into " + names.parked() + " (id, shard, message_id, topic, key, created_at, due_at)"
                + " values (5, 0, gen_random_uuid(), 't', 'k', now(), now())");
    }

    /**
     * Something for every mutating route to change: kept dead letter 1, the foreign orphan {@link #orphan} with one
     * row, one outbox row, a tick whose snapshot xmax lies 10⁹ xids past the counter, as after a dump/restore, waiting
     * retry head 2, retry head 3 of key k, and the shards stranded.
     */
    public void seedEveryRoute() throws SQLException {
        execute("insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                + " values (1, 0, gen_random_uuid(), 't', now(), 3)");
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, created_at, next_attempt_at,"
                + " waiting_since) values (2, 0, gen_random_uuid(), 't', now(), now(), now())");
        execute("insert into " + names.retry() + " (id, shard, message_id, topic, key, created_at,"
                + " next_attempt_at) values (3, 0, gen_random_uuid(), 't', 'k', now(), now())");
        foreignOrphan();
        execute("insert into " + orphan() + " (id, shard, xid, message_id, topic)"
                + " values (1, 0, '10', gen_random_uuid(), 't')");
        execute("insert into " + names.base() + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
        forgeRegression();
        // the cursors' tick gone, so the shards stranded; the forged tick above is the latest
        execute("delete from " + names.tick() + " where id = (select min(tick_id) from " + names.cursor() + ")");
    }

    /** What the mutating routes change: dead letter ids, outbox message ids, the orphan's rows, ticks and cursors. */
    public String state() throws SQLException {
        try (var c = connections.newConnection()) {
            var orphanRows = TestSql.queryString(c, "select to_regclass(?)::text", orphan()) == null ? "dropped"
                    : TestSql.queryString(c, "select count(*)::text from " + orphan());
            return String.join(" | ",
                    TestSql.queryString(c, "select string_agg(id::text, ',' order by id) from " + names.dead()),
                    TestSql.queryString(c, "select string_agg(message_id::text, ',' order by id) from "
                            + names.base()),
                    orphanRows,
                    TestSql.queryString(c, "select string_agg(id::text, ',' order by id) from " + names.tick()),
                    TestSql.queryString(c, "select string_agg(seq::text, ',' order by seq) from " + names.cursor()));
        }
    }

    /** The orphan {@link #seedEveryRoute} makes: {@code <base>_x1000}. */
    public String orphan() {
        return names.partitionPrefix() + "1000";
    }

    public UUID messageId(long letter) throws SQLException {
        try (var c = connections.newConnection()) {
            return UUID.fromString(TestSql.queryString(c, "select message_id::text from " + names.dead()
                    + " where id = " + letter));
        }
    }

    /** A standalone {@code <base>_x1000}, below every range of the outbox, as someone else's detach leaves one. */
    public String foreignOrphan() throws SQLException {
        var orphan = orphan();
        execute("create table " + orphan + " (like " + names.base() + " including defaults)");
        return orphan;
    }

    /** A row of a standalone range with that xid; returns its message id. */
    public UUID orphanRow(String table, long id, long xid) throws SQLException {
        var messageId = UUID.randomUUID();
        try (var c = connections.newConnection(); var ps = c.prepareStatement("insert into " + table
                + " (id, shard, xid, message_id, topic, key, payload) values (?, 0, ?::text::xid8, ?, 't', ?, ?)")) {
            ps.setLong(1, id);
            ps.setString(2, Long.toString(xid));
            ps.setObject(3, messageId);
            ps.setBytes(4, bytes("k" + id));
            ps.setBytes(5, bytes("row"));
            ps.executeUpdate();
        }
        return messageId;
    }

    /**
     * As after a dump/restore: a tick whose snapshot xmax lies 10⁹ xids past the counter, made a minute ago; returns
     * that xmax.
     */
    public long forgeRegression() throws SQLException {
        try (var c = connections.newConnection()) {
            long xmax = TestSql.currentXid(c) + 1_000_000_000L;
            TestSql.execute(c, "insert into " + names.tick() + " (snapshot, created_at) values ('" + xmax + ":" + xmax
                    + ":'::pg_snapshot, now() - interval '1 minute')");
            return xmax;
        }
    }

    public List<Long> deadIds() throws SQLException {
        try (var c = connections.newConnection()) {
            return TestSql.queryLongs(c, "select id from " + names.dead() + " order by id");
        }
    }

    public List<UUID> outboxMessageIds() throws SQLException {
        try (var c = connections.newConnection()) {
            return TestSql.queryStrings(c, "select message_id::text from " + names.base() + " order by id").stream()
                    .map(UUID::fromString).toList();
        }
    }

    public boolean exists(String table) throws SQLException {
        try (var c = connections.newConnection()) {
            return TestSql.queryString(c, "select to_regclass(?)::text", table) != null;
        }
    }

    public void execute(String sql) throws SQLException {
        try (var c = connections.newConnection()) {
            TestSql.execute(c, sql);
        }
    }
}
