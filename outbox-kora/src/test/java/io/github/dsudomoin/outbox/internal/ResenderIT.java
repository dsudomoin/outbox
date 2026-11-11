package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

class ResenderIT {

    private static final short STALE_SHARD = 999;

    private final TestDatabase db = TestDb.newDatabase(4);
    private final TestOutboxConfig config = new TestOutboxConfig().relayEnabled(false);
    private final ExecutorService pool = Executors.newFixedThreadPool(2);
    private OutboxSchema schema;
    private TableNames names;

    record Stored(long id, int shard, UUID messageId, String topic, byte[] key, byte[] payload, byte[] headers) {}

    @BeforeEach
    void bootstrap() throws Exception {
        schema = new OutboxSchema(db, config);
        schema.start();
        names = schema.names();
    }

    @AfterEach
    void release() {
        pool.shutdownNow();
        db.release();
    }

    @Test
    void keepsMessageIdAndHeadersAndRecomputesTheShard() throws Exception {
        var headers = HeaderCodec.encode(List.of(new OutboxHeader("h", bytes("v")), new OutboxHeader("n", null)));
        try (var c = TestDb.connect(); var ps = c.prepareStatement("insert into " + names.base()
                + " (shard, message_id, topic, key, payload, headers) values (?, gen_random_uuid(), ?, ?, ?, ?)")) {
            // sharded for another shard count, as rows written before a reshard or set aside elsewhere
            for (var row : List.of(new Object[] {"t", bytes("a"), bytes("1"), headers},
                    new Object[] {"t", null, bytes("2"), null}, new Object[] {"u", bytes("b"), null, headers},
                    new Object[] {"t", bytes("a"), bytes("3"), null})) {
                ps.setShort(1, STALE_SHARD);
                ps.setString(2, (String) row[0]);
                ps.setBytes(3, (byte[]) row[1]);
                ps.setBytes(4, (byte[]) row[2]);
                ps.setBytes(5, (byte[]) row[3]);
                ps.executeUpdate();
            }
        }
        var stale = stored();
        var rows = stale.stream()
                .map(s -> new Resender.Row(s.messageId(), s.topic(), s.key(), s.payload(), s.headers()))
                .toList();

        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            assertThat(new Resender(names, config.shards()).insertAll(c, rows)).isEqualTo(rows.size());
            c.commit();
        }

        var resent = stored().stream().filter(s -> s.shard() != STALE_SHARD).toList();
        assertThat(resent).extracting(Stored::messageId).as("new ids in list order")
                .containsExactlyElementsOf(rows.stream().map(Resender.Row::messageId).toList());
        long lastStale = stale.get(stale.size() - 1).id();
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            var s = resent.get(i);
            assertThat(s.id()).isGreaterThan(lastStale);
            assertThat(s.topic()).isEqualTo(row.topic());
            assertThat(s.key()).isEqualTo(row.key());
            assertThat(s.payload()).isEqualTo(row.payload());
            assertThat(s.headers()).as("the stored encoding").isEqualTo(row.headers());
            if (row.key() == null) {
                assertThat(s.shard()).isBetween(0, config.shards() - 1);
            } else {
                assertThat(s.shard())
                        .isEqualTo(Hashing.shard(Hashing.keyHash(row.topic(), row.key()), config.shards()));
            }
        }
    }

    @Test
    void waitsForAConcurrentSendOfTheSameKey() throws Exception {
        var outbox = new PgOutbox(db, schema, config.shards(), () -> {}, TestTelemetry.kora());
        var sent = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var a = pool.submit(() -> db.inTx((SqlFunction0<UUID>) () -> {
            var id = outbox.send(OutboxMessage.of("t", "k", bytes("a")));
            sent.countDown();
            await(commit);
            return id;
        }));
        assertThat(sent.await(10, SECONDS)).as("A holds the key lock").isTrue();
        var other = new Resender.Row(UUID.randomUUID(), "t", bytes("other"), bytes("o"), null);
        var same = new Resender.Row(UUID.randomUUID(), "t", bytes("k"), bytes("b"), null);
        var b = pool.submit(() -> {
            try (var c = TestDb.connect()) {
                c.setAutoCommit(false);
                new Resender(names, config.shards()).insertAll(c, List.of(other, same));
                c.commit();
            }
            return null;
        });

        try (var c = TestDb.connect()) {
            long deadline = System.nanoTime() + SECONDS.toNanos(10);
            while (queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and not granted"
                    + " and classid::bigint = " + Integer.toUnsignedLong(names.keyNamespace())) == 0) {
                assertThat(System.nanoTime()).as("B waits for the key lock").isLessThan(deadline);
                Thread.sleep(5);
            }
        }
        assertThat(b.isDone()).isFalse();
        commit.countDown();
        var aId = a.get(10, SECONDS);
        b.get(10, SECONDS);

        assertThat(stored()).extracting(Stored::messageId).as("ids in commit order")
                .containsExactly(aId, other.messageId(), same.messageId());
    }

    @Test
    void insertsAListOverTheStatementBoundInConsecutiveStatementsInListOrder() throws Exception {
        var rows = new ArrayList<Resender.Row>();
        for (int i = 0; i < 51; i++) {
            var payload = new byte[i == 25 ? 1_500_000 : 100_000]; // one row over the bound on its own
            payload[0] = (byte) i;
            rows.add(new Resender.Row(UUID.randomUUID(), "t", i % 2 == 0 ? null : bytes("k" + i % 7), payload, null));
        }
        var inserts = new AtomicInteger();
        var locks = new AtomicInteger();

        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            var counted = counting(c, inserts, locks);
            assertThat(new Resender(names, config.shards(), ShardGuard.NONE, 1 << 20).insertAll(counted, rows))
                    .isEqualTo(rows.size());
            c.commit();
        }

        // 10 rows of 100 KB fit 1 MB: 0-9, 10-19, 20-24, the 1.5 MB row alone, 26-35, 36-45, 46-50
        assertThat(inserts).as("insert statements").hasValue(7);
        assertThat(locks).as("every key locked once, before the first insert").hasValue(1);
        var stored = stored();
        assertThat(stored).extracting(Stored::messageId).as("ids in list order")
                .containsExactlyElementsOf(rows.stream().map(Resender.Row::messageId).toList());
        for (int i = 0; i < rows.size(); i++) {
            assertThat(stored.get(i).payload()).isEqualTo(rows.get(i).payload());
        }
        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(distinct xmin::text) from " + names.base()))
                    .as("one transaction").isEqualTo(1);
        }
    }

    /**
     * About 1.13 GB of payloads, over the 1 GB a Bind message may carry; the rows share one array, so only the
     * driver's encoding of a statement's arrays takes heap, 64 MB a statement. As one statement the list would fail
     * in the driver with "Bind message length ... too long", after encoding 1.13 GB in the heap.
     */
    @Test
    @Tag("chaos")
    void insertsAListOverTheBindMessageLimit() throws Exception {
        var payload = new byte[1_100_000];
        var rows = new ArrayList<Resender.Row>();
        for (int i = 0; i < 1030; i++) {
            rows.add(new Resender.Row(UUID.randomUUID(), "t", null, payload, null));
        }

        try (var c = TestDb.connect()) {
            c.setAutoCommit(false);
            assertThat(new Resender(names, config.shards()).insertAll(c, rows)).isEqualTo(rows.size());
            c.commit();
        }

        try (var c = TestDb.connect()) {
            assertThat(queryLong(c, "select count(*) from " + names.base())).isEqualTo(rows.size());
        }
    }

    /** {@code c}, counting the insert and key-lock statements prepared on it. */
    private static Connection counting(Connection c, AtomicInteger inserts, AtomicInteger locks) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("prepareStatement") && args[0] instanceof String sql) {
                        if (sql.startsWith("insert ")) {
                            inserts.incrementAndGet();
                        } else if (sql.contains("pg_advisory_xact_lock")) {
                            locks.incrementAndGet();
                        }
                    }
                    try {
                        return method.invoke(c, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private List<Stored> stored() throws SQLException {
        var rows = new ArrayList<Stored>();
        try (var c = TestDb.connect();
                var st = c.createStatement();
                var rs = st.executeQuery("select id, shard, message_id, topic, key, payload, headers from "
                        + names.base() + " order by id")) {
            while (rs.next()) {
                rows.add(new Stored(rs.getLong(1), rs.getInt(2), rs.getObject(3, UUID.class), rs.getString(4),
                        rs.getBytes(5), rs.getBytes(6), rs.getBytes(7)));
            }
        }
        return rows;
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
