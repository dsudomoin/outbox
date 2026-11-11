package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.internal.BatchReader.ShardRead;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The chunk read with small messages: 1000 rows of 300 bytes between two ticks, read as a group step reads them (8
 * shards in one statement, {@link BatchReader#readMany}) and as a bulk step does (one shard, read's own statement), by
 * the plain statement and by the budgeted one, interleaved, each under {@code force_custom_plan} in a transaction of
 * its own, as a step runs it. Prints p50, p90 and the mean of each after warm-up, and both plans with
 * {@code EXPLAIN (ANALYZE, BUFFERS)}. {@code steadyStream}: relay steps of 8 shards over a stream of such rows, and
 * which statement each step's chunk read ran.
 *
 * <p>Not part of the build: {@code ./gradlew :outbox-kora:test -Pbenchmark --tests '*ChunkReadBenchmark'}; property
 * {@code bench.rounds} (default 300), {@code bench.batchBytes} (of {@code steadyStream}, default 16 MiB).
 */
@Tag("benchmark")
class ChunkReadBenchmark {

    private static final int ROWS = 1000;
    private static final int PAYLOAD = 300;
    private static final int WARMUP = 100;
    /** What the relay passes by default: {@code outbox.relay.batchBytes}. */
    private static final long BATCH_BYTES = 16L * 1024 * 1024;

    private final TestDatabase db = TestDb.newDatabase(2);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void groupOfEightShards() throws Exception {
        measure(8);
    }

    @Test
    void oneShard() throws Exception {
        measure(1);
    }

    @Test
    void steadyStream() throws Exception {
        int shards = 8;
        var config = new TestOutboxConfig().shards(shards).batchSize(ROWS)
                .batchBytes(Long.getLong("bench.batchBytes", BATCH_BYTES));
        var schema = new OutboxSchema(db, config);
        schema.start();
        var names = schema.names();
        var ticks = new Ticks(names, shards);
        var outbox = new PgOutbox(db, schema, shards, () -> {}, TestTelemetry.kora());
        var reader = new BatchReader(names);
        var plain = List.of(reader.manySql(), reader.uncheckedManySql(), reader.sql(true), reader.sql(false));
        var budgeted = List.of(reader.budgetedManySql(true), reader.budgetedManySql(false), reader.budgetedSql(true),
                reader.budgetedSql(false));
        try (var processor = new ShardProcessor(names, config, record -> {}, TestTelemetry.kora(), false)) {
            var random = new Random(2);
            var group = IntStream.range(0, shards).boxed().toList();
            var steps = new ArrayList<String>();
            for (int round = 0; round < 20; round++) {
                send(db, outbox, random);
                long latest;
                try (var c = TestDb.connect()) {
                    ticks.make(c, Duration.ZERO);
                    latest = ticks.latest(c).id();
                }
                var prepared = new ArrayList<String>();
                try (var c = recording(TestDb.connect(), prepared)) {
                    ShardProcessor.prepareTransaction(c, config.batchTimeout());
                    processor.tryProcess(c, group, latest, Instant.now());
                    c.commit();
                }
                steps.add(String.join("+", prepared.stream().filter(sql -> plain.contains(sql)
                        || budgeted.contains(sql)).map(sql -> budgeted.contains(sql) ? "budgeted" : "plain").toList()));
            }
            System.out.println("steady stream of " + ROWS + " rows of " + PAYLOAD + " B a step, chunk reads: " + steps);
        }
    }

    private void measure(int shards) throws Exception {
        int rounds = Integer.getInteger("bench.rounds", 300);
        var config = new TestOutboxConfig().shards(shards).batchSize(ROWS);
        var schema = new OutboxSchema(db, config);
        schema.start();
        var names = schema.names();
        var ticks = new Ticks(names, shards);
        var outbox = new PgOutbox(db, schema, shards, () -> {}, TestTelemetry.kora());
        var reader = new BatchReader(names);
        var p = tick(ticks);
        send(db, outbox, new Random(1));
        var c = tick(ticks);
        var reads = IntStream.range(0, shards).mapToObj(s -> new ShardRead(s, p, c, 0, ROWS, false)).toList();

        try (var conn = TestDb.connect()) {
            conn.setAutoCommit(false);
            var times = new long[2][rounds];
            for (int i = 0; i < WARMUP + rounds; i++) {
                for (int k = 0; k < 2; k++) {
                    boolean budgeted = (i + k) % 2 == 1;
                    execute(conn, "set local plan_cache_mode = force_custom_plan");
                    long started = System.nanoTime();
                    var batch = budgeted ? reader.readMany(conn, reads, ROWS, BATCH_BYTES, true)
                            : reader.readMany(conn, reads, ROWS);
                    long took = System.nanoTime() - started;
                    conn.commit();
                    if (batch.rows().values().stream().mapToInt(List::size).sum() != ROWS) {
                        throw new AssertionError("not all rows read");
                    }
                    if (i >= WARMUP) {
                        times[budgeted ? 1 : 0][i - WARMUP] = took;
                    }
                }
            }
            for (int k = 0; k < 2; k++) {
                var t = times[k];
                Arrays.sort(t);
                System.out.printf(Locale.ROOT, "chunk read, %d shards, %d rows of %d B, %s: p50 %.0f us, p90 %.0f us,"
                        + " mean %.0f us over %d rounds%n", shards, ROWS, PAYLOAD, k == 0 ? "plain" : "budgeted",
                        t[rounds / 2] / 1e3, t[rounds * 9 / 10] / 1e3, Arrays.stream(t).average().orElse(0) / 1e3,
                        rounds);
            }

            for (boolean budgeted : new boolean[] {false, true}) {
                execute(conn, "set local plan_cache_mode = force_custom_plan");
                var r = reads.get(0);
                String sql = shards == 1 ? (budgeted ? reader.budgetedSql(false) : reader.sql(false))
                        : (budgeted ? reader.budgetedManySql(false) : reader.uncheckedManySql());
                try (var ps = conn.prepareStatement("explain (analyze, buffers) " + sql)) {
                    if (shards == 1 && budgeted) {
                        reader.bindBudgeted(ps, r.shard(), r.prev(), r.target(), r.lastId(), r.limit(), BATCH_BYTES,
                                true);
                    } else if (shards == 1) {
                        reader.bind(ps, r.shard(), r.prev(), r.target(), r.lastId(), r.limit());
                    } else if (budgeted) {
                        reader.bindBudgetedMany(ps, reads, ROWS, BATCH_BYTES, true);
                    } else {
                        reader.bindMany(ps, reads, ROWS);
                    }
                    try (var rs = ps.executeQuery()) {
                        var plan = new ArrayList<String>();
                        while (rs.next()) {
                            plan.add(rs.getString(1));
                        }
                        System.out.println((budgeted ? "budgeted" : "plain") + " plan:\n" + String.join("\n", plan));
                    }
                }
                conn.rollback();
            }
        }
    }

    /** {@link #ROWS} rows of {@link #PAYLOAD} random bytes over random keys, in 10 transactions. */
    private static void send(TestDatabase db, PgOutbox outbox, Random random) {
        for (int tx = 0; tx < 10; tx++) {
            var messages = IntStream.range(0, ROWS / 10).mapToObj(i -> {
                var payload = new byte[PAYLOAD];
                random.nextBytes(payload);
                return OutboxMessage.of("t", "k" + random.nextInt(10_000), payload);
            }).toList();
            db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
        }
    }

    /** {@code c}, adding the SQL of each statement it prepares to {@code prepared}. */
    private static Connection recording(Connection c, List<String> prepared) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().startsWith("prepare") && args != null && args[0] instanceof String sql) {
                        prepared.add(sql);
                    }
                    try {
                        return method.invoke(c, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static PgSnapshot tick(Ticks ticks) throws SQLException {
        try (Connection c = TestDb.connect()) {
            ticks.make(c, Duration.ZERO);
            return ticks.latest(c).snapshot();
        }
    }
}
