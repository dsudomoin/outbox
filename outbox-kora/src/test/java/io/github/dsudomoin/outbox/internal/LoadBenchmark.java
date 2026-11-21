package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * Latency and writer throughput under load, the relay on {@link Bench#relay} (the library's defaults but a 30 s
 * {@code batchTimeout}) and a handler that delivers at once: {@code bench.writers} threads each commit transactions of
 * {@code bench.perTx} messages, at {@code bench.rate} transactions a second in all (0: as fast as they go). With one
 * message a transaction each bumps the business row of a key among {@code bench.keys} under its row lock first, as an
 * order service does; with more, it sends them in one {@code sendAll}, each with a random key. Prints committed
 * transactions a second, delivered messages a second, the latency from a writer's commit to the handler's call (p50,
 * p90, p99, max, sampled messages) and WAL per message.
 *
 * <p>Not part of the build: {@code ./gradlew :outbox-kora:test -Pbenchmark --tests '*LoadBenchmark'}; properties
 * {@code bench.shards} (256), {@code bench.writers} (16), {@code bench.perTx} (1), {@code bench.rate} (2200),
 * {@code bench.keys} (1000), {@code bench.batchSize} (the library's, 10 000), {@code bench.minTickMs} (50),
 * {@code bench.warmup} (15 s), {@code bench.seconds} (30 s), and {@code bench.jdbcUrl}, {@code bench.user},
 * {@code bench.password} for a Postgres of your own.
 */
@Tag("benchmark")
class LoadBenchmark {

    private static final int SHARDS = Integer.getInteger("bench.shards", 256);
    private static final int WRITERS = Integer.getInteger("bench.writers", 16);
    private static final int PER_TX = Integer.getInteger("bench.perTx", 1);
    private static final int RATE = Integer.getInteger("bench.rate", 2200);
    private static final int KEYS = Integer.getInteger("bench.keys", 1000);
    private static final int BATCH = Integer.getInteger("bench.batchSize",
            new OutboxConfig.RelayConfig() {}.batchSize());
    private static final int MIN_TICK_MS = Integer.getInteger("bench.minTickMs", 50);
    private static final int WARMUP = Integer.getInteger("bench.warmup", 15);
    private static final int SECONDS = Integer.getInteger("bench.seconds", 30);
    /** Messages of a sendAll whose latency is sampled. */
    private static final int SAMPLED_PER_TX = 10;

    private final TestDatabase db = Bench.database(WRITERS + 16);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void load() throws Exception {
        var config = Bench.relay(SHARDS, 4, BATCH, MIN_TICK_MS);
        var business = TestDb.uniqueTable();
        try (var c = Bench.connect(); var st = c.createStatement()) {
            st.execute("create table " + business + " (k int primary key, v bigint not null)");
            st.execute("insert into " + business + " select g, 0 from generate_series(0, " + (KEYS - 1) + ") g");
        }
        // sampled message id -> commit time (nanoTime); the handler takes it out and records the latency
        var committedAt = new ConcurrentHashMap<UUID, Long>();
        var measuring = new AtomicBoolean(false);
        var latencies = new long[4_000_000];
        var latencyCount = new AtomicLong();
        var delivered = new AtomicLong();
        try (var runtime = OutboxRuntime.builder(config, db, record -> {
            delivered.incrementAndGet();
            var at = committedAt.remove(record.messageId());
            if (at != null && measuring.get()) {
                long i = latencyCount.getAndIncrement();
                if (i < latencies.length) {
                    latencies[(int) i] = System.nanoTime() - at;
                }
            }
        }).telemetry(TestTelemetry.kora()).build()) {
            runtime.start();
            var outbox = runtime.outbox();
            var payload = new byte[300];
            new Random(1).nextBytes(payload);
            var traceparent = Bench.TRACEPARENT.getBytes(UTF_8);
            var headers = List.of(new OutboxHeader("traceparent", traceparent));
            var update = "update " + business + " set v = v + 1 where k = ?";
            var running = new AtomicBoolean(true);
            var committed = new AtomicLong();
            var threads = new ArrayList<Thread>();
            long intervalNanos = RATE == 0 ? 0 : 1_000_000_000L * WRITERS / RATE;
            for (int w = 0; w < WRITERS; w++) {
                long seed = w;
                var t = new Thread(() -> {
                    var random = new Random(seed);
                    long next = System.nanoTime();
                    while (running.get()) {
                        if (PER_TX == 1) {
                            int k = random.nextInt(KEYS);
                            var message = new OutboxMessage("orders", ("u" + k).getBytes(UTF_8), payload, headers);
                            var id = db.inTx((SqlFunction0<UUID>) () -> {
                                try (var ps = db.currentConnection().prepareStatement(update)) {
                                    ps.setInt(1, k);
                                    ps.executeUpdate();
                                }
                                return outbox.send(message);
                            });
                            committedAt.put(id, System.nanoTime());
                        } else {
                            var messages = new ArrayList<OutboxMessage>(PER_TX);
                            for (int i = 0; i < PER_TX; i++) {
                                messages.add(new OutboxMessage("orders",
                                        UUID.randomUUID().toString().getBytes(UTF_8), payload, headers));
                            }
                            var ids = db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
                            long now = System.nanoTime();
                            for (int i = 0; i < SAMPLED_PER_TX; i++) {
                                committedAt.put(ids.get(random.nextInt(ids.size())), now);
                            }
                        }
                        committed.incrementAndGet();
                        if (intervalNanos > 0) {
                            next += intervalNanos;
                            long left = next - System.nanoTime();
                            if (left > 0) {
                                LockSupport.parkNanos(left);
                            }
                        }
                    }
                }, "writer-" + w);
                t.start();
                threads.add(t);
            }
            Thread.sleep(WARMUP * 1000L);
            try (var c = Bench.connect()) {
                String lsn0 = lsn(c);
                long d0 = delivered.get();
                long w0 = committed.get();
                measuring.set(true);
                Thread.sleep(SECONDS * 1000L);
                measuring.set(false);
                String lsn1 = lsn(c);
                long d1 = delivered.get();
                long w1 = committed.get();
                running.set(false);
                for (var t : threads) {
                    t.join();
                }
                long wal = queryLong(c, "select '" + lsn1 + "'::pg_lsn - '" + lsn0 + "'::pg_lsn");
                int n = (int) Math.min(latencyCount.get(), latencies.length);
                var sorted = Arrays.copyOf(latencies, n);
                Arrays.sort(sorted);
                System.out.printf(Locale.ROOT, "%nshards %d, batchSize %d, minTickInterval %d ms, writers %d x %d"
                                + " messages a transaction, rate %s: committed %.1f tx/s (%.0f msg/s), delivered %.0f"
                                + " msg/s, WAL %.1f B/message%n", SHARDS, BATCH, MIN_TICK_MS, WRITERS, PER_TX,
                        RATE == 0 ? "unthrottled" : RATE + " tx/s", (w1 - w0) / (double) SECONDS,
                        (w1 - w0) * (double) PER_TX / SECONDS, (d1 - d0) / (double) SECONDS,
                        wal / (double) (d1 - d0));
                System.out.printf(Locale.ROOT, "latency commit -> handler: p50 %.1f ms, p90 %.1f, p99 %.1f, max %.1f"
                        + " (%d sampled)%n", ms(sorted, 0.5), ms(sorted, 0.9), ms(sorted, 0.99), ms(sorted, 1.0), n);
            }
        }
    }

    private static double ms(long[] sorted, double q) {
        if (sorted.length == 0) {
            return Double.NaN;
        }
        int i = Math.min(sorted.length - 1, (int) Math.ceil(q * sorted.length) - 1);
        return sorted[Math.max(0, i)] / 1e6;
    }

    private static String lsn(Connection c) throws SQLException {
        return queryString(c, "select pg_current_wal_insert_lsn()::text");
    }
}
