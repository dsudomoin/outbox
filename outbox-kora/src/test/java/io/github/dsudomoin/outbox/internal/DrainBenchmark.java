package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import io.github.dsudomoin.outbox.testing.TestTracing;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * Draining a backlog: {@code bench.rows} messages are written with the relay off (4 writers, 1000 messages a
 * transaction, random keys, {@code bench.payload}-byte payloads), then a relay on {@link Bench#relay} (the library's
 * defaults but {@code bench.batchSize} and a 30 s {@code batchTimeout}) starts and delivers them to a handler that
 * delivers at once. {@code bench.tickEveryMs} > 0 makes a tick that often during the fill, as a running relay would; 0
 * leaves the whole backlog to the relay's first tick. Prints the fill and drain rates.
 *
 * <p>Not part of the build: {@code ./gradlew :outbox-kora:test -Pbenchmark --tests '*DrainBenchmark'}; properties
 * {@code bench.rows} (2 000 000), {@code bench.shards} (256), {@code bench.batchSize} (the library's, 10 000),
 * {@code bench.workers} (4), {@code bench.tickEveryMs} (0), {@code bench.payload} (300).
 */
@Tag("benchmark")
class DrainBenchmark {

    private static final int ROWS = Integer.getInteger("bench.rows", 2_000_000);
    private static final int SHARDS = Integer.getInteger("bench.shards", 256);
    private static final int BATCH = Integer.getInteger("bench.batchSize",
            new OutboxConfig.RelayConfig() {}.batchSize());
    private static final int WORKERS = Integer.getInteger("bench.workers", 4);
    private static final int TICK_EVERY_MS = Integer.getInteger("bench.tickEveryMs", 0);
    private static final int PAYLOAD = Integer.getInteger("bench.payload", 300);
    private static final int WRITERS = 4;
    private static final int PER_TX = 1000;

    private final TestDatabase db = Bench.database(24);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void drain() throws Exception {
        var table = TestSql.uniqueTable();
        var writerConfig = config(table).relayEnabled(false);
        var payload = new byte[PAYLOAD];
        new Random(1).nextBytes(payload);
        var traceparent = TestTracing.TRACEPARENT.getBytes(UTF_8);
        long fillStarted;
        long fillNanos;
        try (var writer = OutboxRuntime.builder(writerConfig, db, record -> {}).telemetry(TestTelemetry.kora())
                .build()) {
            writer.start();
            var outbox = writer.outbox();
            var schema = new OutboxSchema(db, writerConfig);
            schema.start();
            var ticks = new Ticks(schema.names(), SHARDS);
            var filling = new AtomicBoolean(true);
            Thread ticker = null;
            if (TICK_EVERY_MS > 0) {
                ticker = new Thread(() -> {
                    while (filling.get()) {
                        try (var c = Bench.connect()) {
                            ticks.make(c, Duration.ZERO);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                        try {
                            Thread.sleep(TICK_EVERY_MS);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }, "ticker");
                ticker.start();
            }
            var written = new AtomicLong();
            var threads = new ArrayList<Thread>();
            fillStarted = System.nanoTime();
            for (int w = 0; w < WRITERS; w++) {
                var t = new Thread(() -> {
                    while (true) {
                        long from = written.getAndAdd(PER_TX);
                        if (from >= ROWS) {
                            return;
                        }
                        var messages = new ArrayList<OutboxMessage>(PER_TX);
                        for (int i = 0; i < PER_TX; i++) {
                            messages.add(new OutboxMessage("orders", UUID.randomUUID().toString().getBytes(UTF_8),
                                    payload, List.of(new OutboxHeader("traceparent", traceparent))));
                        }
                        db.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(messages));
                    }
                }, "writer-" + w);
                t.start();
                threads.add(t);
            }
            for (var t : threads) {
                t.join();
            }
            fillNanos = System.nanoTime() - fillStarted;
            filling.set(false);
            if (ticker != null) {
                ticker.join();
            }
        }
        long rows = ((ROWS + PER_TX - 1) / PER_TX) * (long) PER_TX;
        var delivered = new AtomicLong();
        if (Bench.URL != null) {
            try (var c = Bench.connect(); var st = c.createStatement()) {
                st.execute("select pg_stat_statements_reset()");
            }
        }
        long drainStarted = System.nanoTime();
        try (var relay = OutboxRuntime.builder(config(table), db, record -> delivered.incrementAndGet())
                .telemetry(TestTelemetry.kora()).build()) {
            relay.start();
            long last = 0;
            long lastAt = System.nanoTime();
            while (delivered.get() < rows) {
                Thread.sleep(100);
                long now = System.nanoTime();
                if (now - lastAt >= 5_000_000_000L) {
                    System.out.printf(Locale.ROOT, "  %.0f s: %.0f msg/s%n", (now - drainStarted) / 1e9,
                            (delivered.get() - last) / ((now - lastAt) / 1e9));
                    last = delivered.get();
                    lastAt = now;
                }
            }
        }
        long drainNanos = System.nanoTime() - drainStarted;
        if (Bench.URL != null) {
            try (var c = Bench.connect(); var st = c.createStatement(); var rs = st.executeQuery("select calls,"
                    + " round(total_exec_time::numeric, 0), round(total_exec_time::numeric * 1000 / " + rows
                    + ", 2), rows, left(regexp_replace(query, '\\s+', ' ', 'g'), 110) from pg_stat_statements"
                    + " where query not like '%pg_stat_statements%' order by total_exec_time desc limit 25")) {
                System.out.println("calls, total ms, µs per drained row, rows, statement");
                while (rs.next()) {
                    System.out.printf(Locale.ROOT, "  %7d %8s %7s %9d  %s%n", rs.getLong(1), rs.getString(2),
                            rs.getString(3), rs.getLong(4), rs.getString(5));
                }
            }
        }
        System.out.printf(Locale.ROOT, "%npayload %d B, rows %d, shards %d, workers %d, batchSize %d, tick every %d ms during the"
                        + " fill: filled at %.0f msg/s, drained in %.1f s at %.0f msg/s%n", PAYLOAD, rows, SHARDS, WORKERS,
                BATCH, TICK_EVERY_MS, rows / (fillNanos / 1e9), drainNanos / 1e9, rows / (drainNanos / 1e9));
    }

    private static TestOutboxConfig config(String table) {
        return Bench.relay(SHARDS, WORKERS, BATCH, 50).table(table);
    }
}
