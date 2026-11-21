package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import io.github.dsudomoin.outbox.testing.TestTracing;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * WAL per delivered message with one message per transaction: writers each bump a business row of their key under its
 * row lock and send one message, at a fixed total rate, while the relay runs on {@link Bench#relay} (the library's
 * defaults but a 30 s {@code batchTimeout}) with a handler that delivers at once. Prints the WAL bytes per message over
 * the measured window and where they went, by relation and record type ({@code pg_walinspect}), plus cursor rows and
 * ticks per message.
 *
 * <p>Not part of the build: {@code ./gradlew :outbox-kora:test -Pbenchmark --tests '*WalPerMessageBenchmark'};
 * properties {@code bench.shards} (256), {@code bench.rate} (2200 tx/s), {@code bench.writers} (16), {@code bench.keys}
 * (1000), {@code bench.minTickMs} (50), {@code bench.warmup} (15 s), {@code bench.seconds} (30 s).
 */
@Tag("benchmark")
class WalPerMessageBenchmark {

    private static final int SHARDS = Integer.getInteger("bench.shards", 256);
    private static final int RATE = Integer.getInteger("bench.rate", 2200);
    private static final int WRITERS = Integer.getInteger("bench.writers", 16);
    private static final int KEYS = Integer.getInteger("bench.keys", 1000);
    private static final int MIN_TICK_MS = Integer.getInteger("bench.minTickMs", 50);
    private static final int WARMUP = Integer.getInteger("bench.warmup", 15);
    private static final int SECONDS = Integer.getInteger("bench.seconds", 30);
    private static final int PAYLOAD = 300;

    private final TestDatabase db = Bench.database(WRITERS + 16);

    @AfterEach
    void release() {
        db.release();
    }

    @Test
    void oneMessagePerTransaction() throws Exception {
        var config = Bench.relay(SHARDS, 4, new OutboxConfig.RelayConfig() {}.batchSize(), MIN_TICK_MS);
        try (var c = Bench.connect()) {
            execute(c, "create extension if not exists pg_walinspect");
        }
        var business = Bench.businessTable(KEYS);
        var delivered = new AtomicLong();
        try (var runtime = OutboxRuntime.builder(config, db, record -> delivered.incrementAndGet())
                .telemetry(TestTelemetry.kora()).build()) {
            runtime.start();
            var outbox = runtime.outbox();
            var payload = new byte[PAYLOAD];
            new Random(1).nextBytes(payload);
            var traceparent = TestTracing.TRACEPARENT.getBytes(UTF_8);
            var update = "update " + business + " set v = v + 1 where k = ?";
            var running = new AtomicBoolean(true);
            var committed = new AtomicLong();
            var threads = new ArrayList<Thread>();
            long intervalNanos = 1_000_000_000L * WRITERS / RATE;
            for (int w = 0; w < WRITERS; w++) {
                long seed = w;
                var t = new Thread(() -> {
                    var random = new Random(seed);
                    long next = System.nanoTime();
                    while (running.get()) {
                        int k = random.nextInt(KEYS);
                        var message = new OutboxMessage("orders", ("u" + k).getBytes(UTF_8), payload,
                                List.of(new OutboxHeader("traceparent", traceparent)));
                        db.inTx((SqlFunction0<Void>) () -> {
                            try (var ps = db.currentConnection().prepareStatement(update)) {
                                ps.setInt(1, k);
                                ps.executeUpdate();
                            }
                            outbox.send(message);
                            return null;
                        });
                        committed.incrementAndGet();
                        next += intervalNanos;
                        long left = next - System.nanoTime();
                        if (left > 0) {
                            LockSupport.parkNanos(left);
                        }
                    }
                }, "writer-" + w);
                t.start();
                threads.add(t);
            }
            Thread.sleep(WARMUP * 1000L);
            try (var c = Bench.connect()) {
                var cursor = config.table() + "_cursor";
                var tick = config.table() + "_tick";
                String lsn0 = Bench.lsn(c);
                long d0 = delivered.get();
                long w0 = committed.get();
                long cursors0 = inserted(c, cursor);
                long ticks0 = inserted(c, tick);
                long xid0 = queryLong(c, "select pg_current_xact_id()::text::bigint");
                Thread.sleep(SECONDS * 1000L);
                String lsn1 = Bench.lsn(c);
                long d1 = delivered.get();
                long w1 = committed.get();
                long cursors1 = inserted(c, cursor);
                long ticks1 = inserted(c, tick);
                long xid1 = queryLong(c, "select pg_current_xact_id()::text::bigint");
                running.set(false);
                for (var t : threads) {
                    t.join();
                }
                long wal = queryLong(c, "select '" + lsn1 + "'::pg_lsn - '" + lsn0 + "'::pg_lsn");
                long messages = d1 - d0;
                System.out.printf(Locale.ROOT, "%nshards %d, minTickInterval %d ms, writers %d, rate %d tx/s:"
                        + " committed %.0f tx/s, delivered %.0f msg/s, xids %.0f/s%n", SHARDS, MIN_TICK_MS, WRITERS,
                        RATE, (w1 - w0) / (double) SECONDS, messages / (double) SECONDS,
                        (xid1 - xid0) / (double) SECONDS);
                System.out.printf(Locale.ROOT, "WAL %.1f B/message; cursor rows %.3f/message (%.0f/s), ticks %.1f/s%n",
                        wal / (double) messages, (cursors1 - cursors0) / (double) messages,
                        (cursors1 - cursors0) / (double) SECONDS, (ticks1 - ticks0) / (double) SECONDS);
                byRelation(c, config.table(), business, lsn0, lsn1, messages);
                byRecord(c, lsn0, lsn1, messages);
            }
        }
    }

    private static long inserted(Connection c, String table) throws Exception {
        execute(c, "select pg_stat_clear_snapshot()");
        return queryLong(c, "select coalesce(n_tup_ins, 0) from pg_stat_user_tables where relid = '" + table
                + "'::regclass");
    }

    /** WAL bytes per message by the relation of each record's first block, its FPI bytes too. */
    private static void byRelation(Connection c, String base, String business, String lsn0, String lsn1,
            long messages) throws Exception {
        var sql = "select case when c.relname is null then 'rfn ' || b.relfilenode"
                + " when c.relname like '" + base + "\\_x%' or c.relname like '" + base + "\\_tail%' then"
                + " case when c.relkind = 'i' then 'outbox partition index' else 'outbox partition heap' end"
                + " else c.relname end rel, sum(b.record_length), sum(coalesce(b.block_fpi_length, 0)), count(*)"
                + " from pg_get_wal_block_info('" + lsn0 + "', '" + lsn1 + "') b"
                + " left join pg_class c on c.relfilenode = b.relfilenode and b.block_id = 0"
                + " where b.block_id = 0 group by 1 order by 2 desc limit 25";
        System.out.println("by relation (first block): B/message, FPI B/message, records/message");
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf(Locale.ROOT, "  %-40s %8.1f %8.1f %7.3f%n",
                        rs.getString(1).replace(business, "business"), rs.getLong(2) / (double) messages,
                        rs.getLong(3) / (double) messages, rs.getLong(4) / (double) messages);
            }
        }
    }

    private static void byRecord(Connection c, String lsn0, String lsn1, long messages) throws Exception {
        var sql = "select \"resource_manager/record_type\", count, combined_size, fpi_size from pg_get_wal_stats('"
                + lsn0 + "', '" + lsn1 + "', true) where count > 0 order by combined_size desc limit 25";
        System.out.println("by record type: B/message, FPI B/message, records/message");
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf(Locale.ROOT, "  %-40s %8.1f %8.1f %7.3f%n", rs.getString(1),
                        rs.getLong(3) / (double) messages, rs.getLong(4) / (double) messages,
                        rs.getLong(2) / (double) messages);
            }
        }
    }
}
