package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

/** What the benchmarks share: the relay's config and their database. */
final class Bench {

    /** With {@code bench.user} and {@code bench.password}: a Postgres of your own; null for the tests' one. */
    static final String URL = System.getProperty("bench.jdbcUrl");

    private Bench() {}

    /**
     * The library's relay and partition defaults but {@code batchTimeout}, 30 s here (the library's is 130 s), with
     * these shards, workers, batch size and minimal tick interval.
     */
    static TestOutboxConfig relay(int shards, int workers, int batchSize, int minTickMs) {
        return new TestOutboxConfig().shards(shards).workers(workers).batchSize(batchSize)
                .minTickInterval(Duration.ofMillis(minTickMs)).idlePollInterval(Duration.ofMillis(200))
                .batchTimeout(Duration.ofSeconds(30)).maintenanceInterval(Duration.ofSeconds(30))
                .xidsPerPartition(16_777_216L).premake(64).retry(Duration.ofSeconds(1), Duration.ofMinutes(5), 0);
    }

    /** A Kora pool of {@code size} over {@link #URL}'s Postgres; the caller closes it with {@code release()}. */
    static TestDatabase database(int size) {
        return URL == null ? TestDb.newDatabase(size)
                : TestDb.newDatabase(URL, System.getProperty("bench.user"), System.getProperty("bench.password"), size);
    }

    /** A plain connection to {@link #URL}'s Postgres. */
    static Connection connect() throws SQLException {
        return URL == null ? TestDb.connect() : DriverManager.getConnection(URL, System.getProperty("bench.user"),
                System.getProperty("bench.password"));
    }

    /**
     * A new business table of {@code keys} rows {@code (k, 0)}, {@code k} its primary key, whose rows the writers bump
     * under their row lock; its name.
     */
    static String businessTable(int keys) throws SQLException {
        var business = TestSql.uniqueTable();
        try (var c = connect()) {
            TestSql.execute(c, "create table " + business + " (k int primary key, v bigint not null)");
            TestSql.execute(c, "insert into " + business + " select g, 0 from generate_series(0, " + (keys - 1)
                    + ") g");
        }
        return business;
    }

    /** The current WAL insert position. */
    static String lsn(Connection c) throws SQLException {
        return TestSql.queryString(c, "select pg_current_wal_insert_lsn()::text");
    }

    /** Nearest-rank quantile of sorted nanoseconds, in ms; NaN when there are none. */
    static double ms(long[] sorted, double q) {
        if (sorted.length == 0) {
            return Double.NaN;
        }
        int i = Math.min(sorted.length - 1, (int) Math.ceil(q * sorted.length) - 1);
        return sorted[Math.max(0, i)] / 1e6;
    }
}
