package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

/** What the benchmarks share: the relay's config, their database, and the traceparent their messages carry. */
final class Bench {

    /** With {@code bench.user} and {@code bench.password}: a Postgres of your own; null for the tests' one. */
    static final String URL = System.getProperty("bench.jdbcUrl");
    static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

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
}
