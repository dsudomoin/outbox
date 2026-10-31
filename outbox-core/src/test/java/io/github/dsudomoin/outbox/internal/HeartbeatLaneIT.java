package io.github.dsudomoin.outbox.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * The heartbeat on a connection source of its own keeps its row and the writer guard's read fresh while business load
 * holds every connection of the application's pool; on the application's, both go stale.
 */
class HeartbeatLaneIT {

    private static final Duration INTERVAL = Duration.ofMillis(500);
    /** {@value OutboxSchema#LIVE_INTERVALS} intervals: older than that, the row has expired and the read is stale. */
    private static final long STALE_MILLIS = INTERVAL.toMillis() * OutboxSchema.LIVE_INTERVALS;
    private static final String SHARING = "outbox heartbeat shares the application's connections";
    private static final String OTHER = "d" + TestSql.uniqueTable();
    private static final DataSource DIRECT = TestDataSources.newDataSource();

    @BeforeAll
    static void otherDatabase() throws SQLException {
        try (var c = DIRECT.getConnection()) {
            TestSql.execute(c, "create database " + OTHER);
        }
    }

    @AfterAll
    static void dropIt() throws SQLException {
        try (var c = DIRECT.getConnection()) {
            TestSql.execute(c, "drop database " + OTHER + " with (force)");
        }
    }

    @Test
    void beatsStayFreshWhileTheAppPoolIsExhausted() throws Exception {
        var run = exhaust(true);

        assertThat(run.second()).as("beats while the gate is closed").isGreaterThan(run.first());
        assertThat(run.rowAge()).isLessThan(STALE_MILLIS);
        assertThat(run.readAge()).isLessThan(STALE_MILLIS);
        assertThat(run.unchecked()).isEmpty();
        assertThat(run.rows()).as("the row, deleted on close behind the closed gate").isZero();
    }

    @Test
    void withoutTheDedicatedSourceTheSnapshotGoesStale() throws Exception {
        var run = exhaust(false);

        assertThat(run.rowAge()).isGreaterThan(STALE_MILLIS);
        assertThat(run.unchecked()).as("the send went in on a stale read").containsExactly("audit");
        assertThat(run.rows()).as("the row, whose delete on close needs the closed gate").isOne();
    }

    @Test
    void sharingLogsOneInfoLine() throws Exception {
        assertThat(startLog(false)).containsOnlyOnce(SHARING);
        assertThat(startLog(true)).doesNotContain(SHARING);
    }

    @Test
    void otherDatabaseFailsTheStartBeforeAnyRow() throws Exception {
        var other = (PGSimpleDataSource) TestDataSources.newDataSource();
        other.setDatabaseName(OTHER);
        var config = new TestOutboxConfig();
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), null).route(new Audit())
                .heartbeatConnections(other::getConnection).build();

        assertThatThrownBy(runtime::start).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reaches another database than the application's");
        try (var c = DIRECT.getConnection()) {
            assertThat(TestSql.queryString(c, "select to_regclass(?)::text", config.table() + "_instance"))
                    .as("no instance table, so no row: the check ran before the bootstrap").isNull();
        }
    }

    /** What {@link #exhaust} saw; ages in milliseconds, {@code rows}: the instance rows left after the close. */
    private record Run(long first, long second, long rowAge, long readAge, List<String> unchecked, long rows) {}

    /**
     * Starts a runtime on a gated application pool, with the heartbeat on its own source when {@code lane}; after the
     * first beat closes the gate inside a transaction, waits 8 intervals, sampling {@code last_seen} twice, 2 s apart,
     * sends a routed message and commits. Then measures, closes the runtime with the gate still closed, and opens it.
     */
    private static Run exhaust(boolean lane) throws Exception {
        var gate = new Gate();
        var transactions = new JdbcTransactions(gate);
        var unchecked = new CopyOnWriteArrayList<String>();
        var builder = OutboxRuntime.builder(new TestOutboxConfig().maintenanceInterval(INTERVAL), transactions, null)
                .route(new Audit()).telemetry(new OutboxTelemetry() {
                    @Override
                    public void routeUnchecked(String topic) {
                        unchecked.add(topic);
                    }
                });
        if (lane) {
            builder.heartbeatConnections(DIRECT::getConnection);
        }
        var runtime = builder.build();
        runtime.start();
        var names = runtime.schema().names();
        var samples = new long[2];
        long rowAge;
        long readAge;
        try {
            long bootstrapped = millis("last_seen", names);
            await(() -> millis("last_seen", names) > bootstrapped);
            var id = transactions.inTx(c -> {
                gate.closed = true;
                sleep(1000);
                samples[0] = millis("last_seen", names);
                sleep(2000);
                samples[1] = millis("last_seen", names);
                sleep(1000);
                return runtime.outbox().send(OutboxMessage.of("audit", "k", "p".getBytes(UTF_8)));
            });
            rowAge = millis("now() - last_seen", names);
            var read = runtime.schema().contractGuard().snapshot();
            readAge = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - read.readAt);
            try (var c = DIRECT.getConnection()) {
                assertThat(TestSql.queryString(c, "select message_id::text from " + names.base()))
                        .as("the send went in").isEqualTo(id.toString());
            }
        } finally {
            runtime.close();
            gate.closed = false;
        }
        try (var c = DIRECT.getConnection()) {
            return new Run(samples[0], samples[1], rowAge, readAge, List.copyOf(unchecked),
                    TestSql.queryLong(c, "select count(*) from " + names.instance()));
        }
    }

    /** The start's log of a runtime on a fresh table, with the heartbeat on its own source when {@code lane}. */
    private static String startLog(boolean lane) {
        var builder = OutboxRuntime.builder(new TestOutboxConfig().relayEnabled(false),
                new JdbcTransactions(DIRECT), null).route(new Audit());
        if (lane) {
            builder.heartbeatConnections(DIRECT::getConnection);
        }
        try (var runtime = builder.build(); var log = new CapturedStderr()) {
            runtime.start();
            return log.text();
        }
    }

    /** {@code expression} of the instance's row, in milliseconds since the epoch or as a duration. */
    private static long millis(String expression, TableNames names) throws SQLException {
        try (var c = DIRECT.getConnection()) {
            return TestSql.queryLong(c, "select (extract(epoch from " + expression + ") * 1000)::bigint from "
                    + names.instance());
        }
    }

    private static void sleep(long millis) throws SQLException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException(e);
        }
    }

    private static void await(Check condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10 s");
            }
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }

    /** The application's pool, exhausted while closed: a connection is then refused after a 200 ms wait. */
    private static final class Gate extends PGSimpleDataSource {

        volatile boolean closed;

        Gate() {
            setUrl(TestDataSources.jdbcUrl());
            setUser(TestDataSources.user());
            setPassword(TestDataSources.password());
        }

        @Override
        public Connection getConnection() throws SQLException {
            if (closed) {
                sleep(200);
                throw new SQLException("gate closed");
            }
            return super.getConnection();
        }
    }

    private static final class Audit implements OutboxTopicHandler {

        @Override
        public Set<String> topics() {
            return Set.of("audit");
        }

        @Override
        public void handle(OutboxRecord record) {}
    }
}
