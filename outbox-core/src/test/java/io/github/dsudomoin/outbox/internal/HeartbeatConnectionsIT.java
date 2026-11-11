package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/** The heartbeat's own connections, the application's as the fallback, and the check that both reach one database. */
class HeartbeatConnectionsIT {

    private static final String OTHER = "d" + TestSql.uniqueTable();
    private static final String TABLE = "public." + TestSql.uniqueTable();
    private static final DataSource MAIN = TestDataSources.newDataSource();

    @BeforeAll
    static void databases() throws SQLException {
        try (var c = MAIN.getConnection()) {
            TestSql.execute(c, "create database " + OTHER);
            TestSql.execute(c, "create table " + TABLE + " (id int)");
        }
    }

    @AfterAll
    static void dropThem() throws SQLException {
        try (var c = MAIN.getConnection()) {
            TestSql.execute(c, "drop table " + TABLE);
            TestSql.execute(c, "drop database " + OTHER + " with (force)");
        }
    }

    @Test
    void aFailedBeatLeavesNoTransactionOnASourceThatKeepsItsConnection() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(50));
        try (var kept = MAIN.getConnection()) {
            ConnectionSource keeping = () -> (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class}, (c, method, args) -> {
                        if (method.getName().equals("close")) {
                            return null; // a source that rolls nothing back and keeps the connection
                        }
                        try {
                            return method.invoke(kept, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            var runtime = OutboxRuntime.builder(config, new JdbcTransactions(MAIN), record -> {})
                    .heartbeatConnections(keeping).build();
            runtime.start();
            try (var holder = MAIN.getConnection()) {
                var instances = runtime.schema().names().instance();
                holder.setAutoCommit(false);
                TestSql.execute(holder, "lock table " + instances + " in access exclusive mode");
                Thread.sleep(300); // beats give up on their lock wait
                holder.rollback();
                var lastSeen = "select (extract(epoch from last_seen) * 1000000)::bigint from " + instances
                        + " where instance_id = '" + runtime.schema().instanceId() + "'";
                long seen = TestSql.queryLong(holder, lastSeen);
                holder.commit();

                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (true) {
                    boolean moved = TestSql.queryLong(holder, lastSeen) > seen;
                    holder.commit();
                    if (moved) {
                        break;
                    }
                    assertThat(System.nanoTime()).as("a beat gets through once the lock is gone").isLessThan(deadline);
                    Thread.sleep(20);
                }
            } finally {
                runtime.close();
            }
        }
    }

    /** A hot standby after a failover, or a role whose transactions default to read-only: 25006 on the upsert. */
    @Test
    void aReadOnlySourceLeavesTheBeatsAndTheDeleteToTheApplication() throws Exception {
        beatsAndDeleteGoToTheApplication("set default_transaction_read_only = on", "25006");
    }

    /** A role without the rights on {@code <base>_instance}: 42501. */
    @Test
    void aSourceWithoutTheRightsLeavesTheBeatsAndTheDeleteToTheApplication() throws Exception {
        var role = "r" + TestSql.uniqueTable();
        try (var c = MAIN.getConnection()) {
            TestSql.execute(c, "create role " + role + " nologin");
        }
        try {
            beatsAndDeleteGoToTheApplication("set role " + role, "42501");
        } finally {
            try (var c = MAIN.getConnection()) {
                TestSql.execute(c, "drop role " + role);
            }
        }
    }

    /**
     * A heartbeat source whose sessions run {@code setup} connects and passes the start check, then cannot write: the
     * beats keep the row live on the application's connections, the lane goes off with one ERROR naming
     * {@code sqlState}, and the close deletes the row.
     */
    private static void beatsAndDeleteGoToTheApplication(String setup, String sqlState) throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(100));
        ConnectionSource cannotWrite = () -> {
            var c = MAIN.getConnection();
            TestSql.execute(c, setup);
            return c;
        };
        try (var log = new CapturedStderr()) {
            var runtime = OutboxRuntime.builder(config, new JdbcTransactions(MAIN), record -> {})
                    .heartbeatConnections(cannotWrite).build();
            runtime.start();
            var row = runtime.schema().names().instance() + " where instance_id = '" + runtime.schema().instanceId()
                    + "'";
            try (var holder = MAIN.getConnection()) {
                try {
                    var lastSeen = "select (extract(epoch from last_seen) * 1000000)::bigint from " + row;
                    long seen = TestSql.queryLong(holder, lastSeen);
                    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    while (TestSql.queryLong(holder, lastSeen) <= seen) {
                        assertThat(System.nanoTime()).as("a beat refreshes the row").isLessThan(deadline);
                        Thread.sleep(20);
                    }
                } finally {
                    runtime.close();
                }
                assertThat(TestSql.queryLong(holder, "select count(*) from " + row)).as("deleted at close").isZero();
            }
            assertThat(log.text()).containsOnlyOnce("heartbeat connection source of").contains(sqlState)
                    .doesNotContain("could not delete its row");
        }
    }

    /**
     * The whole database read-only for a while (a reload of {@code default_transaction_read_only}, a managed server at
     * its storage limit): the retry on the application's connections fails the same way, so the source is not to blame,
     * keeps its lane without an ERROR, and serves the beats again once the database writes.
     */
    @Test
    void aDatabaseThatCannotWriteLeavesTheLaneOn() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(100));
        var readOnly = new AtomicBoolean();
        var app = readOnlyWhile(readOnly);
        var own = new AtomicInteger();
        ConnectionSource lane = () -> {
            own.incrementAndGet();
            return app.getConnection();
        };
        try (var log = new CapturedStderr()) {
            var runtime = OutboxRuntime.builder(config, new JdbcTransactions(app), record -> {})
                    .heartbeatConnections(lane).build();
            runtime.start();
            try {
                readOnly.set(true);
                awaitLog(log, "outbox heartbeat of instance");
                readOnly.set(false);
                int at = own.get();
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (own.get() < at + 2) {
                    assertThat(System.nanoTime()).as("the beats are back on the lane").isLessThan(deadline);
                    Thread.sleep(20);
                }
            } finally {
                runtime.close();
            }
            assertThat(log.text()).doesNotContain("cannot write");
        }
    }

    /**
     * A lane that could not write is off for a few maintenance intervals only, then measured again: a standby promoted,
     * a role fixed, and the beats are back on it without a restart.
     */
    @Test
    void aLaneOffForCannotWriteIsMeasuredAgain() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).maintenanceInterval(Duration.ofMillis(100));
        var readOnly = new AtomicBoolean();
        var lanes = readOnlyWhile(readOnly);
        var own = new AtomicInteger();
        ConnectionSource lane = () -> {
            own.incrementAndGet();
            return lanes.getConnection();
        };
        try (var log = new CapturedStderr()) {
            var runtime = OutboxRuntime.builder(config, new JdbcTransactions(MAIN), record -> {})
                    .heartbeatConnections(lane).build();
            runtime.start();
            try {
                readOnly.set(true);
                awaitLog(log, "cannot write");
                readOnly.set(false);
                int at = own.get();
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (own.get() < at + 2) {
                    assertThat(System.nanoTime()).as("the lane is measured again and serves").isLessThan(deadline);
                    Thread.sleep(20);
                }
            } finally {
                runtime.close();
            }
            assertThat(log.text()).containsOnlyOnce("cannot write");
        }
    }

    private static void awaitLog(CapturedStderr log, String text) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!log.text().contains(text)) {
            assertThat(System.nanoTime()).as("logged: " + text).isLessThan(deadline);
            Thread.sleep(20);
        }
    }

    /** Connections of the main database whose transactions default to read-only while {@code readOnly} is set. */
    private static DataSource readOnlyWhile(AtomicBoolean readOnly) {
        var ds = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                var c = super.getConnection();
                if (readOnly.get()) {
                    TestSql.execute(c, "set default_transaction_read_only = on");
                }
                return c;
            }
        };
        ds.setUrl(TestDataSources.jdbcUrl());
        ds.setUser(TestDataSources.user());
        ds.setPassword(TestDataSources.password());
        return ds;
    }

    @Test
    void matchingSourceAtStartServesTheBeats() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> false);
        var connections = new HeartbeatConnections(app, lane);

        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }
        for (int i = 0; i < 3; i++) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(lane.opened.get(i + 1));
            }
        }

        assertThat(lane.calls).isEqualTo(4);
        assertThat(app.calls).isZero();
        assertThat(lane.opened.get(0).isClosed()).as("the check's connection").isTrue();
    }

    @Test
    void otherDatabaseAtStartFailsIt() throws Exception {
        var lane = new Recorded("lane", other(), n -> false);
        var connections = new HeartbeatConnections(new Recorded("app", MAIN, n -> false), lane);

        try (var reference = MAIN.getConnection()) {
            assertThatThrownBy(() -> connections.start(reference, TABLE)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reaches another database than the application's")
                    .hasMessageContaining("database " + OTHER)
                    .hasMessageContaining("database " + reference.getCatalog());
        }
        assertThat(lane.opened.get(0).isClosed()).isTrue();
    }

    @Test
    void unreachableAtStartFallsBackWithOneWarn() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> true);
        var connections = new HeartbeatConnections(app, lane);

        try (var log = new CapturedStderr()) {
            try (var reference = MAIN.getConnection()) {
                connections.start(reference, TABLE);
            }
            for (int i = 0; i < 2; i++) {
                try (var c = connections.newConnection()) {
                    assertThat(c).isSameAs(app.opened.get(i));
                }
            }

            assertThat(app.calls).isEqualTo(2);
            assertThat(log.text()).containsOnlyOnce("unreachable at start")
                    .containsOnlyOnce("this beat uses the application's connections");
        }
    }

    @Test
    void deferredCheckMeasuresBothSidesAtOnce() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(0));
        }
        assertThat(app.calls).isEqualTo(1);
        assertThat(app.opened.get(0).isClosed()).as("the comparison's connection").isTrue();
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
        assertThat(app.calls).isEqualTo(1);
    }

    @Test
    void deferredMismatchTurnsTheLaneOff() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", other(), n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        try (var log = new CapturedStderr()) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(app.opened.get(0));
            }
            assertThat(log.text()).containsOnlyOnce("reaches another database");
        }
        assertThat(lane.opened.get(0).isClosed()).isTrue();
        for (int i = 1; i < 3; i++) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(app.opened.get(i));
            }
        }
        assertThat(lane.calls).isEqualTo(2);
    }

    @Test
    void deferredCheckWithoutAnAppConnectionFailsTheBeat() throws Exception {
        var app = new Recorded("app", MAIN, n -> n == 1);
        var lane = new Recorded("lane", MAIN, n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        assertThatThrownBy(connections::newConnection).isInstanceOf(SQLException.class).hasMessage("app refused");
        assertThat(lane.opened.get(0).isClosed()).isTrue();
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
        assertThat(app.opened.get(0).isClosed()).isTrue();
    }

    /** A pooler connects at once and waits on the first query: a check that gets no answer does not fail the start. */
    @Test
    void checkQueryFailureAtStartDoesNotFailIt() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> false, n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);

        try (var log = new CapturedStderr()) {
            try (var reference = MAIN.getConnection()) {
                connections.start(reference, TABLE);
            }
            assertThat(log.text()).containsOnlyOnce("unreachable at start")
                    .doesNotContain("this beat uses the application's connections");
        }
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
        assertThat(app.calls).as("the deferred check's").isEqualTo(1);
    }

    @Test
    void checkQueryFailureLaterFallsBackForThatBeat() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> n == 1, n -> n == 2);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        try (var log = new CapturedStderr()) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(app.opened.get(0)).isNotSameAs(lane.opened.get(0));
            }
            assertThat(log.text()).containsOnlyOnce("this beat uses the application's connections");
        }
        try (var c = connections.newConnection()) {
            assertThat(c).as("checked again").isSameAs(lane.opened.get(1));
        }
        assertThat(app.calls).isEqualTo(2);
    }

    /** Another instance's first bootstrap may create the table between the two measures: measured again, they match. */
    @Test
    void tableCreatedBetweenTheMeasuresAtStartIsNoMismatch() throws Exception {
        var table = "public." + TestSql.uniqueTable();
        var app = new Recorded("app", MAIN, n -> false);
        var connections = new HeartbeatConnections(app, creating(table));

        try (var reference = MAIN.getConnection()) {
            connections.start(reference, table);
        }
        try (var c = connections.newConnection()) {
            assertThat(app.calls).as("checked at start: no comparison later").isZero();
        }
    }

    @Test
    void tableCreatedBetweenTheDeferredMeasuresIsNoMismatch() throws Exception {
        var table = "public." + TestSql.uniqueTable();
        var app = new Recorded("app", MAIN, n -> false);
        var creating = creating(table);
        var calls = new AtomicInteger();
        var connections = new HeartbeatConnections(app, () -> {
            if (calls.incrementAndGet() == 1) {
                throw new SQLException("lane refused");
            }
            return creating.newConnection();
        });
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, table);
        }

        try (var log = new CapturedStderr()) {
            try (var c = connections.newConnection()) {
                assertThat(app.opened.get(0).isClosed()).as("the comparison's, closed: the lane serves").isTrue();
            }
            assertThat(log.text()).doesNotContain("reaches another database");
        }
        try (var c = connections.newConnection()) {
            assertThat(app.calls).as("checked").isEqualTo(1);
        }
    }

    /**
     * Connections of {@link #MAIN}; the first creates {@code table} right before its first statement, as another
     * instance's first bootstrap may between the application side's measure and the heartbeat side's.
     */
    private static ConnectionSource creating(String table) {
        var created = new AtomicBoolean();
        return () -> {
            var real = MAIN.getConnection();
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                        if (method.getName().equals("prepareStatement") && created.compareAndSet(false, true)) {
                            try (var c = MAIN.getConnection()) {
                                TestSql.execute(c, "create table " + table + " (id int)");
                            }
                        }
                        try {
                            return method.invoke(real, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        };
    }

    private static DataSource other() {
        var ds = (PGSimpleDataSource) TestDataSources.newDataSource();
        ds.setDatabaseName(OTHER);
        return ds;
    }

    /**
     * Counts its calls and keeps what it handed out; call {@code n} (from 1) throws when {@code fails} says so, and
     * hands out a connection already closed, whose every query fails, when {@code closed} does.
     */
    private static final class Recorded implements ConnectionSource {

        final List<Connection> opened = new ArrayList<>();
        int calls;
        private final String name;
        private final DataSource source;
        private final IntPredicate fails;
        private final IntPredicate closed;

        Recorded(String name, DataSource source, IntPredicate fails) {
            this(name, source, fails, n -> false);
        }

        Recorded(String name, DataSource source, IntPredicate fails, IntPredicate closed) {
            this.name = name;
            this.source = source;
            this.fails = fails;
            this.closed = closed;
        }

        @Override
        public Connection newConnection() throws SQLException {
            if (fails.test(++calls)) {
                throw new SQLException(name + " refused");
            }
            var c = source.getConnection();
            if (closed.test(calls)) {
                c.close();
            }
            opened.add(c);
            return c;
        }
    }

    @Test
    void aSourceInAnotherTimeZoneIsCheckedAtStartAsOneDatabase() throws Exception {
        var lane = new Recorded("lane", inZone("Europe/Moscow"), n -> false);
        var connections = new HeartbeatConnections(new Recorded("app", MAIN, n -> false), lane);

        try (var reference = inZone("UTC").getConnection()) {
            connections.start(reference, TABLE);
        }
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
    }

    @Test
    void aSourceInAnotherTimeZoneFirstReachedByABeatIsOneDatabase() throws Exception {
        var lane = new Recorded("lane", inZone("Europe/Moscow"), n -> n == 1);
        var app = new Recorded("app", inZone("UTC"), n -> false);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        try (var log = new CapturedStderr()) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(lane.opened.get(0));
            }
            assertThat(log.text()).doesNotContain("reaches another database");
        }
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
    }

    @Test
    void anotherDatabaseIsRefusedAcrossTimeZonesToo() throws Exception {
        var other = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                var c = super.getConnection();
                TestSql.execute(c, "set time zone 'Europe/Moscow'");
                return c;
            }
        };
        other.setUrl(TestDataSources.jdbcUrl());
        other.setUser(TestDataSources.user());
        other.setPassword(TestDataSources.password());
        other.setDatabaseName(OTHER);
        var connections = new HeartbeatConnections(new Recorded("app", MAIN, n -> false),
                new Recorded("lane", other, n -> false));

        try (var reference = inZone("UTC").getConnection()) {
            assertThatThrownBy(() -> connections.start(reference, TABLE)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reaches another database than the application's");
        }
    }

    /** Connections of the main database whose sessions run in {@code zone}. */
    private static DataSource inZone(String zone) {
        var ds = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                var c = super.getConnection();
                TestSql.execute(c, "set time zone '" + zone + "'");
                return c;
            }
        };
        ds.setUrl(TestDataSources.jdbcUrl());
        ds.setUser(TestDataSources.user());
        ds.setPassword(TestDataSources.password());
        return ds;
    }
}
