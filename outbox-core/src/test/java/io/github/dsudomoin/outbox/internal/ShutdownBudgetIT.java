package io.github.dsudomoin.outbox.internal;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Nothing that stands in the way of the instance's row holds the shutdown past {@link Heartbeat#STOP_BUDGET}: a lock on
 * the row, a connection source that hands out nothing. A row left behind stops counting by its {@code expires_at}.
 */
class ShutdownBudgetIT {

    private static final Duration INTERVAL = Duration.ofMillis(200);
    /** A guard against a hang alone, what a weak runner reaches too: {@link #aLockedRowCostsTheBudgetAtMost} is tight. */
    private static final Duration BOUND = Duration.ofSeconds(60);
    private static final DataSource DIRECT = TestDataSources.newDataSource();

    /** Sources that hang wait on it; released after every test. */
    private final CountDownLatch released = new CountDownLatch(1);

    @AfterEach
    void release() {
        released.countDown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aLockedRowDoesNotHoldTheClose(boolean relay) throws Exception {
        var runtime = runtime(relay, new JdbcTransactions(DIRECT), null);
        runtime.start();
        var names = runtime.schema().names();

        try (var holder = lockRow(names)) {
            closeWithinTheBound(runtime::close);
            Thread.sleep(1000); // the delete's lock_timeout is out by now: the release below hands it no lock
            holder.rollback();
        }

        assertThat(count(names, "true")).as("left to expire").isOne();
        var seen = lastSeen(names);
        Thread.sleep(INTERVAL.multipliedBy(OutboxSchema.LIVE_INTERVALS).toMillis());
        assertThat(lastSeen(names)).as("no beat after the stop").isEqualTo(seen);
        assertThat(count(names, "expires_at > now()")).as("no longer live").isZero();
    }

    @Test
    @Tag("timing")
    void aLockedRowCostsTheBudgetAtMost() throws Exception {
        var runtime = runtime(false, new JdbcTransactions(DIRECT), null);
        runtime.start();

        try (var holder = lockRow(runtime.schema().names())) {
            long closing = System.nanoTime();
            closeWithinTheBound(runtime::close);
            assertThat(Duration.ofNanos(System.nanoTime() - closing)).isLessThan(Heartbeat.STOP_BUDGET.plusSeconds(1));
            holder.rollback();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void anUnlockedRowIsDeletedOnce(boolean relay) throws Exception {
        var deletes = new AtomicInteger();
        var beats = new AtomicInteger();
        var config = new TestOutboxConfig().relayEnabled(relay).maintenanceInterval(Duration.ofHours(1))
                .shutdownTimeout(Duration.ofMillis(100));
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(counting(deletes, beats)),
                new RecordingHandler()).build();
        runtime.start();
        var names = runtime.schema().names();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (beats.get() == 0 && System.nanoTime() < deadline) { // with no beat in flight: one that commits after
            Thread.sleep(5); // the stop deletes the row again, on purpose
        }
        assertThat(beats).as("the first beat ended").hasPositiveValue();

        closeWithinTheBound(runtime::close);

        assertThat(count(names, "true")).isZero();
        assertThat(deletes).as("one delete, one budget").hasValue(1);
    }

    @Test
    void aHangingHeartbeatSourceDoesNotHoldTheClose() throws Exception {
        var hanging = new CountDownLatch(1);
        ConnectionSource lane = () -> {
            if (hanging.getCount() == 0) {
                await(released);
            }
            return DIRECT.getConnection();
        };
        var runtime = runtime(false, new JdbcTransactions(DIRECT), lane);
        runtime.start();
        hanging.countDown();

        closeWithinTheBound(runtime::close); // the delete given up on may still end on the application's connections
    }

    @Test
    void aHangingApplicationSourceDoesNotHoldTheClose() throws Exception {
        var hanging = new CountDownLatch(1);
        var source = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                if (hanging.getCount() == 0) {
                    await(released);
                }
                return super.getConnection();
            }
        };
        source.setUrl(TestDataSources.jdbcUrl());
        source.setUser(TestDataSources.user());
        source.setPassword(TestDataSources.password());
        var runtime = runtime(false, new JdbcTransactions(source), null);
        runtime.start();
        hanging.countDown();

        closeWithinTheBound(runtime::close);
    }

    @Test
    void theSchemaAloneIsBoundedToo() throws Exception {
        var schema = new OutboxSchema(new JdbcTransactions(DIRECT), new TestOutboxConfig().relayEnabled(false));
        schema.start();

        try (var holder = lockRow(schema.names())) {
            closeWithinTheBound(schema::close);
            holder.rollback();
        }
    }

    private OutboxRuntime runtime(boolean relay, JdbcTransactions transactions, ConnectionSource lane) {
        var config = new TestOutboxConfig().relayEnabled(relay).maintenanceInterval(INTERVAL)
                .shutdownTimeout(Duration.ofMillis(100));
        var builder = OutboxRuntime.builder(config, transactions, new RecordingHandler());
        if (lane != null) {
            builder.heartbeatConnections(lane);
        }
        return builder.build();
    }

    /** A transaction of its own holding the instance's row, as an operator's {@code select for update} would. */
    private static Connection lockRow(TableNames names) throws SQLException {
        var holder = DIRECT.getConnection();
        holder.setAutoCommit(false);
        TestSql.execute(holder, "select 1 from " + names.instance() + " for update");
        return holder;
    }

    private static void closeWithinTheBound(Runnable close) throws Exception {
        var closing = new CompletableFuture<Void>();
        var thread = new Thread(() -> {
            try {
                close.run();
                closing.complete(null);
            } catch (Throwable t) {
                closing.completeExceptionally(t);
            }
        }, "test-close");
        thread.setDaemon(true);
        thread.start();
        closing.get(BOUND.toNanos(), NANOSECONDS);
    }

    private static long count(TableNames names, String where) throws SQLException {
        try (var c = DIRECT.getConnection()) {
            return TestSql.queryLong(c, "select count(*) from " + names.instance() + " where " + where);
        }
    }

    private static long lastSeen(TableNames names) throws SQLException {
        try (var c = DIRECT.getConnection()) {
            return TestSql.queryLong(c, "select (extract(epoch from last_seen) * 1000000)::bigint from "
                    + names.instance());
        }
    }

    private static void await(CountDownLatch latch) throws SQLException {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted waiting for a connection", e);
        }
    }

    /** {@link #DIRECT}, counting the statements that delete an instance's row and the beats' connections closed. */
    private static DataSource counting(AtomicInteger deletes, AtomicInteger beats) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    var result = invoke(method, DIRECT, args);
                    return result instanceof Connection c ? counting(c, deletes, beats) : result;
                });
    }

    private static Connection counting(Connection c, AtomicInteger deletes, AtomicInteger beats) {
        var beat = Thread.currentThread().getName().equals("outbox-heartbeat");
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("prepareStatement") && ((String) args[0]).startsWith("delete from ")
                            && ((String) args[0]).contains("_instance where instance_id")) {
                        deletes.incrementAndGet();
                    }
                    var result = invoke(method, c, args);
                    if (beat && method.getName().equals("close")) {
                        beats.incrementAndGet();
                    }
                    return result;
                });
    }

    private static Object invoke(java.lang.reflect.Method method, Object on, Object[] args) throws Throwable {
        try {
            return method.invoke(on, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
