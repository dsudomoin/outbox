package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * The relay's database stops answering: the network goes silent, connections staying open and nothing arriving, or a
 * lock holds its reads. With timeouts on the DataSource every wait ends in a failure the relay survives; without them,
 * and under the lock, the shutdown stays bounded all the same.
 */
class NetworkBlackholeIT {

    private static final DataSource DIRECT = TestDataSources.newDataSource();

    private final Blackhole hole = new Blackhole();
    private final List<AutoCloseable> closing = new ArrayList<>();

    NetworkBlackholeIT() throws IOException {}

    @AfterEach
    void release() throws Exception {
        hole.open();
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
        hole.close();
    }

    @Test
    void withTimeoutsTheRelayFailsWithABoundAndLosesNothing() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(500));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var failures = new AtomicInteger();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(throughTheHole(3)),
                record -> delivered.add(record.messageId())).telemetry(new OutboxTelemetry() {
                    @Override
                    public void relayFailed(Throwable error) {
                        failures.incrementAndGet();
                    }
                }).build();
        closing.add(relay);
        relay.start();
        var writerTransactions = new JdbcTransactions(DIRECT);
        var writer = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()).relayEnabled(false)
                .maintenanceInterval(Duration.ofMillis(500)), writerTransactions, record -> {}).build();
        closing.add(writer);
        writer.start();
        var sent = new HashSet<UUID>();
        sent.add(send(writer, writerTransactions, "before"));
        await(() -> delivered.containsAll(sent));

        hole.shut();
        sent.add(send(writer, writerTransactions, "during"));
        int failed = failures.get();
        await(() -> failures.get() > failed);
        assertThat(delivered.containsAll(sent)).as("what was sent during it waits").isFalse();

        hole.open();
        sent.add(send(writer, writerTransactions, "after"));
        await(() -> delivered.containsAll(sent));
        assertThat(relay.relay().running()).isTrue();
        var names = relay.schema().names();
        long seen = lastSeen(names.instance(), relay);
        await(() -> lastSeen(names.instance(), relay) > seen);
    }

    @Test
    void aLockHeldOnTheCursorsHoldsTheRelayOnlyWhileItLasts() throws Exception {
        var config = new TestOutboxConfig();
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(DIRECT);
        var runtime = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(runtime);
        runtime.start();
        var sent = new HashSet<UUID>();
        sent.add(send(runtime, transactions, "before"));
        await(() -> delivered.containsAll(sent));

        try (var locker = DIRECT.getConnection()) {
            locker.setAutoCommit(false);
            TestSql.execute(locker, "lock table " + runtime.schema().names().cursor() + " in access exclusive mode");
            sent.add(send(runtime, transactions, "during"));
            Thread.sleep(1000);
            assertThat(delivered.containsAll(sent)).as("what was sent during it waits").isFalse();
            locker.rollback();
        }

        await(() -> delivered.containsAll(sent));
        assertThat(runtime.relay().running()).isTrue();
    }

    @Test
    void aLockHeldOnTheCursorsDoesNotHoldTheClose() throws Exception {
        var config = new TestOutboxConfig().shutdownTimeout(Duration.ofMillis(100));
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {}).build();
        runtime.start();

        try (var locker = DIRECT.getConnection()) {
            locker.setAutoCommit(false);
            TestSql.execute(locker, "lock table " + runtime.schema().names().cursor() + " in access exclusive mode");
            Thread.sleep(500); // the workers wait on the lock by now

            CompletableFuture.runAsync(runtime::close).get(60, TimeUnit.SECONDS);
            locker.rollback();
        }
    }

    @Test
    void withoutTimeoutsTheCloseStaysBounded() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(200))
                .idlePollInterval(Duration.ofMillis(20)).shutdownTimeout(Duration.ofMillis(100));
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(throughTheHole(0)), record -> {}).build();
        runtime.start();

        hole.shut();
        Thread.sleep(500); // the workers and the next beat are stuck on the silent sockets by now

        CompletableFuture.runAsync(runtime::close).get(60, TimeUnit.SECONDS);
    }

    @Test
    void aSilentDropHealsByItselfOnTheDefaultDataSource() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 0))),
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        var writerTransactions = new JdbcTransactions(DIRECT);
        var writer = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()).relayEnabled(false),
                writerTransactions, record -> {}).build();
        closing.add(writer);
        writer.start();
        var sent = new HashSet<UUID>();
        sent.add(send(writer, writerTransactions, "before"));
        await(() -> delivered.containsAll(sent));

        hole.forget(); // each worker's next statement, on its pooled connection, waits on a silent socket
        sent.add(send(writer, writerTransactions, "after"));

        await(() -> delivered.containsAll(sent));
        assertThat(relay.relay().running()).isTrue();
    }

    @Test
    void aSocketTimeoutOfTheDataSourceWins() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofHours(1));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 1))),
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        var writerTransactions = new JdbcTransactions(DIRECT);
        var writer = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()).relayEnabled(false),
                writerTransactions, record -> {}).build();
        closing.add(writer);
        writer.start();

        hole.forget();
        var after = send(writer, writerTransactions, "after");

        await(() -> delivered.contains(after)); // within the await, so by the 1 s of the DataSource, not by the hour
    }

    @Test
    void onlyRelayWorkersGetTheNetworkTimeout() throws Exception {
        var calls = new ConcurrentLinkedQueue<String>();
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(9))
                .maintenanceInterval(Duration.ofMillis(100));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(recordingTimeouts(DIRECT, calls));
        var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();

        var sent = send(relay, transactions, "k");
        await(() -> delivered.contains(sent));
        Thread.sleep(500); // some maintenance runs and beats

        assertThat(calls).isNotEmpty().allMatch(call -> call.startsWith("outbox-relay-") && call.endsWith(":9000"));
    }

    @Test
    void aZeroNetworkTimeoutSetsNone() throws Exception {
        var calls = new ConcurrentLinkedQueue<String>();
        var config = new TestOutboxConfig().networkTimeout(Duration.ZERO);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(recordingTimeouts(DIRECT, calls));
        var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();

        var sent = send(relay, transactions, "k");
        await(() -> delivered.contains(sent));

        assertThat(calls).isEmpty();
    }

    @Test
    void aPoolGivesItsNextBorrowerTheTimeoutOfTheConnection() throws Exception {
        var config = new HikariConfig();
        config.setDataSource(DIRECT);
        config.setMaximumPoolSize(1);
        try (var pool = new HikariDataSource(config)) {
            try (var c = pool.getConnection()) {
                assertThat(c.getNetworkTimeout()).isZero();
                c.setNetworkTimeout(Runnable::run, 1000);
                assertThat(c.getNetworkTimeout()).isEqualTo(1000);
            }
            try (var c = pool.getConnection()) {
                assertThat(c.getNetworkTimeout()).as("not left to the next borrower").isZero();
            }
        }
    }

    private static UUID send(OutboxRuntime runtime, JdbcTransactions transactions, String key) {
        return transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", key, "p".getBytes(UTF_8))));
    }

    private static long lastSeen(String instances, OutboxRuntime runtime) throws Exception {
        try (var c = DIRECT.getConnection()) {
            return TestSql.queryLong(c, "select (extract(epoch from last_seen) * 1000000)::bigint from " + instances
                    + " where instance_id = '" + runtime.schema().instanceId() + "'");
        }
    }

    /** Connections through {@link #hole}; {@code seconds}: connect, login and socket timeouts, 0 for none. */
    private DataSource throughTheHole(int seconds) {
        return throughTheHole(seconds, seconds);
    }

    /** {@code connectSeconds}: connect and login timeouts; {@code socketSeconds}: socket timeout; 0 for none. */
    private DataSource throughTheHole(int connectSeconds, int socketSeconds) {
        var ds = new PGSimpleDataSource();
        ds.setUrl("jdbc:postgresql://127.0.0.1:" + hole.port() + "/" + TARGET.getPath().substring(1));
        ds.setUser(TestDataSources.user());
        ds.setPassword(TestDataSources.password());
        ds.setConnectTimeout(connectSeconds);
        ds.setLoginTimeout(connectSeconds);
        ds.setSocketTimeout(socketSeconds);
        return ds;
    }

    /** {@code source} behind a pool, so a worker keeps its connection from pass to pass, as in a service. */
    private DataSource pool(DataSource source) {
        var config = new HikariConfig();
        config.setDataSource(source);
        config.setMaximumPoolSize(8);
        var pool = new HikariDataSource(config);
        closing.add(pool);
        return pool;
    }

    /** {@code source} whose connections add to {@code calls}, per {@code setNetworkTimeout}: thread name:milliseconds. */
    private static DataSource recordingTimeouts(DataSource source, Queue<String> calls) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (ds, method, args) -> {
                    var result = invoke(source, method, args);
                    return !method.getName().equals("getConnection") ? result : Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (c, call, values) -> {
                                if (call.getName().equals("setNetworkTimeout")) {
                                    calls.add(Thread.currentThread().getName() + ":" + values[1]);
                                }
                                return invoke(result, call, values);
                            });
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void await(Check condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 30 s");
            }
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }

    private static final URI TARGET = URI.create(TestDataSources.jdbcUrl().substring("jdbc:".length()));

    /**
     * A TCP forwarder to the test's Postgres that, shut, keeps every connection open and forwards nothing; a connection
     * it forgot stays so for good, as a firewall that lost track of it would keep it, while new ones go through.
     */
    private static final class Blackhole implements AutoCloseable {

        private final ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final Set<Socket> forgotten = ConcurrentHashMap.newKeySet();
        private volatile boolean shut;

        Blackhole() throws IOException {
            daemon(this::accept);
        }

        int port() {
            return server.getLocalPort();
        }

        void shut() {
            shut = true;
        }

        void open() {
            shut = false;
        }

        /** Every connection stays silent for good, and the hole lets new ones through. */
        void forget() {
            forgotten.addAll(sockets);
            shut = false;
        }

        @Override
        public void close() throws IOException {
            server.close();
            for (var socket : sockets) {
                socket.close();
            }
        }

        private void accept() {
            try {
                while (true) {
                    var client = server.accept();
                    var target = new Socket(TARGET.getHost(), TARGET.getPort());
                    sockets.add(client);
                    sockets.add(target);
                    daemon(() -> pump(client, target));
                    daemon(() -> pump(target, client));
                }
            } catch (IOException e) { // closed
            }
        }

        private void pump(Socket from, Socket to) {
            var buffer = new byte[8192];
            try (from; to) {
                var in = from.getInputStream();
                var out = to.getOutputStream();
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    while (shut || forgotten.contains(from)) {
                        Thread.sleep(10);
                    }
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException | InterruptedException e) { // one side went away, and the other goes with it
            }
        }

        private static void daemon(Runnable work) {
            var thread = new Thread(work, "test-blackhole");
            thread.setDaemon(true);
            thread.start();
        }
    }
}
