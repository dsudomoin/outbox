package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * An OutOfMemoryError on the relay's own threads, as a backlog of large messages or the rest of the service may cause:
 * each thread reports it and goes on after a pause, none ends for good.
 */
class FatalErrorIT {

    private static final DataSource DIRECT = TestDataSources.newDataSource();

    /**
     * While {@code on}, a statement prepared on a thread whose name starts with one of the prefixes throws OOM, or with
     * {@code ordinary} an SQLException that loses no connection.
     */
    private static final class Failing implements DataSource {

        volatile boolean on;
        volatile boolean ordinary;
        final AtomicInteger thrown = new AtomicInteger();
        final Set<String> threads = ConcurrentHashMap.newKeySet();

        @Override
        public Connection getConnection() throws SQLException {
            var c = DIRECT.getConnection();
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                        var name = Thread.currentThread().getName();
                        if (on && method.getName().startsWith("prepare")
                                && (name.startsWith("outbox-relay-") || name.startsWith("outbox-maintenance")
                                        || name.startsWith("outbox-dead-letters"))) {
                            thrown.incrementAndGet();
                            threads.add(name.replaceAll("-\\d+$", ""));
                            if (ordinary) {
                                throw new SQLException("statement failed (test)");
                            }
                            throw new OutOfMemoryError("Java heap space (test)");
                        }
                        try {
                            return method.invoke(c, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        @Override
        public Connection getConnection(String user, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {}

        @Override
        public void setLoginTimeout(int seconds) {}

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not a wrapper");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }

    @Test
    void theRelaysThreadsOutliveAnOutOfMemoryError() throws Exception {
        var failing = new Failing();
        var transactions = new JdbcTransactions(failing);
        var config = new TestOutboxConfig().workers(2).maintenanceInterval(Duration.ofMillis(200))
                .retry(Duration.ofMillis(50), Duration.ofMillis(400), 0);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        try (var runtime = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId()))
                .deadLetterHandler(FatalErrorIT::keepAll).build(); var err = new CapturedStderr()) {
            runtime.start();
            var before = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("before"))));
            await(() -> delivered.contains(before));

            failing.on = true;
            var during = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("during"))));
            await(() -> failing.threads.containsAll(Set.of("outbox-relay", "outbox-maintenance", "outbox-dead-letters"))
                    && failing.thrown.get() > 10);
            Thread.sleep(1000); // several fatal errors in a row on each thread
            assertThat(delivered).doesNotContain(during);
            failing.on = false;

            var after = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("after"))));
            await(() -> delivered.containsAll(Set.of(during, after)));
            assertThat(workersAlive()).as("both workers alive").isEqualTo(2);
            await(() -> runtime.admin().status().maintenanceSinceSuccessSeconds() < 1); // maintenance runs again
            assertThat(err.text()).contains("hit a fatal error, going on in", "outbox maintenance failed; next run in",
                            "outbox dead-letter pass hit a fatal error, retrying in")
                    .doesNotContain("stopped by a fatal error");
        }
    }

    /**
     * A shard whose step fails with a fatal error after its handler call every time (the error its handler threw is
     * too large to describe) is stepped alone from then on: the shards it was grouped with commit, their records are
     * not delivered again on every tick.
     */
    @Test
    void aShardFailingFatallyHoldsUpNoNeighbour() throws Exception {
        var transactions = new JdbcTransactions(DIRECT);
        var config = new TestOutboxConfig().shards(4).workers(1)
                .retry(Duration.ofMillis(50), Duration.ofMillis(400), 0);
        var keys = keysOnEachShard(4);
        var poison = keys.get(0);
        var thrown = new AtomicInteger();
        Map<UUID, AtomicInteger> calls = new ConcurrentHashMap<>();
        OutboxHandler handler = record -> {
            if (record.keyAsString().equals(poison) && thrown.get() >= 0) {
                thrown.incrementAndGet();
                throw new RuntimeException() {
                    @Override
                    public String toString() { // as a message of hundreds of MB would
                        throw new OutOfMemoryError("Java heap space (test)");
                    }
                };
            }
            calls.computeIfAbsent(record.messageId(), id -> new AtomicInteger()).incrementAndGet();
        };
        try (var runtime = OutboxRuntime.builder(config, transactions, handler).build()) {
            runtime.start();
            var sent = transactions.inTx(c -> {
                var ids = new ArrayList<UUID>();
                ids.add(runtime.outbox().send(OutboxMessage.of("t", poison, bytes("poison"))));
                for (var key : keys.subList(1, keys.size())) {
                    ids.add(runtime.outbox().send(OutboxMessage.of("t", key, bytes("neighbour"))));
                }
                return ids;
            });
            var neighbours = sent.subList(1, sent.size());
            await("the neighbours delivered", () -> thrown.get() >= 5 && calls.keySet().containsAll(neighbours));
            await(() -> thrown.get() >= 8); // more ticks, each one more step of the failing shard
            for (var id : neighbours) {
                // once, or twice when the group's failed step had handed it the record before the poison's error
                assertThat(calls.get(id).get()).as("calls of a neighbour's record").isBetween(1, 2);
            }
            thrown.set(Integer.MIN_VALUE);
            await(() -> calls.containsKey(sent.get(0)));
        }
    }

    /**
     * A fatal error raised while an ordinary failure is reported (a telemetry hook or the logger out of memory) pauses
     * the thread as any other fatal error does, the workers and the dead-letter thread alike.
     */
    @Test
    void aFatalErrorReportingAnOrdinaryFailureEndsNoThread() throws Exception {
        var failing = new Failing();
        failing.ordinary = true;
        var transactions = new JdbcTransactions(failing);
        var config = new TestOutboxConfig().workers(2).retry(Duration.ofMillis(50), Duration.ofMillis(400), 0);
        var hookThrown = new AtomicInteger();
        var logThrown = new AtomicInteger();
        var telemetry = new OutboxTelemetry() {
            @Override
            public void relayFailed(Throwable error) {
                if (failing.on) {
                    hookThrown.incrementAndGet();
                    throw new OutOfMemoryError("Java heap space (test)");
                }
            }
        };
        var original = System.err;
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                write(new byte[] {(byte) b}, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                if (failing.on && Thread.currentThread().getName().equals("outbox-dead-letters")) {
                    logThrown.incrementAndGet();
                    throw new OutOfMemoryError("Java heap space (test)");
                }
                original.write(b, off, len);
            }
        }, true, UTF_8));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        try (var runtime = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId()))
                .deadLetterHandler(FatalErrorIT::keepAll).telemetry(telemetry).build()) {
            runtime.start();
            failing.on = true;
            await("the workers going on after relayFailed threw", () -> hookThrown.get() >= 4);
            await("the dead-letter pass going on after its log threw", () -> logThrown.get() >= 2);
            failing.on = false;

            var after = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("after"))));
            await(() -> delivered.contains(after));
            assertThat(workersAlive()).as("both workers alive").isEqualTo(2);
            assertThat(threadAlive("outbox-dead-letters")).as("the dead-letter thread alive").isTrue();
        } finally {
            System.setErr(original);
        }
    }

    private static List<DeadLetterDecision> keepAll(List<DeadLetter> letters) {
        return letters.stream().map(l -> DeadLetterDecision.keep()).toList();
    }

    /** Topic "t" keys, one on each of {@code shards} shards. */
    private static List<String> keysOnEachShard(int shards) {
        var keys = new String[shards];
        for (int i = 0; Arrays.asList(keys).contains(null); i++) {
            var key = "k" + i;
            int shard = Hashing.shard(Hashing.keyHash("t", bytes(key)), shards);
            if (keys[shard] == null) {
                keys[shard] = key;
            }
        }
        return List.of(keys);
    }

    private static long workersAlive() {
        return Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
                .filter(n -> n.startsWith("outbox-relay-")).count();
    }

    private static boolean threadAlive(String name) {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(name));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(Check check) throws Exception {
        await("condition", check);
    }

    private static void await(String what, Check check) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!check.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(what + " not met within 30 s");
            }
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }
}
