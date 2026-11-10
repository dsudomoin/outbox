package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
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

    /** While {@code on}, a statement prepared on a thread whose name starts with one of the prefixes throws OOM. */
    private static final class Failing implements DataSource {

        volatile boolean on;
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
                .build(); var err = new CapturedStderr()) {
            runtime.start();
            var before = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("before"))));
            await(() -> delivered.contains(before));

            failing.on = true;
            var during = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("during"))));
            await(() -> failing.threads.containsAll(Set.of("outbox-relay", "outbox-maintenance"))
                    && failing.thrown.get() > 10);
            Thread.sleep(1000); // several fatal errors in a row on each thread
            assertThat(delivered).doesNotContain(during);
            failing.on = false;

            var after = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("after"))));
            await(() -> delivered.containsAll(Set.of(during, after)));
            assertThat(Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
                    .filter(n -> n.startsWith("outbox-relay-")).count()).as("both workers alive").isEqualTo(2);
            await(() -> runtime.admin().status().maintenanceSinceSuccessSeconds() < 1); // maintenance runs again
            assertThat(err.text()).contains("hit a fatal error, going on in", "outbox maintenance failed; next run in")
                    .doesNotContain("stopped by a fatal error");
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!check.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("not met within 30 s");
            }
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }
}
