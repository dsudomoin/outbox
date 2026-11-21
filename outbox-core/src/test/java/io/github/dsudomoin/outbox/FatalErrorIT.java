package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.Intercepting.intercepting;
import static io.github.dsudomoin.outbox.testing.Utf8.bytes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.Await;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.ShardKeys;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * An OutOfMemoryError on the relay's own threads, as a backlog of large messages or the rest of the service may cause:
 * each thread reports it and goes on after a pause, none ends for good.
 */
class FatalErrorIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final DataSource DIRECT = TestDataSources.newDataSource();

    /**
     * While {@code on}, a statement prepared on a thread whose name starts with one of the prefixes throws OOM, or with
     * {@code ordinary} an SQLException that loses no connection.
     */
    private static final class Failing {

        volatile boolean on;
        volatile boolean ordinary;
        final AtomicInteger thrown = new AtomicInteger();
        final Set<String> threads = ConcurrentHashMap.newKeySet();
        final DataSource source = intercepting(DIRECT, (c, call, args) -> {
            var name = Thread.currentThread().getName();
            if (on && call.getName().startsWith("prepare") && (name.startsWith("outbox-relay-")
                    || name.startsWith("outbox-maintenance") || name.startsWith("outbox-dead-letters"))) {
                thrown.incrementAndGet();
                threads.add(name.replaceAll("-\\d+$", ""));
                if (ordinary) {
                    throw new SQLException("statement failed (test)");
                }
                throw new OutOfMemoryError("Java heap space (test)");
            }
        });
    }

    @Test
    void theRelaysThreadsOutliveAnOutOfMemoryError() throws Exception {
        var failing = new Failing();
        var transactions = new JdbcTransactions(failing.source);
        var config = new TestOutboxConfig().workers(2).maintenanceInterval(Duration.ofMillis(200))
                .retry(Duration.ofMillis(50), Duration.ofMillis(400), 0);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        try (var runtime = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId()))
                .deadLetterHandler(FatalErrorIT::keepAll).build(); var err = new CapturedStderr()) {
            runtime.start();
            var before = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("before"))));
            Await.until(TIMEOUT, () -> delivered.contains(before));

            failing.on = true;
            var during = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("during"))));
            Await.until(TIMEOUT, () -> failing.threads.containsAll(
                    Set.of("outbox-relay", "outbox-maintenance", "outbox-dead-letters")) && failing.thrown.get() > 10);
            Thread.sleep(1000); // several fatal errors in a row on each thread
            assertThat(delivered).doesNotContain(during);
            failing.on = false;

            var after = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("after"))));
            Await.until(TIMEOUT, () -> delivered.containsAll(Set.of(during, after)));
            assertThat(workersAlive()).as("both workers alive").isEqualTo(2);
            // maintenance runs again
            Await.until(TIMEOUT, () -> runtime.admin().status().maintenanceSinceSuccessSeconds() < 1);
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
        var keys = IntStream.range(0, 4).mapToObj(s -> ShardKeys.first("t", "k", s, 4)).toList();
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
            Await.until(TIMEOUT, "the neighbours delivered",
                    () -> thrown.get() >= 5 && calls.keySet().containsAll(neighbours));
            Await.until(TIMEOUT, () -> thrown.get() >= 8); // more ticks, each one more step of the failing shard
            for (var id : neighbours) {
                // once, or twice when the group's failed step had handed it the record before the poison's error
                assertThat(calls.get(id).get()).as("calls of a neighbour's record").isBetween(1, 2);
            }
            thrown.set(Integer.MIN_VALUE);
            Await.until(TIMEOUT, () -> calls.containsKey(sent.get(0)));
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
        var transactions = new JdbcTransactions(failing.source);
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
            Await.until(TIMEOUT, "the workers going on after relayFailed threw", () -> hookThrown.get() >= 4);
            Await.until(TIMEOUT, "the dead-letter pass going on after its log threw", () -> logThrown.get() >= 2);
            failing.on = false;

            var after = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "k", bytes("after"))));
            Await.until(TIMEOUT, () -> delivered.contains(after));
            assertThat(workersAlive()).as("both workers alive").isEqualTo(2);
            assertThat(threadAlive("outbox-dead-letters")).as("the dead-letter thread alive").isTrue();
        } finally {
            System.setErr(original);
        }
    }

    private static List<DeadLetterDecision> keepAll(List<DeadLetter> letters) {
        return letters.stream().map(l -> DeadLetterDecision.keep()).toList();
    }

    private static long workersAlive() {
        return Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
                .filter(n -> n.startsWith("outbox-relay-")).count();
    }

    private static boolean threadAlive(String name) {
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(name));
    }
}
