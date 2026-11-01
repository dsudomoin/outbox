package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A target that hangs past {@code batchTimeout} and ignores the interrupt, as a JDBC or HTTP call may, holds one handler
 * thread per call: the relay stays up and its heartbeat live, and the gauge shows it. Another target stays out of it
 * only on an outbox of its own.
 */
class HungTargetIT {

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final CountDownLatch recovered = new CountDownLatch(1);
    private final List<OutboxRuntime> runtimes = new ArrayList<>();

    @AfterEach
    void release() {
        recovered.countDown();
        runtimes.forEach(OutboxRuntime::close);
    }

    @Test
    void hungCallsTakeEveryHandlerThreadWhileTheInstanceLooksAlive() throws Exception {
        var free = new ConcurrentHashMap<OutboxGauge, DoubleSupplier>();
        var runtime = runtime(hanging(ConcurrentHashMap.newKeySet()), free);
        send(runtime, "a", 8);

        await(() -> free.get(OutboxGauge.HANDLER_THREADS_FREE).getAsDouble() == 0);

        assertThat(runtime.relay().running()).isTrue();
        assertThat(runtime.admin().status().instances()).singleElement()
                .satisfies(i -> assertThat(i.live()).as("the heartbeat says the process runs, no more").isTrue());
    }

    @Test
    void aHungTargetDoesNotStopAnotherOnItsOwnOutboxAndLosesNothing() throws Exception {
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var free = new ConcurrentHashMap<OutboxGauge, DoubleSupplier>();
        var hung = runtime(hanging(delivered), free);
        var sent = send(hung, "a", 8);
        await(() -> free.get(OutboxGauge.HANDLER_THREADS_FREE).getAsDouble() == 0);

        Set<UUID> healthy = ConcurrentHashMap.newKeySet();
        var other = runtime(record -> healthy.add(record.messageId()), new ConcurrentHashMap<>());
        var sentThere = send(other, "b", 8);
        await(() -> healthy.containsAll(sentThere));

        assertThat(free.get(OutboxGauge.HANDLER_THREADS_FREE).getAsDouble()).as("still hung, and bounded").isZero();
        recovered.countDown();
        await(() -> delivered.containsAll(sent));
    }

    private OutboxRuntime runtime(OutboxHandler handler, Map<OutboxGauge, DoubleSupplier> gauges) {
        var config = new TestOutboxConfig().shards(8).workers(1).batchTimeout(Duration.ofMillis(200))
                .retry(Duration.ofMillis(50), Duration.ofMillis(200), 0).shutdownTimeout(Duration.ofMillis(100));
        var runtime = OutboxRuntime.builder(config, transactions, handler).telemetry(new OutboxTelemetry() {
            @Override
            public void gauge(OutboxGauge gauge, DoubleSupplier value) {
                gauges.put(gauge, value);
            }
        }).build();
        runtimes.add(runtime);
        runtime.start();
        return runtime;
    }

    /** Hangs until {@link #recovered}, whatever interrupts it, then delivers. */
    private OutboxHandler hanging(Set<UUID> delivered) {
        return record -> {
            boolean interrupted = false;
            while (recovered.getCount() > 0) {
                try {
                    recovered.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            delivered.add(record.messageId());
        };
    }

    private Set<UUID> send(OutboxRuntime runtime, String topic, int messages) {
        var ids = new HashSet<UUID>();
        for (int i = 0; i < messages; i++) {
            var key = "k" + i;
            ids.add(transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, key, "p".getBytes(UTF_8)))));
        }
        return ids;
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
}
