package io.github.dsudomoin.outbox.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class CooldownTest {

    private static final Duration LEASE = Duration.ofSeconds(20);

    private final TopicPartition p = new TopicPartition("t", 0);
    private final TopicPartition q = new TopicPartition("t", 1);
    /** On the {@link System#nanoTime} scale, which may overflow: 10 s before it does. */
    private final long t0 = Long.MAX_VALUE - s(10);

    private static long s(long seconds) {
        return Duration.ofSeconds(seconds).toNanos();
    }

    private static long ms(long millis) {
        return Duration.ofMillis(millis).toNanos();
    }

    @Test
    void doublesAndCaps() {
        var cooldown = new Cooldown<TopicPartition>(LEASE);
        assertThat(cooldown.coolingUntil(p, t0)).isNull();

        long now = t0;
        for (long expected : new long[] {1, 2, 4, 8, 16, 30, 30}) {
            assertThat(cooldown.failed(p, now)).isEqualTo(Duration.ofSeconds(expected));
            assertThat(cooldown.coolingUntil(p, now)).isEqualTo(Duration.ofSeconds(expected));
            assertThat(cooldown.coolingUntil(p, now + s(expected) - 1)).isEqualTo(Duration.ofNanos(1));
            assertThat(cooldown.coolingUntil(p, now + s(expected))).isNull(); // ends exactly at the instant, probe
            now += s(expected);
        }
        assertThat(cooldown.coolingUntil(q, t0)).isNull();
    }

    /** I2: an expired cooldown admits one probe; it holds the key for the lease, then the next caller probes. */
    @Test
    void expiryAdmitsOneProbeAtATime() {
        var cooldown = new Cooldown<TopicPartition>(LEASE);
        cooldown.failed(p, t0);
        long expired = t0 + s(1);

        assertThat(cooldown.coolingUntil(p, expired)).as("the probe").isNull();
        assertThat(cooldown.coolingUntil(p, expired + ms(500))).as("everyone else, early: back in 1 s")
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(cooldown.coolingUntil(p, expired + s(8))).as("8 s into the lease: back in 8 s")
                .isEqualTo(Duration.ofSeconds(8));
        assertThat(cooldown.coolingUntil(p, expired + s(15))).as("never past the lease")
                .isEqualTo(Duration.ofSeconds(5));

        long failedAt = expired + s(3);
        cooldown.failed(p, failedAt);
        assertThat(cooldown.coolingUntil(p, failedAt)).as("the probe failed: doubled").isEqualTo(Duration.ofSeconds(2));

        long second = failedAt + s(2);
        assertThat(cooldown.coolingUntil(p, second)).as("the next probe").isNull();
        assertThat(cooldown.coolingUntil(p, second + LEASE.toNanos())).as("a probe that never reported hands over")
                .isNull();
        assertThat(cooldown.coolingUntil(p, second + LEASE.toNanos() + ms(1))).isEqualTo(Duration.ofSeconds(1));

        cooldown.succeeded(p);
        assertThat(cooldown.contains(p)).isFalse();
        assertThat(cooldown.coolingUntil(p, second)).isNull();
    }

    @Test
    void concurrentCallersAdmitOneProbe() throws Exception {
        var cooldown = new Cooldown<TopicPartition>(LEASE);
        cooldown.failed(p, t0);
        long expired = t0 + s(1);
        int callers = 16;
        var start = new CyclicBarrier(callers);
        var pool = Executors.newFixedThreadPool(callers);
        try {
            Callable<Optional<Duration>> lookup = () -> {
                start.await();
                return Optional.ofNullable(cooldown.coolingUntil(p, expired));
            };
            var results = pool.invokeAll(Collections.nCopies(callers, lookup));
            int probes = 0;
            for (var r : results) {
                probes += r.get().isEmpty() ? 1 : 0;
            }
            assertThat(probes).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void successResets() {
        var cooldown = new Cooldown<TopicPartition>(LEASE);
        cooldown.failed(p, t0);
        cooldown.failed(p, t0);
        cooldown.failed(p, t0);
        cooldown.failed(q, t0);

        cooldown.succeeded(p);
        assertThat(cooldown.coolingUntil(p, t0)).isNull();
        assertThat(cooldown.coolingUntil(q, t0)).isEqualTo(Duration.ofSeconds(1));

        cooldown.failed(p, t0);
        assertThat(cooldown.coolingUntil(p, t0)).isEqualTo(Duration.ofSeconds(1));
    }
}
