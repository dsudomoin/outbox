package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * An instance whose JVM clock is off, ahead or behind, on real Postgres: what it schedules goes by the database's clock,
 * so it stalls no other instance and shortens no backoff, and a handler's or hook's own time means the delay it meant.
 */
class ClockSkewIT {

    private static final String TOPIC = "t";
    /** A delay long enough to outlast the test; what was scheduled is read from the database, not waited out. */
    private static final Duration LONG = Duration.ofSeconds(60);
    /** How much of {@link #LONG} a slow machine may have spent before the assertion reads it. */
    private static final long LONG_LOW = 30;
    private static final long LONG_HIGH = 61;

    private final DataSource direct = TestDataSources.newDataSource();
    private final JdbcTransactions transactions = new JdbcTransactions(direct);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<OutboxRuntime> runtimes = new ArrayList<>();

    @AfterEach
    void release() {
        runtimes.forEach(OutboxRuntime::close);
    }

    @ParameterizedTest
    @ValueSource(longs = {7200, -7200})
    void aSkewedRelayLeavesNothingForTheOthersToWaitFor(long skew) throws Exception {
        config.shards(4).retry(Duration.ofMillis(100), Duration.ofMillis(200), 0);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var skewed = runtime(skew, new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) { // the target is down, but for its key "ok"
                return records.stream().map(r -> {
                    if ("ok".equals(key(r))) {
                        delivered.add(r.messageId());
                        return Outcome.delivered();
                    }
                    return Outcome.retry(new IOException("down"));
                }).toList();
            }
        });
        var sent = new HashSet<UUID>();
        for (var key : List.of("ok", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p")) {
            sent.add(send(skewed, key));
        }
        var names = skewed.schema().names();
        await(() -> count("select count(*) from " + names.retry()) > 0
                && count("select count(*) from (select distinct on (shard) paused_until from " + names.cursor()
                        + " order by shard, seq desc) c where paused_until is not null") > 0);
        skewed.close();
        runtimes.remove(skewed);

        runtime(0, record -> delivered.add(record.messageId()));

        await(() -> delivered.containsAll(sent));
        assertThat(count("select count(*) from " + names.retry())).isZero();
    }

    @ParameterizedTest
    @ValueSource(longs = {7200, -7200})
    void aDivertedKeyWaitsOutItsBackoffOnEveryInstance(long skew) throws Exception {
        config.shards(1).retry(LONG, LONG, 0);
        var skewed = runtime(skew, new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                return records.stream().map(r -> "ok".equals(key(r)) ? Outcome.delivered()
                        : Outcome.retry(new IOException("down"))).toList();
            }
        });
        var bad = transactions.inTx(c -> {
            skewed.outbox().send(OutboxMessage.of(TOPIC, "ok", "p".getBytes(UTF_8)));
            return skewed.outbox().send(OutboxMessage.of(TOPIC, "bad", "p".getBytes(UTF_8)));
        });
        var names = skewed.schema().names();
        await(() -> count("select count(*) from " + names.retry()) == 1);

        assertThat(count(remaining("next_attempt_at", names.retry()))).as("the backoff, by the database's clock")
                .isBetween(LONG_LOW, LONG_HIGH);
        skewed.close();
        runtimes.remove(skewed);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        runtime(0, record -> delivered.add(record.messageId()));
        Thread.sleep(1000);

        assertThat(delivered).as("not due on a true clock either").doesNotContain(bad);
        assertThat(count("select attempts from " + names.retry())).as("one attempt spent").isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(longs = {7200, -7200})
    void aPausedShardWaitsOutItsBackoffOnEveryInstance(long skew) throws Exception {
        config.shards(1).retry(LONG, LONG, 0);
        var skewed = runtime(skew, record -> {
            throw new IOException("down");
        });
        var names = skewed.schema().names();
        var sent = transactions.inTx(c -> {
            var first = skewed.outbox().send(OutboxMessage.of(TOPIC, "a", "p".getBytes(UTF_8)));
            return List.of(first, skewed.outbox().send(OutboxMessage.of(TOPIC, "b", "p".getBytes(UTF_8))));
        });
        var pausedUntil = "select paused_until from " + names.cursor() + " order by seq desc limit 1";
        await(() -> count("select count(*) from (" + pausedUntil + ") c where paused_until is not null") == 1);

        assertThat(count("select (extract(epoch from (" + pausedUntil + ") - clock_timestamp()))::bigint"))
                .as("the pause, by the database's clock").isBetween(LONG_LOW, LONG_HIGH);
        skewed.close();
        runtimes.remove(skewed);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        runtime(0, record -> delivered.add(record.messageId()));
        Thread.sleep(1000);

        assertThat(delivered).as("still paused on a true clock").doesNotContainAnyElementsOf(sent);
    }

    @ParameterizedTest
    @ValueSource(longs = {7200, -7200})
    void aHandlersOwnTimeMeansTheDelayItMeant(long skew) throws Exception {
        config.shards(1);
        var clock = clock(skew);
        var skewed = runtime(skew, new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                var at = clock.instant().plus(LONG);
                return records.stream().map(r -> switch (key(r)) {
                    case "retry" -> Outcome.retry(new IOException("down"), at);
                    case "skipped" -> Outcome.notAttempted(at);
                    case "waits" -> Outcome.waitFor("not yet", at);
                    case "requeued" -> Outcome.requeue(at);
                    case "forever" -> Outcome.retry(new IOException("down"), Instant.MAX);
                    case "past" -> Outcome.retry(new IOException("down"), Instant.MIN);
                    default -> Outcome.delivered();
                }).toList();
            }
        });
        transactions.inTx(c -> {
            for (var key : List.of("ok", "retry", "skipped", "waits", "requeued", "forever", "past")) {
                skewed.outbox().send(OutboxMessage.of(TOPIC, key, "p".getBytes(UTF_8)));
            }
            return null;
        });
        var names = skewed.schema().names();
        await(() -> count("select count(*) from " + names.parked()) == 1
                && count("select count(*) from " + names.retry() + " where key <> 'past'::bytea") == 4);

        for (var key : List.of("retry", "skipped", "waits")) {
            assertThat(count(remaining("next_attempt_at", names.retry()) + " where key = '" + key + "'::bytea"))
                    .as(key).isBetween(LONG_LOW, LONG_HIGH);
        }
        assertThat(count(remaining("due_at", names.parked()))).as("requeued").isBetween(LONG_LOW, LONG_HIGH);
        assertThat(count(remaining("next_attempt_at", names.retry()) + " where key = 'forever'::bytea"))
                .as("capped at 24 h").isBetween(23 * 3600L, 24 * 3600L);
    }

    @ParameterizedTest
    @ValueSource(longs = {7200, -7200})
    void aHooksOwnTimeMeansTheDelayItMeant(long skew) throws Exception {
        config.shards(1);
        var clock = clock(skew);
        var skewed = OutboxRuntime.builder(config, transactions, new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                return records.stream().map(r -> Outcome.permanent(new IOException("never"))).toList();
            }
        }).deadLetterHandler(letters -> letters.stream().map(letter -> {
            var key = new String(letter.key(), UTF_8);
            return key.equals("later") ? DeadLetterDecision.redriveAt(clock.instant().plus(LONG))
                    : DeadLetterDecision.redriveAt(Instant.MAX);
        }).toList()).jvmClock(clock).build();
        runtimes.add(skewed);
        skewed.start();
        transactions.inTx(c -> {
            skewed.outbox().send(OutboxMessage.of(TOPIC, "later", "p".getBytes(UTF_8)));
            return skewed.outbox().send(OutboxMessage.of(TOPIC, "forever", "p".getBytes(UTF_8)));
        });
        var dead = skewed.schema().names().dead();
        await(() -> count("select count(*) from " + dead + " where status = 'scheduled'") == 2);

        assertThat(count(remaining("due_at", dead) + " where key = 'later'::bytea"))
                .isBetween(LONG_LOW, LONG_HIGH);
        assertThat(count(remaining("due_at", dead) + " where key = 'forever'::bytea")).as("capped at 7 days")
                .isBetween(7 * 86400L - 60, 7 * 86400L);
    }

    private OutboxRuntime runtime(long skew, OutboxHandler handler) {
        var runtime = OutboxRuntime.builder(config, transactions, handler).jvmClock(clock(skew)).build();
        runtimes.add(runtime);
        runtime.start();
        return runtime;
    }

    private static InstantSource clock(long skewSeconds) {
        return InstantSource.offset(InstantSource.system(), Duration.ofSeconds(skewSeconds));
    }

    private UUID send(OutboxRuntime runtime, String key) {
        return transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(TOPIC, key, "p".getBytes(UTF_8))));
    }

    private static String key(OutboxRecord record) {
        return new String(record.key(), UTF_8);
    }

    /** The whole seconds from the database's clock until {@code column}. */
    private static String remaining(String column, String table) {
        return "select (extract(epoch from " + column + " - clock_timestamp()))::bigint from " + table;
    }

    private long count(String sql) throws Exception {
        try (var c = direct.getConnection()) {
            return TestSql.queryLong(c, sql);
        }
    }

    private static void await(Check condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (!condition.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 15 s");
            }
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }
}
