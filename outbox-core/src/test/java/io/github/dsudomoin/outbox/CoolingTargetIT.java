package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A target that asks the relay to come back later: every record of a call {@code NotAttempted} with a
 * {@code notBefore}, as the Kafka sender answers for a partition cooling down after a failure. Such a call tried
 * nothing, so it is no failed attempt: the shard waits until the earliest {@code notBefore}, and neither its backoff
 * nor its batch limit grows.
 */
class CoolingTargetIT {

    private static final int RECORDS = 8;
    private static final Duration COOLING = Duration.ofSeconds(1);
    private static final int COOLING_CALLS = 4;

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> runtimes = new ArrayList<>();
    /** When each handler call began, nanoTime, and how many records it got. */
    private final List<long[]> calls = new CopyOnWriteArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (int i = runtimes.size() - 1; i >= 0; i--) {
            runtimes.get(i).close();
        }
    }

    @Test
    void aCallThatTriedNothingPausesTheShardUntilTheTargetAsksAndNoLonger() throws Exception {
        var runtime = start(n -> n <= COOLING_CALLS
                ? Outcome.notAttempted(Instant.now().plus(COOLING))
                : Outcome.delivered());
        sendDistinctKeys(runtime);

        await(Duration.ofSeconds(30), () -> calls.size() > COOLING_CALLS);
        long failures = transactions.inTx(c -> TestSql.queryLong(c, "select failures from "
                + runtime.schema().names().cursor() + " order by seq desc limit 1"));

        for (int i = 1; i <= COOLING_CALLS; i++) {
            var gap = Duration.ofNanos(calls.get(i)[0] - calls.get(i - 1)[0]);
            assertThat(gap).as("pause after cooling call %d", i)
                    .isGreaterThanOrEqualTo(COOLING.minusMillis(50)).isLessThan(COOLING.multipliedBy(2));
        }
        assertThat(failures).as("failures counted for calls that tried nothing").isZero();
        assertThat(calls.get(COOLING_CALLS)[1]).as("records in the first call after the cooling").isEqualTo(RECORDS);
    }

    @Test
    void aCallThatTriedAndFailedStillBacksOffDoubling() throws Exception {
        var runtime = start(n -> n <= 3 ? Outcome.retry(new IOException("down")) : Outcome.delivered());
        sendDistinctKeys(runtime);

        await(Duration.ofSeconds(30), () -> calls.size() > 3);

        var second = Duration.ofNanos(calls.get(2)[0] - calls.get(1)[0]);
        var third = Duration.ofNanos(calls.get(3)[0] - calls.get(2)[0]);
        assertThat(second).as("second pause").isGreaterThanOrEqualTo(Duration.ofMillis(1950));
        assertThat(third).as("third pause").isGreaterThanOrEqualTo(Duration.ofMillis(3950));
        assertThat(calls.get(3)[1]).as("records in the first call after three failures").isEqualTo(1);
    }

    /**
     * A call whose earliest {@code notBefore} has passed by the time it returns asked for nothing to wait for: a
     * failed attempt, backing off, so a target that keeps answering so cannot spin the shard.
     */
    @Test
    void aNotBeforeAlreadyPastIsAFailedAttempt() throws Exception {
        var runtime = start(n -> n <= 2 ? Outcome.notAttempted(Instant.now().minusSeconds(1)) : Outcome.delivered());
        sendDistinctKeys(runtime);

        await(Duration.ofSeconds(30), () -> calls.size() > 2);

        assertThat(Duration.ofNanos(calls.get(2)[0] - calls.get(1)[0])).as("second pause")
                .isGreaterThanOrEqualTo(Duration.ofMillis(1950));
    }

    /** One shard, {@link #RECORDS} rows a call, the relay running; {@code outcome} by the call's number from 1. */
    private OutboxRuntime start(Function<Integer, Outcome> outcome) {
        var config = new TestOutboxConfig().shards(1).batchSize(RECORDS).retry(Duration.ofSeconds(1),
                Duration.ofMinutes(5), 0);
        var runtime = OutboxRuntime.builder(config, transactions, new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                calls.add(new long[] {System.nanoTime(), records.size()});
                var answer = outcome.apply(calls.size());
                return records.stream().map(r -> answer).toList();
            }
        }).build();
        runtimes.add(runtime);
        runtime.start();
        return runtime;
    }

    /** Distinct keys, so that a call failing as a whole pauses the shard rather than diverting one key. */
    private void sendDistinctKeys(OutboxRuntime runtime) {
        transactions.inTx(c -> {
            var messages = new ArrayList<OutboxMessage>();
            for (int i = 0; i < RECORDS; i++) {
                messages.add(OutboxMessage.of("orders", "k" + i, ("m" + i).getBytes(UTF_8)));
            }
            return runtime.outbox().sendAll(messages);
        });
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime() < deadline).as("condition within %s", timeout).isTrue();
            Thread.sleep(20);
        }
    }
}
