package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.Handlers.answering;
import static io.github.dsudomoin.outbox.testing.Handlers.eachRecord;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A target that asks the relay to come back later: every record of a call {@code NotAttempted} with a
 * {@code notBefore}, as the Kafka sender answers for a partition cooling down after a failure. Such a call tried
 * nothing: the shard waits until the earliest {@code notBefore}, not a doubling backoff, and its batch limit stays.
 * The step still counts towards probe mode and the divert of a single key ({@code headFailuresBeforeDivert}, 3 here).
 */
class CoolingTargetIT {

    private static final int RECORDS = 8;
    private static final Duration COOLING = Duration.ofSeconds(1);
    /** Below {@code headFailuresBeforeDivert}: no probe yet. */
    private static final int COOLING_CALLS = 2;

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> runtimes = new ArrayList<>();
    /** When each handler call began, nanoTime, and how many records it got. */
    private final List<long[]> calls = new CopyOnWriteArrayList<>();
    /** The shard's failures as each call began: those its last committed cursor carries. */
    private final List<Long> failuresAtCall = new CopyOnWriteArrayList<>();
    private volatile String cursorTable = "";

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

        await(Duration.ofSeconds(30), () -> failuresAtCall.size() > COOLING_CALLS);

        for (int i = 1; i <= COOLING_CALLS; i++) {
            var gap = Duration.ofNanos(calls.get(i)[0] - calls.get(i - 1)[0]);
            assertThat(gap).as("pause after cooling call %d", i)
                    .isGreaterThanOrEqualTo(COOLING.minusMillis(50)).isLessThan(COOLING.multipliedBy(2));
        }
        assertThat(failuresAtCall.get(COOLING_CALLS)).as("one per cooling call, towards the divert")
                .isEqualTo(COOLING_CALLS);
        assertThat(calls.get(COOLING_CALLS)[1]).as("records in the first call after the cooling: the limit stayed")
                .isEqualTo(RECORDS);
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

    /** A target asking to come back sooner than {@code retry.initialDelay} still waits that long: no spinning shard. */
    @Test
    void aPauseTheTargetAsksForIsAtLeastTheInitialDelay() throws Exception {
        var runtime = start(n -> n <= 2 ? Outcome.notAttempted(Instant.now().plusMillis(50)) : Outcome.delivered());
        sendDistinctKeys(runtime);

        await(Duration.ofSeconds(30), () -> calls.size() > 2);

        for (int i = 1; i <= 2; i++) {
            assertThat(Duration.ofNanos(calls.get(i)[0] - calls.get(i - 1)[0])).as("pause after call %d", i)
                    .isGreaterThanOrEqualTo(Duration.ofMillis(950));
        }
    }

    /** A target asking to come back in an hour gets {@code retry.maxDelay}, as a failing one would at most. */
    @Test
    void aPauseTheTargetAsksForIsAtMostTheMaxDelay() throws Exception {
        var runtime = start(n -> n <= 1 ? Outcome.notAttempted(Instant.now().plus(Duration.ofHours(1)))
                : Outcome.delivered(), Duration.ofSeconds(2));
        sendDistinctKeys(runtime);

        await(Duration.ofSeconds(10), () -> calls.size() > 1);

        assertThat(Duration.ofNanos(calls.get(1)[0] - calls.get(0)[0])).as("pause after the call")
                .isGreaterThanOrEqualTo(Duration.ofMillis(1950)).isLessThan(Duration.ofSeconds(4));
    }

    /**
     * A hot key whose rows fill the shard's whole read window keeps cooling: each call holds only its rows, and the
     * target asks to come back every time. Cooling steps still count towards the divert, so the key goes to the retry
     * table and a healthy key behind the window is delivered while it cools, not after the outage; once the key stops
     * cooling, its rows arrive in order.
     */
    @Test
    void aHotKeyThatKeepsCoolingIsDivertedAndTheKeysBehindItGoOn() throws Exception {
        var cooling = new AtomicBoolean(true);
        var delivered = new CopyOnWriteArrayList<String>();
        var config = new TestOutboxConfig().shards(1).batchSize(RECORDS).retry(Duration.ofSeconds(1),
                Duration.ofMinutes(5), 0);
        var runtime = OutboxRuntime.builder(config, transactions, eachRecord(r -> {
            var key = new String(r.key(), UTF_8);
            if (key.equals("hot") && cooling.get()) {
                return Outcome.notAttempted(Instant.now().plus(COOLING));
            }
            delivered.add(key + ":" + new String(r.payload(), UTF_8));
            return Outcome.delivered();
        })).build();
        runtimes.add(runtime);
        runtime.start();
        int hotRows = 2 * RECORDS + 3;
        transactions.inTx(c -> {
            var messages = new ArrayList<OutboxMessage>();
            for (int i = 0; i < hotRows; i++) {
                messages.add(OutboxMessage.of("orders", "hot", ("" + i).getBytes(UTF_8)));
            }
            messages.add(OutboxMessage.of("orders", "healthy", "0".getBytes(UTF_8)));
            return runtime.outbox().sendAll(messages);
        });

        await(Duration.ofSeconds(8), () -> delivered.contains("healthy:0"));
        assertThat(delivered).as("while the hot key cools").containsExactly("healthy:0");

        cooling.set(false);
        await(Duration.ofSeconds(30), () -> delivered.size() == hotRows + 1);
        assertThat(delivered.stream().filter(d -> d.startsWith("hot:")).toList()).as("the hot key, in order")
                .containsExactlyElementsOf(IntStream.range(0, hotRows).mapToObj(i -> "hot:" + i).toList());
    }

    /** One shard, {@link #RECORDS} rows a call, the relay running; {@code outcome} by the call's number from 1. */
    private OutboxRuntime start(Function<Integer, Outcome> outcome) {
        return start(outcome, Duration.ofMinutes(5));
    }

    /** As {@link #start(Function)}, with {@code retry.maxDelay}; {@code retry.initialDelay} is 1 s. */
    private OutboxRuntime start(Function<Integer, Outcome> outcome, Duration maxDelay) {
        var config = new TestOutboxConfig().shards(1).batchSize(RECORDS).retry(Duration.ofSeconds(1), maxDelay, 0);
        var runtime = OutboxRuntime.builder(config, transactions, answering(records -> {
            calls.add(new long[] {System.nanoTime(), records.size()});
            failuresAtCall.add(transactions.inTx(c -> TestSql.queryLong(c, "select failures from " + cursorTable
                    + " order by seq desc limit 1")));
            var answer = outcome.apply(calls.size());
            return records.stream().map(r -> answer).toList();
        })).build();
        runtimes.add(runtime);
        runtime.start();
        cursorTable = runtime.schema().names().cursor(); // before the first row is sent, so before the first call
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
