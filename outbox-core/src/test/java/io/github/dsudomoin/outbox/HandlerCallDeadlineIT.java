package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.Handlers.answering;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.ShardProcessor;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** What a handler call of a running relay sees of its own bounds ({@link ShardProcessor#callDeadline()}). */
class HandlerCallDeadlineIT {

    private static final Duration BATCH_TIMEOUT = Duration.ofSeconds(5);

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());

    @Test
    void aHandlerCallStartsWithItsWholeBatchTimeoutAhead() throws Exception {
        var seen = new CompletableFuture<long[]>();
        var config = new TestOutboxConfig().shards(1).batchTimeout(BATCH_TIMEOUT);
        try (var runtime = OutboxRuntime.builder(config, transactions, answering(records -> {
            long now = System.nanoTime();
            seen.complete(new long[] {ShardProcessor.callDeadline().orElseThrow() - now,
                    now - ShardProcessor.callStarted().orElseThrow(), ShardProcessor.callCancelled() ? 1 : 0});
            return records.stream().map(r -> Outcome.delivered()).toList();
        })).build()) {
            runtime.start();
            transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("orders", "k", "m".getBytes(UTF_8))));

            var at = seen.get(30, TimeUnit.SECONDS);

            assertThat(Duration.ofNanos(at[0])).as("left of the call at its start")
                    .isGreaterThan(BATCH_TIMEOUT.minusMillis(500)).isLessThanOrEqualTo(BATCH_TIMEOUT);
            assertThat(Duration.ofNanos(at[1])).as("since its submit").isGreaterThanOrEqualTo(Duration.ZERO)
                    .isLessThan(Duration.ofMillis(500));
            assertThat(at[2]).as("cancelled").isZero();
        }
        assertThat(ShardProcessor.callDeadline()).as("on the test's own thread").isEmpty();
    }
}
