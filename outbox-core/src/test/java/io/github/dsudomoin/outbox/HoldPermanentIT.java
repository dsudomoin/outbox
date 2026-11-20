package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.BatchClassifier;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code outbox.relay.retry.holdPermanent}: a {@code Permanent} outcome stops its (topic, key) as a {@code Retry} does,
 * whichever pass hands the record (the main pass, the retry pass, a parked row), instead of moving it to the dead table
 * while its key goes on. The relay runs only on {@code drain()}, one shard.
 */
class HoldPermanentIT {

    /** Past the longest retry backoff (500 ms, see {@link #config()}) and the requeue delay (100 ms). */
    private static final long DUE_MILLIS = 600;

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> runtimes = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (int i = runtimes.size() - 1; i >= 0; i--) {
            runtimes.get(i).close();
        }
    }

    @Test
    void aPermanentFailureHoldsItsKeyUntilTheHandlerAcceptsTheRecord() throws Exception {
        var handler = new Scripted(false);
        handler.reject("m2");
        var runtime = start(config().holdPermanent(true), handler);
        var ids = send(runtime, "k", "m1", "m2", "m3");
        send(runtime, "other", "o1");

        String log;
        try (var stderr = new CapturedStderr()) {
            runtime.relay().drain();
            log = stderr.text();
        }

        assertThat(handler.delivered).as("m3 waits behind m2").containsExactly("m1", "o1");
        assertThat(attempts(runtime, ids.get(1))).as("m2, the retry head").isEqualTo(1);
        assertThat(isFollower(runtime, ids.get(2))).as("m3 follows m2").isTrue();
        assertThat(deadRows(runtime)).isZero();
        assertThat(log).contains("ERROR").contains("holdPermanent").contains("id " + idOf(runtime, ids.get(1)))
                .contains("rejected m2");

        send(runtime, "other", "o2");
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(handler.delivered).as("another key goes on").containsExactly("m1", "o1", "o2");
        assertThat(attempts(runtime, ids.get(1))).as("each retry an attempt").isEqualTo(2);
        assertThat(isFollower(runtime, ids.get(2))).isTrue();
        assertThat(deadRows(runtime)).isZero();

        handler.accept("m2");
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(handler.delivered).containsExactly("m1", "o1", "o2", "m2", "m3");
        assertThat(retryRows(runtime)).isZero();
        assertThat(deadRows(runtime)).isZero();
    }

    @Test
    void byDefaultAPermanentFailureGoesDeadAndItsKeyGoesOn() throws Exception {
        var handler = new Scripted(false);
        handler.reject("m2");
        var runtime = start(config(), handler);
        var ids = send(runtime, "k", "m1", "m2", "m3");

        runtime.relay().drain();

        assertThat(handler.delivered).containsExactly("m1", "m3");
        assertThat(deadIds(runtime)).containsExactly(ids.get(1));
        assertThat(retryRows(runtime)).isZero();
    }

    @Test
    void aPermanentFailureFromTheRetryPassIsHeld() throws Exception {
        var handler = new Scripted(false);
        handler.then("r1", Outcome.retry(new IOException("down")));
        handler.reject("r1");
        var runtime = start(config().holdPermanent(true), handler);
        var ids = send(runtime, "k", "r1", "r2");
        send(runtime, "other", "o1"); // delivered in the call: the shard does not pause

        runtime.relay().drain(); // r1 fails transiently: the retry head of its key
        assertThat(attempts(runtime, ids.get(0))).isEqualTo(1);
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain(); // the retry pass hands it: Permanent

        assertThat(attempts(runtime, ids.get(0))).as("held, not dead").isEqualTo(2);
        assertThat(isFollower(runtime, ids.get(1))).isTrue();
        assertThat(deadRows(runtime)).isZero();
        assertThat(handler.delivered).containsExactly("o1");

        handler.accept("r1");
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(handler.delivered).containsExactly("o1", "r1", "r2");
        assertThat(deadRows(runtime)).isZero();
    }

    /** A parked row comes back ahead of the next record of its key in the main pass, before it is due. */
    @Test
    void aPermanentFailureOfAParkedRowMergedAheadOfItsKeyIsHeld() throws Exception {
        var handler = new Scripted(true);
        handler.then("p1", Outcome.requeue());
        handler.reject("p1");
        var runtime = start(config().holdPermanent(true), handler);
        var ids = send(runtime, "k", "p1");
        runtime.relay().drain();
        assertThat(parkedRows(runtime)).as("p1 requeued").isEqualTo(1);

        ids.addAll(send(runtime, "k", "p2"));
        send(runtime, "other", "o1"); // delivered in the call: the shard does not pause
        runtime.relay().drain();

        assertThat(parkedRows(runtime)).isZero();
        assertThat(attempts(runtime, ids.get(0))).as("p1, the retry head").isEqualTo(1);
        assertThat(isFollower(runtime, ids.get(1))).as("p2 follows p1").isTrue();
        assertThat(deadRows(runtime)).isZero();
        assertThat(handler.delivered).containsExactly("o1");

        handler.accept("p1");
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(handler.delivered).containsExactly("o1", "p1", "p2");
        assertThat(deadRows(runtime)).isZero();
    }

    /** A parked row that is due goes to the handler from the retry pass, with no new record of its key. */
    @Test
    void aPermanentFailureOfADueParkedRowIsHeld() throws Exception {
        var handler = new Scripted(true);
        handler.then("p1", Outcome.requeue());
        handler.reject("p1");
        var runtime = start(config().holdPermanent(true), handler);
        var ids = send(runtime, "k", "p1");
        runtime.relay().drain();
        assertThat(parkedRows(runtime)).as("p1 requeued").isEqualTo(1);

        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(parkedRows(runtime)).isZero();
        assertThat(attempts(runtime, ids.get(0))).as("p1, the retry head").isEqualTo(1);
        assertThat(deadRows(runtime)).isZero();

        handler.accept("p1");
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(handler.delivered).containsExactly("p1");
        assertThat(retryRows(runtime)).isZero();
        assertThat(deadRows(runtime)).isZero();
    }

    /** A {@code handleBatch} that throws {@link OutboxPermanentException} for its lone record. */
    @Test
    void aLoneRecordsThrownPermanentFailureIsHeld() throws Exception {
        var handler = new Scripted(true);
        handler.throwing = true;
        handler.reject("x1");
        var runtime = start(config().holdPermanent(true), handler);
        var ids = send(runtime, "k", "x1");

        runtime.relay().drain();

        assertThat(attempts(runtime, ids.get(0))).as("held, not dead").isEqualTo(1);
        assertThat(deadRows(runtime)).isZero();

        handler.accept("x1");
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(handler.delivered).containsExactly("x1");
        assertThat(deadRows(runtime)).isZero();
    }

    @Test
    void withMaxAttemptsAHeldRecordStillGoesDeadAfterThem() throws Exception {
        var handler = new Scripted(false);
        handler.reject("m1");
        var config = config().holdPermanent(true);
        var runtime = start(config.retry(config.initialDelay(), config.maxDelay(), 2), handler);
        var ids = send(runtime, "k", "m1", "m2");
        send(runtime, "other", "o1"); // delivered in the call: the shard does not pause

        runtime.relay().drain();
        assertThat(attempts(runtime, ids.get(0))).as("held after one attempt").isEqualTo(1);
        assertThat(deadRows(runtime)).isZero();
        Thread.sleep(DUE_MILLIS);
        runtime.relay().drain();

        assertThat(deadIds(runtime)).containsExactly(ids.get(0));
        Thread.sleep(DUE_MILLIS); // m2, not attempted in the call behind m1, is the head now
        runtime.relay().drain();

        assertThat(handler.delivered).as("its key goes on").containsExactly("o1", "m2");
    }

    /**
     * One shard, the relay only on {@code drain()}; a key alone in a failing call diverts at once. A retry is due
     * 300 ms after a first failure, 500 ms after a later one: {@code drain()} goes on while a retry head is due, and
     * so makes one attempt of a head, no more.
     */
    private static TestOutboxConfig config() {
        return new TestOutboxConfig().relayEnabled(false).shards(1).headFailuresBeforeDivert(1)
                .retry(Duration.ofMillis(300), Duration.ofMillis(500), 0);
    }

    private OutboxRuntime start(TestOutboxConfig config, OutboxHandler handler) {
        var runtime = OutboxRuntime.builder(config, transactions, handler).build();
        runtimes.add(runtime);
        runtime.start();
        return runtime;
    }

    /** {@code payloads} under {@code key}, in that order, in one transaction. */
    private List<UUID> send(OutboxRuntime runtime, String key, String... payloads) {
        var messages = new ArrayList<OutboxMessage>();
        for (var payload : payloads) {
            messages.add(OutboxMessage.of("orders", key, payload.getBytes(UTF_8)));
        }
        return new ArrayList<>(transactions.inTx(c -> runtime.outbox().sendAll(messages)));
    }

    /** The attempts of the message's row in the retry table; -1 when it has none (delivered, or dead). */
    private long attempts(OutboxRuntime runtime, UUID messageId) {
        return query(runtime, "select coalesce(max(attempts), -1) from %s where message_id = '" + messageId + "'");
    }

    /** Its row waits behind its key's head: no {@code next_attempt_at} of its own. */
    private boolean isFollower(OutboxRuntime runtime, UUID messageId) {
        return query(runtime, "select count(*) from %s where next_attempt_at is null and message_id = '"
                + messageId + "'") == 1;
    }

    /** The relay's id of the message, which the log names. */
    private long idOf(OutboxRuntime runtime, UUID messageId) {
        return query(runtime, "select id from %s where message_id = '" + messageId + "'");
    }

    private long retryRows(OutboxRuntime runtime) {
        return query(runtime, "select count(*) from %s");
    }

    private long deadRows(OutboxRuntime runtime) {
        return transactions.inTx(c -> TestSql.queryLong(c, "select count(*) from " + runtime.schema().names().dead()));
    }

    private long parkedRows(OutboxRuntime runtime) {
        return transactions.inTx(c -> TestSql.queryLong(c,
                "select count(*) from " + runtime.schema().names().parked()));
    }

    private List<UUID> deadIds(OutboxRuntime runtime) {
        return runtime.admin().findDeadLetters(DeadLetterFilter.any(), null, 10).letters().stream()
                .map(DeadLetter::messageId).toList();
    }

    /** {@code sql} with {@code %s} the retry table. */
    private long query(OutboxRuntime runtime, String sql) {
        return transactions.inTx(c -> TestSql.queryLong(c, String.format(sql, runtime.schema().names().retry())));
    }

    /**
     * Answers each record by its payload: its next scripted outcome, else {@code Permanent} while it is rejected, else
     * delivered; keeps the payloads it delivered in order. {@code batch}: its own {@code handleBatch}, which answers
     * outcomes as they are (a requeue too) and, as a handler must under {@code holdPermanent}, does not attempt the
     * rest of a key after a failure; otherwise the default {@code handleBatch} over {@code handle}. {@code throwing}:
     * a call of one rejected record throws {@link OutboxPermanentException} instead.
     */
    private static final class Scripted implements OutboxHandler {

        final List<String> delivered = new CopyOnWriteArrayList<>();
        private final boolean batch;
        private final Map<String, Queue<Outcome>> next = new ConcurrentHashMap<>();
        private final Set<String> rejected = ConcurrentHashMap.newKeySet();
        volatile boolean throwing;

        Scripted(boolean batch) {
            this.batch = batch;
        }

        void then(String payload, Outcome outcome) {
            next.computeIfAbsent(payload, p -> new ConcurrentLinkedQueue<>()).add(outcome);
        }

        void reject(String payload) {
            rejected.add(payload);
        }

        void accept(String payload) {
            rejected.remove(payload);
        }

        private Outcome answer(OutboxRecord record) {
            var payload = new String(record.payload(), UTF_8);
            var scripted = next.getOrDefault(payload, new ConcurrentLinkedQueue<>()).poll();
            if (scripted != null) {
                return scripted;
            }
            if (rejected.contains(payload)) {
                return Outcome.permanent(new OutboxPermanentException("rejected " + payload));
            }
            delivered.add(payload);
            return Outcome.delivered();
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            var outcome = answer(record);
            if (outcome instanceof Outcome.Permanent p) {
                throw (Exception) p.error();
            } else if (outcome instanceof Outcome.Retry r) {
                throw (Exception) r.error();
            } else if (outcome != Outcome.delivered()) {
                throw new IllegalStateException("handle() cannot answer " + outcome);
            }
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            if (!batch) {
                return OutboxHandler.super.handleBatch(records);
            }
            if (throwing && records.size() == 1 && rejected.contains(new String(records.get(0).payload(), UTF_8))) {
                throw new OutboxPermanentException("rejected " + new String(records.get(0).payload(), UTF_8));
            }
            var outcomes = new ArrayList<Outcome>(records.size());
            var failed = new HashSet<Object>();
            for (var record : records) {
                var key = BatchClassifier.orderingKey(record);
                if (failed.contains(key)) {
                    outcomes.add(Outcome.notAttempted());
                    continue;
                }
                var outcome = answer(record);
                if (outcome instanceof Outcome.Permanent || outcome instanceof Outcome.Retry) {
                    failed.add(key);
                }
                outcomes.add(outcome);
            }
            return outcomes;
        }
    }
}
