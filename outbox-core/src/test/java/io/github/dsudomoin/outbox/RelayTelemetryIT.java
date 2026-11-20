package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The relay step and relay failure hooks of a running relay: each step ends once, each failure counts once. */
class RelayTelemetryIT {

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final RecordingTelemetry telemetry = new RecordingTelemetry();
    @Nullable
    private OutboxRuntime runtime;

    @AfterEach
    void close() {
        if (runtime != null) {
            runtime.close();
        }
    }

    @Test
    void everyStepWithLockedShardsStartsAndEndsOnce() throws Exception {
        var handler = new RecordingHandler();
        var outbox = start(new TestOutboxConfig(), handler);
        var messages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 20; i++) {
            messages.add(OutboxMessage.of("orders", "k" + i % 5, bytes("m" + i)));
        }

        transactions.inTx(c -> outbox.sendAll(messages));
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 20);
        runtime.close(); // no step in flight from here

        assertThat(telemetry.steps).isNotEmpty().allSatisfy(step -> {
            assertThat(step.shards).isNotEmpty();
            assertThat(step.finished).hasValue(1);
            assertThat(step.failed).hasValue(0);
        });
        assertThat(telemetry.relayFailures).hasValue(0);
    }

    @Test
    void aFailingShardIsCountedOncePerSingleShardFailure() throws Exception {
        // 4 shards, the relay running; one worker steps both shards of the send together first
        var config = new TestOutboxConfig().workers(1);
        int goodShard = shard("good");
        String bad = "bad";
        for (int i = 0; shard(bad) == goodShard; i++) {
            bad = "bad" + i;
        }
        int badShard = shard(bad);
        var handler = new RecordingHandler();
        var outbox = start(config, handler);
        // the step's own SQL fails, not its handler call (a handler failure is an outcome): every cursor append of the
        // bad shard raises, failing each step that holds it
        var function = config.table() + "_fail";
        var cursor = config.table() + "_cursor";
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "create function " + function + "() returns trigger language plpgsql as $$ begin"
                    + " if new.shard = " + badShard + " then raise exception 'shard down'; end if; return new; end $$");
            TestSql.execute(c, "create trigger fail before insert on " + cursor
                    + " for each row execute function " + function + "()");
        }

        var messages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 5; i++) {
            messages.add(OutboxMessage.of("orders", "good", bytes("g" + i)));
            messages.add(OutboxMessage.of("orders", bad, bytes("b" + i)));
        }
        var ids = transactions.inTx(c -> outbox.sendAll(messages));
        await(Duration.ofSeconds(30), () -> telemetry.failedAlone(badShard) >= 2);
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "drop trigger fail on " + cursor);
        }
        await(Duration.ofSeconds(30), () -> delivered(handler).containsAll(ids));
        runtime.close(); // no step in flight from here

        var badKey = bytes(bad);
        assertThat(handler.delivered()).filteredOn(r -> Arrays.equals(r.key(), badKey)).isNotEmpty()
                .allSatisfy(r -> assertThat(r.shard()).isEqualTo(badShard));
        assertThat(telemetry.steps).allSatisfy(step -> assertThat(step.ends()).isOne());
        assertThat(telemetry.steps).as("a step of both shards failed, then was redone shard by shard")
                .anySatisfy(step -> {
                    assertThat(step.shards).containsExactlyInAnyOrder(goodShard, badShard);
                    assertThat(step.failed).hasValue(1);
                });
        assertThat(telemetry.relayFailures).hasValue(telemetry.failedAlone(badShard));
    }

    @Test
    void aShardThatKeepsFailingAloneBacksOffWhileTicksKeepComing() throws Exception {
        // ticks at minTickInterval from commits elsewhere; retry delays from 1 s: the shard's calls come at about 0 s,
        // the next tick, 1 s and 3 s later, not once per tick
        var config = new TestOutboxConfig().workers(1).minTickInterval(Duration.ofMillis(50))
                .idlePollInterval(Duration.ofMillis(200)).retry(Duration.ofSeconds(1), Duration.ofMinutes(5), 0);
        int goodShard = shard("good");
        String bad = "bad";
        for (int i = 0; shard(bad) == goodShard; i++) {
            bad = "bad" + i;
        }
        int badShard = shard(bad);
        var handler = new RecordingHandler();
        var outbox = start(config, handler);
        var function = config.table() + "_fail";
        var cursor = config.table() + "_cursor";
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "create function " + function + "() returns trigger language plpgsql as $$ begin"
                    + " if new.shard = " + badShard + " then raise exception 'shard down'; end if; return new; end $$");
            TestSql.execute(c, "create trigger fail before insert on " + cursor
                    + " for each row execute function " + function + "()");
        }
        var badMessages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 5; i++) {
            badMessages.add(OutboxMessage.of("orders", bad, bytes("b" + i)));
        }
        var badIds = transactions.inTx(c -> outbox.sendAll(badMessages));
        await(Duration.ofSeconds(30), () -> telemetry.failedAlone(badShard) >= 1); // isolated: stepped alone from here

        var goodIds = new ArrayList<UUID>();
        long until = System.nanoTime() + Duration.ofSeconds(6).toNanos();
        int i = 0;
        while (System.nanoTime() < until) {
            var message = OutboxMessage.of("orders", "good", bytes("g" + i++));
            goodIds.add(transactions.inTx(c -> outbox.send(message)));
            Thread.sleep(100);
        }
        var handed = handler.delivered().stream().filter(r -> badIds.contains(r.messageId())).toList();
        assertThat(badIds).allSatisfy(id -> assertThat(handed).filteredOn(r -> r.messageId().equals(id))
                .as("calls of a record of the failing shard in 6 s").hasSizeBetween(1, 4));
        assertThat(telemetry.failedAlone(badShard)).isLessThanOrEqualTo(4);

        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "drop trigger fail on " + cursor);
        }
        await(Duration.ofSeconds(30), () -> delivered(handler).containsAll(badIds) && telemetry.steps.stream()
                .anyMatch(s -> s.shards.equals(List.of(badShard)) && s.finished.get() > 0));
        await(Duration.ofSeconds(10), () -> delivered(handler).containsAll(goodIds));
        runtime.close(); // no step in flight from here

        assertThat(delivered(handler)).filteredOn(goodIds::contains).as("the other shard, delivered once each")
                .doesNotHaveDuplicates().hasSameSizeAs(goodIds);
    }

    @Test
    void stoppingIsNotARelayFailure() throws Exception {
        var called = new CountDownLatch(1);
        OutboxHandler sleeping = record -> {
            called.countDown();
            Thread.sleep(Long.MAX_VALUE); // until interrupted
        };
        var outbox = start(new TestOutboxConfig().shutdownTimeout(Duration.ofMillis(200)), sleeping);

        transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k", bytes("m"))));
        assertThat(called.await(10, TimeUnit.SECONDS)).isTrue();
        runtime.close(); // interrupts the step after shutdownTimeout

        await(Duration.ofSeconds(5), () -> telemetry.steps.stream().allMatch(s -> s.ends() > 0));
        assertThat(telemetry.steps).isNotEmpty().allSatisfy(step -> assertThat(step.ends()).isOne());
        assertThat(telemetry.relayFailures).hasValue(0);
    }

    @Test
    void stepEndsAfterItsCommit() throws Exception {
        var config = new TestOutboxConfig();
        var events = new Events(transactions, config.table() + "_cursor");
        var handler = new RecordingHandler();
        var outbox = start(config, handler, events);

        var id = transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k", bytes("m"))));
        await(Duration.ofSeconds(10), () -> events.ended().stream().anyMatch(Events.Observed::delivered));
        runtime.close();

        var step = events.single();
        int shard = shard("k");
        assertThat(step.log).containsExactly("stepStarted", "fetched(" + shard + ",1)", "deliveryStarted",
                "recordFinished(" + id + ",Delivered)", "deliveryFinished", "settleStarted", "committed(1)",
                "finished");
        assertThat(step.cursorRowsAtFinish).as("the cursor append is committed when the step finishes")
                .isGreaterThan(step.cursorRowsAtSettle);
        assertThat(events.foreignSteps).as("deliveryStarted gets the step its stepStarted returned").isZero();
    }

    @Test
    void aFailingCommitEndsSettleAndStepFailed() throws Exception {
        var config = new TestOutboxConfig().workers(1);
        var cursor = config.table() + "_cursor";
        var events = new Events(transactions, cursor);
        var handler = new RecordingHandler();
        var outbox = start(config, handler, events);
        var function = config.table() + "_refuse";
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "create function " + function + "() returns trigger language plpgsql as $$ begin"
                    + " raise exception 'commit refused'; end $$");
            TestSql.execute(c, "create constraint trigger refuse after insert on " + cursor
                    + " deferrable initially deferred for each row execute function " + function + "()");
        }

        transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k", bytes("m"))));
        await(Duration.ofSeconds(10), () -> events.ended().stream().anyMatch(s -> s.log.contains("failed")));
        runtime.close(); // no step left to contend for the table with the drop
        try (var c = transactions.newConnection()) {
            TestSql.execute(c, "drop trigger refuse on " + cursor);
        }

        var failed = events.ended().stream().filter(s -> s.log.contains("failed")).findFirst().orElseThrow();
        assertThat(failed.log).endsWith("settleStarted", "settleFailed(1)", "failed").doesNotContain("finished",
                "committed(1)");
        assertThat(failed.errors).hasSize(2).allSatisfy(e -> assertThat(e).isSameAs(failed.errors.get(0)));
        assertThat(failed.errors.get(0)).hasMessageContaining("commit refused");
    }

    @Test
    void fetchedCountsEveryRowReadPerShard() throws Exception {
        String a = "a";
        String b = "b";
        for (int i = 0; shard(b) == shard(a); i++) {
            b = "b" + i;
        }
        var config = new TestOutboxConfig();
        var events = new Events(transactions, config.table() + "_cursor");
        var handler = new RecordingHandler();
        var outbox = start(config, handler, events);
        var messages = new ArrayList<OutboxMessage>();
        for (int i = 0; i < 3; i++) {
            messages.add(OutboxMessage.of("orders", a, bytes("a" + i)));
        }
        for (int i = 0; i < 2; i++) {
            messages.add(OutboxMessage.of("orders", b, bytes("b" + i)));
        }

        transactions.inTx(c -> outbox.sendAll(messages));
        await(Duration.ofSeconds(10), () -> handler.delivered().size() >= 5);
        runtime.close();

        assertThat(events.fetched(shard(a))).isEqualTo(3);
        assertThat(events.fetched(shard(b))).isEqualTo(2);
        assertThat(events.ended()).allSatisfy(step -> assertThat(step.log.stream()
                .filter(e -> e.startsWith("fetched(")).map(e -> e.substring(0, e.indexOf(','))))
                .as("one fetched per shard of a step").doesNotHaveDuplicates());
    }

    @Test
    void settledCountsTheRowsTheCommitMoves() throws Exception {
        var keys = sameShardKeys(3);
        var config = new TestOutboxConfig();
        var events = new Events(transactions, config.table() + "_cursor");
        var first = new java.util.concurrent.atomic.AtomicBoolean(true);
        OutboxHandler handler = new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                if (records.size() == 3 && first.compareAndSet(true, false)) {
                    return List.of(Outcome.delivered(), Outcome.retry(new java.io.IOException("down")),
                            Outcome.notAttempted());
                }
                return java.util.Collections.nCopies(records.size(), Outcome.delivered());
            }
        };
        var outbox = start(config, handler, events);
        var messages = keys.stream().map(k -> OutboxMessage.of("orders", k, bytes(k))).toList();

        transactions.inTx(c -> outbox.sendAll(messages));
        await(Duration.ofSeconds(10), () -> !first.get() && events.ended().stream().anyMatch(s -> s.handed == 3));
        runtime.close();

        var step = events.ended().stream().filter(s -> s.handed == 3).findFirst().orElseThrow();
        assertThat(step.log).as("delivered and retried move, the trailing not-attempted row is read again")
                .contains("committed(2)");
    }

    @Test
    void recordFinishedReachesTheDelivery() throws Exception {
        var config = new TestOutboxConfig();
        var events = new Events(transactions, config.table() + "_cursor");
        var reports = new CopyOnWriteArrayList<Boolean>();
        OutboxHandler handler = new OutboxHandler() {
            @Override
            public void handle(OutboxRecord record) {}

            @Override
            public List<Outcome> handleBatch(List<OutboxRecord> records) {
                reports.add(io.github.dsudomoin.outbox.internal.RecordScope.reports());
                OutboxHandler.recordFinished(records.get(0), Outcome.delivered());
                return java.util.Collections.nCopies(records.size(), Outcome.delivered());
            }
        };
        var outbox = start(config, handler, events);

        var id = transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k", bytes("m"))));
        await(Duration.ofSeconds(10), () -> events.ended().stream().anyMatch(Events.Observed::delivered));
        runtime.close();

        var log = events.single().log;
        assertThat(log).containsSubsequence("deliveryStarted", "recordFinished(" + id + ",Delivered)",
                "deliveryFinished");
        assertThat(events.sameRecord).as("the handed record itself").containsOnly(true).isNotEmpty();
        assertThat(reports).containsOnly(true);
        assertThat(io.github.dsudomoin.outbox.internal.RecordScope.reports()).as("outside a call").isFalse();
        OutboxHandler.recordFinished(events.lastHanded, Outcome.delivered()); // outside a call: nothing, no throw
        assertThat(events.single().log).isEqualTo(log);
    }

    @Test
    void defaultHandleBatchReportsEachAttempt() throws Exception {
        var keys = sameShardKeys(2);
        var config = new TestOutboxConfig();
        var events = new Events(transactions, config.table() + "_cursor");
        var failed = new java.util.concurrent.atomic.AtomicBoolean();
        OutboxHandler handler = record -> {
            if (new String(record.key(), UTF_8).equals(keys.get(1)) && failed.compareAndSet(false, true)) {
                throw new IllegalStateException("down");
            }
        };
        var outbox = start(config, handler, events);
        var ids = transactions.inTx(c -> outbox.sendAll(List.of(OutboxMessage.of("orders", keys.get(0), bytes("1")),
                OutboxMessage.of("orders", keys.get(1), bytes("2")), OutboxMessage.of("orders", keys.get(1),
                        bytes("3")))));
        await(Duration.ofSeconds(10), () -> events.ended().stream().anyMatch(s -> s.handed == 3));
        runtime.close();

        var step = events.ended().stream().filter(s -> s.handed == 3).findFirst().orElseThrow();
        assertThat(step.log.stream().filter(e -> e.startsWith("recordFinished")).toList()).containsExactly(
                "recordFinished(" + ids.get(0) + ",Delivered)", "recordFinished(" + ids.get(1) + ",Retry)");
    }

    @Test
    void deadLetteredCarriesTheErrorType() throws Exception {
        var config = new TestOutboxConfig().retry(Duration.ofMillis(20), Duration.ofMillis(100), 1);
        var events = new Events(transactions, config.table() + "_cursor");
        OutboxHandler handler = record -> {
            if (new String(record.key(), UTF_8).equals("permanent")) {
                throw new OutboxPermanentException("x");
            }
            throw new IllegalStateException("down");
        };
        var outbox = start(config, handler, events);

        var failing = transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "failing", bytes("f"))));
        var permanent = transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "permanent", bytes("p"))));
        await(Duration.ofSeconds(15), () -> events.deadLettered.size() >= 2);
        runtime.close();

        assertThat(events.deadLettered).containsExactlyInAnyOrder(failing + " max_attempts",
                permanent + " io.github.dsudomoin.outbox.OutboxPermanentException");
    }

    private Outbox start(TestOutboxConfig config, OutboxHandler handler) {
        return start(config, handler, telemetry);
    }

    private Outbox start(TestOutboxConfig config, OutboxHandler handler, OutboxTelemetry telemetry) {
        var r = OutboxRuntime.builder(config, transactions, handler).telemetry(telemetry).build();
        runtime = r;
        r.start();
        return r.outbox();
    }

    private static int shard(String key) {
        return Hashing.shard(Hashing.keyHash("orders", bytes(key)), 4);
    }

    /** {@code count} distinct keys on one shard. */
    private static List<String> sameShardKeys(int count) {
        var keys = new ArrayList<String>();
        int shard = shard("s0");
        for (int i = 0; keys.size() < count; i++) {
            if (shard("s" + i) == shard) {
                keys.add("s" + i);
            }
        }
        return keys;
    }

    private static List<UUID> delivered(RecordingHandler handler) {
        return handler.delivered().stream().map(OutboxRecord::messageId).toList();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(10);
        }
    }

    /** The steps started, in order, and the relay failures. */
    private static final class RecordingTelemetry implements OutboxTelemetry {

        final List<Step> steps = new CopyOnWriteArrayList<>();
        final AtomicInteger relayFailures = new AtomicInteger();

        @Override
        public RelayStep stepStarted(List<Integer> shards) {
            var step = new Step(List.copyOf(shards));
            steps.add(step);
            return step;
        }

        @Override
        public void relayFailed(Throwable error) {
            relayFailures.incrementAndGet();
        }

        /** The failed steps of {@code shard} alone. */
        int failedAlone(int shard) {
            return (int) steps.stream().filter(s -> s.failed.get() > 0 && s.shards.equals(List.of(shard))).count();
        }
    }

    /**
     * The relay steps with work, each with the events of its step, settle and deliveries in order, and the dead
     * letters; at the settle's start and the step's end it counts the shard's cursor rows on a connection of its own.
     */
    private static final class Events implements OutboxTelemetry {

        final List<Observed> steps = new CopyOnWriteArrayList<>();
        final List<String> deadLettered = new CopyOnWriteArrayList<>();
        final List<Boolean> sameRecord = new CopyOnWriteArrayList<>();
        private final JdbcTransactions transactions;
        private final String cursor;
        volatile int foreignSteps;
        @Nullable
        volatile OutboxRecord lastHanded;

        Events(JdbcTransactions transactions, String cursor) {
            this.transactions = transactions;
            this.cursor = cursor;
        }

        @Override
        public RelayStep stepStarted(List<Integer> shards) {
            var step = new Observed();
            step.log.add("stepStarted");
            steps.add(step);
            return step;
        }

        @Override
        public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, java.time.Instant now) {
            if (!(step instanceof Observed observed)) {
                foreignSteps++;
                return Delivery.NONE;
            }
            observed.log.add("deliveryStarted");
            observed.handed = records.size();
            return new Delivery() {
                private volatile List<OutboxRecord> handed = List.of();

                @Override
                public List<OutboxRecord> handed(List<OutboxRecord> records) {
                    handed = records;
                    return records;
                }

                @Override
                public void recordFinished(OutboxRecord record, Outcome outcome) {
                    sameRecord.add(handed.stream().anyMatch(r -> r == record));
                    lastHanded = record;
                    observed.log.add("recordFinished(" + record.messageId() + "," + outcome.getClass().getSimpleName()
                            + ")");
                }

                @Override
                public void finished(List<Outcome> outcomes) {
                    observed.log.add("deliveryFinished");
                }

                @Override
                public void failed(Throwable error) {
                    observed.log.add("deliveryFailed");
                }
            };
        }

        @Override
        public void deadLettered(OutboxRecord record, String errorType) {
            deadLettered.add(record.messageId() + " " + errorType);
        }

        List<Observed> ended() {
            return steps.stream().filter(s -> s.log.contains("finished") || s.log.contains("failed")).toList();
        }

        /** The one step that handed records: a step may find work and hand none (a tick of no row of its shard). */
        Observed single() {
            var delivering = steps.stream().filter(Observed::delivered).toList();
            assertThat(delivering).hasSize(1);
            return delivering.get(0);
        }

        /** The rows fetched of {@code shard}, all steps together. */
        int fetched(int shard) {
            return steps.stream().flatMap(s -> s.log.stream()).filter(e -> e.startsWith("fetched(" + shard + ","))
                    .mapToInt(e -> Integer.parseInt(e.substring(e.indexOf(',') + 1, e.length() - 1))).sum();
        }

        private long cursorRows(int shard) {
            try (var c = transactions.newConnection()) {
                return TestSql.queryLong(c, "select count(*) from " + cursor + " where shard = " + shard);
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        final class Observed implements RelayStep {

            final List<String> log = new CopyOnWriteArrayList<>();
            final List<Throwable> errors = new CopyOnWriteArrayList<>();
            volatile int shard = -1;
            volatile int handed;
            volatile long cursorRowsAtSettle = -1;
            volatile long cursorRowsAtFinish = -1;

            boolean delivered() {
                return log.contains("deliveryStarted") && (log.contains("finished") || log.contains("failed"));
            }

            @Override
            public void fetched(int shard, int records) {
                this.shard = shard;
                log.add("fetched(" + shard + "," + records + ")");
            }

            @Override
            public Settle settleStarted() {
                log.add("settleStarted");
                if (shard >= 0) {
                    cursorRowsAtSettle = cursorRows(shard);
                }
                return new Settle() {
                    @Override
                    public void committed(int records) {
                        log.add("committed(" + records + ")");
                    }

                    @Override
                    public void failed(int records, Throwable error) {
                        errors.add(error);
                        log.add("settleFailed(" + records + ")");
                    }
                };
            }

            @Override
            public void finished() {
                if (shard >= 0) {
                    cursorRowsAtFinish = cursorRows(shard);
                }
                log.add("finished");
            }

            @Override
            public void failed(Throwable error) {
                errors.add(error);
                log.add("failed");
            }
        }
    }

    /** Counts each way it ends. */
    private static final class Step implements OutboxTelemetry.RelayStep {

        final List<Integer> shards;
        final AtomicInteger finished = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();

        Step(List<Integer> shards) {
            this.shards = shards;
        }

        int ends() {
            return finished.get() + failed.get();
        }

        @Override
        public void finished() {
            finished.incrementAndGet();
        }

        @Override
        public void failed(Throwable error) {
            failed.incrementAndGet();
        }
    }
}
