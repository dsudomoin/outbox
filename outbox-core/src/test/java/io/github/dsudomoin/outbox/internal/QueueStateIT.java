package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTelemetry.ShardQueue;
import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.internal.Cursors.Position;
import io.github.dsudomoin.outbox.internal.Ticks.Tick;
import io.github.dsudomoin.outbox.testing.ShardKeys;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Maintenance's per-shard report of the pending messages and the age of the oldest, with its two total gauges. */
class QueueStateIT {

    private static final int SHARDS = 4;
    private static final String TOPIC = "t";

    private final DataSource direct = TestDataSources.newDataSource();
    private final JdbcTransactions transactions = new JdbcTransactions(direct);
    private final Recording telemetry = new Recording();
    private final ShardKeys keys = new ShardKeys(TOPIC, SHARDS);
    private OutboxRuntime writer;
    private TableNames names;
    private long extraId = 1_000_000;

    private void start(OutboxTelemetry telemetry) {
        writer = OutboxRuntime.builder(new TestOutboxConfig().shards(SHARDS).relayEnabled(false), transactions,
                record -> {}).telemetry(telemetry).build();
        writer.start();
        names = writer.schema().names();
    }

    @AfterEach
    void release() {
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void exactCountsPerShard() throws Exception {
        start(telemetry);
        fillElevenPending();

        assertThat(writer.maintenance().runOnce()).isTrue();

        var state = telemetry.lastState();
        assertThat(state).extracting(ShardQueue::shard).containsExactly(0, 1, 2, 3);
        assertThat(state).extracting(ShardQueue::pending).containsExactly(5.0, 5.0, 1.0, 0.0);
        assertThat(state).extracting(ShardQueue::estimated).containsOnly(false);
        assertThat(state.get(0).oldestSeconds()).as("a main row aged by an hour").isGreaterThanOrEqualTo(3600);
        assertThat(state.get(1).oldestSeconds()).as("a retry row aged by two hours").isGreaterThanOrEqualTo(7200);
        assertThat(state.get(2).oldestSeconds()).as("the parked row aged by three hours")
                .isGreaterThanOrEqualTo(10800);
        assertThat(state.get(3).oldestSeconds()).isZero();
    }

    @Test
    void aBatchInProgressCountsOnlyItsUnreadRows() throws Exception {
        start(telemetry);
        long from = latestTick().id();
        var ids = new ArrayList<Long>();
        for (int i = 0; i < 4; i++) {
            ids.add(idOf(send(0)));
        }
        makeATick();
        long target = latestTick().id();
        send(0); // after the target: the next batch's
        try (var c = direct.getConnection()) {
            new Cursors(names, SHARDS).append(c, new Cursor(0, from, target, ids.get(1), null, 0, null, null));
        }

        assertThat(writer.maintenance().runOnce()).isTrue();

        var state = telemetry.lastState();
        assertThat(state.get(0)).as("2 rows above last_id and 1 after the target")
                .isEqualTo(new ShardQueue(0, 3, state.get(0).oldestSeconds(), false));
        assertThat(state.get(1).pending()).isZero();
    }

    @Test
    void overBudgetFallsBackToTheEstimate() throws Exception {
        start(telemetry);
        long before = latestTick().maxId();
        for (int i = 0; i < 4; i++) {
            send(0);
        }
        send(1);
        send(1);
        makeATick();
        try (var c = direct.getConnection()) {
            insertRetry(c, 2, 0);
        }
        var queueState = new QueueState(names, SHARDS, Duration.ofMillis(200));
        try (var holder = direct.getConnection(); var c = direct.getConnection()) {
            holder.setAutoCommit(false);
            // the count waits for the lock past its budget
            execute(holder, "lock table " + names.base() + " in access exclusive mode");
            execute(c, "set statement_timeout = '7s'");
            var latest = new Ticks(names, SHARDS).latest(c);
            long latestMaxId = latest.maxId();
            long effective = new Cursors(names, SHARDS).positions(c, latest.id()).get(0).effectiveTickId();
            double perShard = (latestMaxId - before) / (double) SHARDS;

            var state = queueState.read(c);

            assertThat(state).extracting(ShardQueue::estimated).containsOnly(true);
            assertThat(state.get(0).pending()).isCloseTo(perShard, within(1e-9));
            assertThat(state.get(1).pending()).isCloseTo(perShard, within(1e-9));
            assertThat(state.get(2).pending()).as("a shard not busy: its retry row alone").isEqualTo(1);
            assertThat(state.get(3).pending()).isZero();
            assertThat(state.get(0).oldestSeconds()).isGreaterThanOrEqualTo(0);
            assertThat(queryString(c, "show statement_timeout")).as("the session's own, given back").isEqualTo("7s");
            assertThat(c.getAutoCommit()).isTrue();
            assertThat(queryLong(c, "select 1")).isEqualTo(1);

            // a tick without max_id: unknown
            execute(c, "update " + names.tick() + " set max_id = null where id = " + effective);
            var unknown = queueState.read(c);
            assertThat(unknown.get(0).pending()).isNaN();
            assertThat(unknown.get(0).estimated()).isTrue();
            assertThat(unknown.get(2).pending()).isEqualTo(1);
            holder.rollback();
        }
    }

    @Test
    void aStepCommittedAfterTheCursorReadIsCountedAsItLeftTheRows() throws Exception {
        start(telemetry);
        var ids = new ArrayList<java.util.UUID>();
        for (int i = 0; i < 3; i++) {
            ids.add(send(0));
        }
        makeATick();
        long tick = latestTick().id();
        // a relay step between maintenance's cursor read and its count: all 3 handed, the last one to the retry table
        writer.maintenance().gaugesStarted = () -> {
            try (var step = direct.getConnection()) {
                step.setAutoCommit(false);
                new Cursors(names, SHARDS).append(step, new Cursor(0, tick, null, 0, null, 0, null, null));
                var columns = RetryStore.COLUMNS;
                execute(step, "insert into " + names.retry() + " (" + columns + ", attempts, next_attempt_at)"
                        + " select " + columns + ", 1, now() + interval '1 hour' from " + names.base()
                        + " where message_id = '" + ids.get(2) + "'");
                step.commit();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        };

        assertThat(writer.maintenance().runOnce()).isTrue();

        var state = telemetry.lastState();
        assertThat(state.get(0)).as("the retry row alone, once")
                .isEqualTo(new ShardQueue(0, 1, state.get(0).oldestSeconds(), false));
    }

    @Test
    void aLockTimeoutBelowTheBudgetEndsInTheEstimate() throws Exception {
        start(telemetry);
        send(0);
        makeATick();
        var queueState = new QueueState(names, SHARDS, Duration.ofMillis(500));
        try (var holder = direct.getConnection(); var c = direct.getConnection()) {
            holder.setAutoCommit(false);
            execute(holder, "lock table " + names.base() + " in access exclusive mode");
            execute(c, "set lock_timeout = '50ms'"); // as a role or database default may set it

            var state = queueState.read(c);

            assertThat(state).extracting(ShardQueue::estimated).containsOnly(true);
            assertThat(state.get(0).pending()).isGreaterThan(0);
            assertThat(queryString(c, "show lock_timeout")).isEqualTo("50ms");
            holder.rollback();
        }
    }

    @Test
    void noTelemetryNoQuery() throws Exception {
        assertThat(GuardedTelemetry.of(OutboxTelemetry.NONE)).isSameAs(OutboxTelemetry.NONE);
        start(OutboxTelemetry.NONE);
        send(0);
        var reads = new AtomicInteger();
        writer.maintenance().queueState = new QueueState(names, SHARDS, QueueState.BUDGET) {
            @Override
            List<ShardQueue> read(Connection c) throws SQLException {
                reads.incrementAndGet();
                return super.read(c);
            }
        };

        assertThat(writer.maintenance().runOnce()).isTrue();

        assertThat(reads).hasValue(0);
    }

    @Test
    void gaugesSumAndMax() throws Exception {
        start(telemetry);
        fillElevenPending();

        assertThat(writer.maintenance().runOnce()).isTrue();

        var state = telemetry.lastState();
        assertThat(telemetry.gauge(OutboxGauge.PENDING)).isEqualTo(11);
        assertThat(telemetry.gauge(OutboxGauge.PENDING_OLDEST))
                .isEqualTo(state.stream().mapToDouble(ShardQueue::oldestSeconds).max().orElseThrow());
    }

    /**
     * 5 rows on shard 0 (two before a tick, so the shard is behind at it), 3 on shard 1, 2 retry rows on shard 1, 1
     * parked row on shard 2; one of each kind aged by hours.
     */
    private void fillElevenPending() throws Exception {
        long agedMain = idOf(send(0));
        send(0);
        makeATick();
        for (int i = 0; i < 3; i++) {
            send(0);
            send(1);
        }
        try (var c = direct.getConnection()) {
            execute(c, "update " + names.base() + " set created_at = now() - interval '1 hour' where id = " + agedMain);
            insertRetry(c, 1, 2);
            insertRetry(c, 1, 0);
            execute(c, "insert into " + names.parked() + " (id, shard, message_id, topic, created_at, due_at) values ("
                    + extraId++ + ", 2, gen_random_uuid(), '" + TOPIC + "', now() - interval '3 hours',"
                    + " now() + interval '1 hour')");
        }
    }

    /** A retry row of {@code shard} created {@code hoursAgo}, with an id the outbox never handed out. */
    private void insertRetry(Connection c, int shard, int hoursAgo) throws SQLException {
        execute(c, "insert into " + names.retry() + " (id, shard, message_id, topic, created_at, attempts,"
                + " next_attempt_at) values (" + extraId++ + ", " + shard + ", gen_random_uuid(), '" + TOPIC
                + "', now() - interval '" + hoursAgo + " hours', 1, now() + interval '1 hour')");
    }

    private Tick latestTick() throws SQLException {
        try (var c = direct.getConnection()) {
            return new Ticks(names, SHARDS).latest(c);
        }
    }

    private void makeATick() throws SQLException {
        try (var c = direct.getConnection()) {
            assertThat(new Ticks(names, SHARDS).make(c, Duration.ZERO)).isTrue();
        }
    }

    private long idOf(java.util.UUID messageId) throws SQLException {
        try (var c = direct.getConnection()) {
            return queryLong(c, "select id from " + names.base() + " where message_id = '" + messageId + "'");
        }
    }

    private java.util.UUID send(int shard) {
        var key = keyOfShard(shard);
        return transactions.inTx(c -> writer.outbox().send(OutboxMessage.of(TOPIC, key, "p".getBytes(UTF_8))));
    }

    /** A new key hashing to {@code shard}. */
    private String keyOfShard(int shard) {
        return keys.next(shard);
    }

    private static final class Recording implements OutboxTelemetry {

        private final Map<OutboxGauge, DoubleSupplier> gauges = new ConcurrentHashMap<>();
        private volatile List<ShardQueue> last;

        @Override
        public void gauge(OutboxGauge gauge, DoubleSupplier value) {
            gauges.put(gauge, value);
        }

        @Override
        public void queueState(List<ShardQueue> shards) {
            last = shards;
        }

        List<ShardQueue> lastState() {
            assertThat(last).as("queueState reported").isNotNull();
            return last;
        }

        double gauge(OutboxGauge gauge) {
            return gauges.get(gauge).getAsDouble();
        }
    }
}
