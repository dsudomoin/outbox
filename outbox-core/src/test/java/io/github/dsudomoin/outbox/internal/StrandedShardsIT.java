package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.testing.ByteReads;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Shards whose tick was deleted, repaired by the one admin call on a running relay: the rows of those shards come back
 * as new messages with their message ids, the others keep flowing, and nothing committed meanwhile is lost.
 */
class StrandedShardsIT {

    private static final int SHARDS = 8;
    private static final String TOPIC = "t";
    private static final List<Integer> STRANDED = List.of(1, 2);

    private final DataSource direct = TestDataSources.newDataSource();
    private final JdbcTransactions transactions = new JdbcTransactions(direct);
    private final TestOutboxConfig config = new TestOutboxConfig().shards(SHARDS);
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final Set<UUID> delivered = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Integer> times = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> nextKey = new HashMap<>();
    private OutboxRuntime writer;
    private OutboxRuntime relay;
    private TableNames names;

    @BeforeEach
    void bootstrap() throws Exception {
        writer = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()).shards(SHARDS).relayEnabled(false),
                transactions, record -> {}).build();
        writer.start();
        names = writer.schema().names();
    }

    @AfterEach
    void release() {
        pool.shutdownNow();
        if (relay != null) {
            relay.close();
        }
        writer.close();
    }

    @Test
    void repairsThemOnARunningFleetUnderWriters() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var strandedRows = new HashSet<UUID>();
        for (var shard : STRANDED) { // over a chunk of 1 000 rows on one of them
            strandedRows.addAll(sendMany(shard, shard == 1 ? 2500 : 50));
        }
        var others = sendMany(0, 10);
        deleteTheirTick();
        startRelay();
        await(() -> delivered.containsAll(others));
        assertThat(delivered).as("stranded: nothing of them moves").doesNotContainAnyElementsOf(strandedRows);

        var stop = new AtomicBoolean();
        var sent = ConcurrentHashMap.<UUID>newKeySet();
        var writers = pool.submit(() -> {
            int i = 0;
            while (!stop.get()) {
                sent.add(send(keyOfShard(i % SHARDS)));
                i++;
                Thread.sleep(1);
            }
            return null;
        });
        await(() -> sent.size() > 20);
        var flowing = sendMany(0, 1);
        await(() -> delivered.containsAll(flowing));

        long resent = admin().recoverStrandedShards();

        assertThat(resent).as("every row of theirs from before the call").isGreaterThanOrEqualTo(strandedRows.size());
        var afterIt = sendMany(1, 1);
        await(() -> delivered.containsAll(afterIt));
        stop.set(true);
        writers.get(10, TimeUnit.SECONDS);
        await(() -> delivered.containsAll(strandedRows) && delivered.containsAll(sent));
        assertThatThrownBy(() -> admin().recoverStrandedShards()).isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("no stranded shards");
    }

    @Test
    void aTransactionCommittingDuringTheCallIsDelivered() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var before = sendMany(1, 3);
        var openWhileTheCopyRuns = openSend(keyOfShard(1));
        var openPastTheCall = openSend(keyOfShard(2));
        // a tick that covers none of them: the cursors go to the newest tick, made after they took their xids
        makeATick();
        deleteTheirTick();
        startRelay();
        var startedInTheCall = new ArrayList<Future<UUID>>();
        var once = new AtomicBoolean();
        admin().afterChunk = c -> { // right after a copy, before the cursor moves
            if (once.compareAndSet(false, true)) {
                openWhileTheCopyRuns.commit();
                startedInTheCall.add(pool.submit(() -> send(keyOfShard(1))));
                try {
                    startedInTheCall.get(0).get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        };

        admin().recoverStrandedShards();
        openPastTheCall.commit();

        var sent = new ArrayList<>(before);
        sent.add(openWhileTheCopyRuns.id());
        sent.add(openPastTheCall.id());
        sent.add(startedInTheCall.get(0).get());
        await(() -> delivered.containsAll(sent));
        Thread.sleep(300); // a second delivery would come now
        assertThat(times.get(openWhileTheCopyRuns.id())).as("committed after the tick: only the relay's").isEqualTo(1);
        assertThat(times.get(openPastTheCall.id())).isEqualTo(1);
    }

    @Test
    void aRowCoveredByTheTickIsCopiedOnce() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var rows = sendMany(1, 5);
        makeATick();
        deleteTheirTick();
        startRelay();

        assertThat(admin().recoverStrandedShards()).isEqualTo(5);

        await(() -> delivered.containsAll(rows));
        Thread.sleep(300);
        assertThat(rows).allSatisfy(id -> assertThat(times.get(id)).isEqualTo(1));
        try (var c = direct.getConnection()) {
            assertThat(queryLong(c, "select count(*) from " + names.base() + " where shard = 1"))
                    .as("moved, not copied: a call that fails midway repeats none of them").isEqualTo(5);
        }
    }

    /** The rows go back into the outbox inside the database: a chunk of large ones never comes into the heap. */
    @Test
    void largeRowsMoveWithoutTheirPayloadsOrHeadersComingIntoJava() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var rows = sendMany(1, 10);
        try (var c = direct.getConnection()) {
            execute(c, "update " + names.base() + " set headers = decode(md5(id::text), 'hex'),"
                    + " payload = decode(repeat(md5(id::text), 4096), 'hex') where shard = 1"); // 64 KB each
        }
        var stored = contents(rows);
        makeATick();
        deleteTheirTick();
        relay = OutboxRuntime.builder(config, new JdbcTransactions(ByteReads.of(direct)), this::deliver).build();
        relay.start();

        long read = ByteReads.during(() -> assertThat(admin().recoverStrandedShards()).isEqualTo(10));

        assertThat(read).as("bytes read into Java: the keys at most").isLessThan(1024);
        assertThat(contents(rows)).as("byte for byte").isEqualTo(stored);
    }

    @Test
    void rowsOfTheOtherTablesStayThereAndTheirDuesComeAlong() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var inRetry = send(keyOfShard(1));
        var inParked = send(keyOfShard(1));
        var inDead = send(keyOfShard(1));
        var plain = send(keyOfShard(1));
        try (var c = direct.getConnection()) {
            var columns = "id, shard, message_id, topic, key, payload, headers, created_at";
            execute(c, "insert into " + names.retry() + " (" + columns + ", attempts, next_attempt_at, last_error)"
                    + " select " + columns + ", 1, now() + interval '1 hour', 'down' from " + names.base()
                    + " where message_id = '" + inRetry + "'");
            execute(c, "insert into " + names.parked() + " (" + columns + ", requeues, due_at) select " + columns
                    + ", 1, now() + interval '2 hours' from " + names.base() + " where message_id = '" + inParked
                    + "'");
            execute(c, "insert into " + names.dead() + " (" + columns + ", attempts, error, status) select " + columns
                    + ", 1, 'gone', 'kept' from " + names.base() + " where message_id = '" + inDead + "'");
        }
        makeATick();
        deleteTheirTick();
        startRelay();

        assertThat(admin().recoverStrandedShards()).as("the plain row alone").isEqualTo(1);

        await(() -> delivered.contains(plain));
        assertThat(delivered).as("each waits in its table").doesNotContain(inRetry, inParked, inDead);
        try (var c = direct.getConnection()) {
            var cursor = new Cursors(names, SHARDS).latest(c, 1).cursor();
            assertThat(cursor.retryDueAt()).isNotNull();
            assertThat(cursor.parkedDueAt()).isNotNull();
            assertThat(cursor.pausedUntil()).isNull();
            assertThat(cursor.targetTickId()).isNull();
        }
    }

    @Test
    void aKeyAnOpenTransactionHoldsIsWaitedForAndACallThatGivesUpGoesOnNextTime() throws Exception {
        config.swapLockTimeout(Duration.ofMillis(100));
        leaveTheOtherShardsOnANewerTick();
        var held = keyOfShard(1);
        var heldRow = send(held);
        var free = sendMany(1, 3);
        var open = openSend(held);
        makeATick();
        deleteTheirTick();
        startRelay();

        long freeId = idOf(free.get(0));
        long heldId = idOf(heldRow);

        assertThatThrownBy(() -> admin().recoverStrandedShards()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("busy, retry");

        assertThat(idOf(free.get(0))).as("a free row went").isNotEqualTo(freeId);
        assertThat(idOf(heldRow)).as("the held key's stayed").isEqualTo(heldId);
        open.commit();
        assertThat(admin().recoverStrandedShards()).as("the moved rows again, and the held key's").isGreaterThan(0);
        var sent = new ArrayList<>(free);
        sent.add(heldRow);
        sent.add(open.id());
        await(() -> delivered.containsAll(sent));
    }

    @Test
    void aShardMidBatchWhoseTargetTickIsGoneIsStuckUntilTheCallAndGetsOnlyItsNewRowsBack() throws Exception {
        config.maintenanceInterval(Duration.ofMillis(100));
        leaveTheOtherShardsOnANewerTick();
        var relayed = sendMany(1, 3);
        var cursors = new Cursors(names, SHARDS);
        var ticks = new Ticks(names, SHARDS);
        long from;
        long target;
        var rows = new ArrayList<UUID>();
        try (var c = direct.getConnection()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            from = ticks.latest(c).id();
            cursors.append(c, new Cursor(1, from, null, 0, null, 0, null, null)); // what is sent so far went through
            rows.addAll(sendMany(1, 5));
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            target = ticks.latest(c).id();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            cursors.append(c, new Cursor(1, from, target, idOf(rows.get(0)), null, 0, null, null));
            execute(c, "delete from " + names.tick() + " where id = " + target);
            execute(c, "delete from " + names.checkpoint());
        }
        var after = new ArrayList<UUID>();
        String log;
        try (var err = new CapturedStderr()) {
            startRelay();
            after.addAll(sendMany(1, 1));
            Thread.sleep(2000); // steps and maintenance runs: nothing moves it by itself
            log = err.text();
        }
        assertThat(delivered).doesNotContainAnyElementsOf(rows).doesNotContainAnyElementsOf(after);
        assertThat(log).contains("no tick " + target);
        assertThat(log.contains("start from ticks [" + target + "]")).as("the ERROR names the tick").isTrue();

        assertThat(admin().recoverStrandedShards()).as("the rows its tick does not see, read or not").isEqualTo(6);

        rows.addAll(after);
        await(() -> delivered.containsAll(rows));
        Thread.sleep(300);
        assertThat(delivered).as("what its tick sees went through before").doesNotContainAnyElementsOf(relayed);
        assertThatThrownBy(() -> admin().recoverStrandedShards()).hasMessageStartingWith("no stranded shards");
    }

    @Test
    void aHealthyShardIdleBehindAGoneTickKeepsFlowing() throws Exception {
        // the stranded shard on tick X, with a row behind it, so busy at a checkpoint at K; the healthy one idle, its
        // cursor row on the bootstrap tick, below X, and its effective tick the checkpoint's
        int stranded = 1;
        int healthy = 3;
        var cursors = new Cursors(names, SHARDS);
        var ticks = new Ticks(names, SHARDS);
        var maintenance = writer.maintenance();
        long gone;
        try (var c = direct.getConnection()) {
            sendMany(stranded, 1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            gone = ticks.latest(c).id();
            cursors.append(c, new Cursor(stranded, gone, null, 0, null, 0, null, null));
            sendMany(stranded, 1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
        }
        assertThat(maintenance.runOnce()).isTrue();
        try (var c = direct.getConnection()) {
            execute(c, "delete from " + names.tick() + " where id = " + gone);
        }
        var row = sendMany(healthy, 1); // busy at the next read
        makeATick();

        assertThat(maintenance.runOnce()).isTrue();

        try (var c = direct.getConnection()) {
            assertThat(maintenance.strandedShards(c)).as("the checkpoint strands no other shard")
                    .containsOnlyKeys(stranded);
        }
        startRelay();
        await(() -> delivered.containsAll(row));
        Thread.sleep(300);
        assertThat(times.get(row.get(0))).as("once: the relay's, no repair's").isEqualTo(1);
    }

    @Test
    void aFailedStepLeavesItsConnectionInAutocommit() throws Exception {
        var atClose = new ConcurrentLinkedQueue<Boolean>();
        leaveTheOtherShardsOnANewerTick();
        sendMany(1, 2);
        makeATick();
        deleteTheirTick();
        relay = OutboxRuntime.builder(config, new JdbcTransactions(recordingAutocommit(direct, atClose)),
                this::deliver).build();
        relay.start();

        await(() -> atClose.size() > 10);

        assertThat(atClose).as("as every pass closes it, a stranded shard's step failing in each").containsOnly(true);
    }

    /**
     * The shard with the oldest tick goes first in a round, and a failing shard's tick stays old. With a new tick, so a
     * new round, after every step it would be all a worker ever steps.
     */
    @Test
    void aFailingShardHoldsUpNoNeighbourWithANewRoundAfterEveryStep() throws Exception {
        config.workers(1).idlePollInterval(Duration.ofMillis(1)).minTickInterval(Duration.ZERO);
        leaveTheOtherShardsOnANewerTick();
        sendMany(1, 2);
        makeATick();
        deleteTheirTick();
        var failures = new AtomicInteger();
        relay = OutboxRuntime.builder(config, transactions, this::deliver).telemetry(new OutboxTelemetry() {
            @Override
            public void relayFailed(Throwable error) {
                failures.incrementAndGet();
            }
        }).build();
        relay.start();
        await(() -> failures.get() > 2);

        var sent = sendMany(0, 5);

        await(() -> delivered.containsAll(sent));
        int before = failures.get();
        await(() -> failures.get() > before + 2); // and it is still tried: a repair is not left waiting
    }

    @Test
    void aFailedStepCostsAHealthyShardNothingOnARepeatableReadSession() throws Exception {
        var repeatableRead = new PGSimpleDataSource();
        repeatableRead.setUrl(TestDataSources.jdbcUrl());
        repeatableRead.setUser(TestDataSources.user());
        repeatableRead.setPassword(TestDataSources.password());
        repeatableRead.setOptions("-c default_transaction_isolation=repeatable\\ read");
        // a tick attempt, and with it a read of the latest tick, between any two steps of a pass
        config.idlePollInterval(Duration.ofMillis(1)).minTickInterval(Duration.ZERO);
        leaveTheOtherShardsOnANewerTick();
        sendMany(1, 2);
        makeATick();
        deleteTheirTick();
        var failures = new ConcurrentLinkedQueue<String>();
        relay = OutboxRuntime.builder(config, new JdbcTransactions(repeatableRead), this::deliver)
                .telemetry(new OutboxTelemetry() {
                    @Override
                    public void relayFailed(Throwable error) {
                        failures.add(error.toString());
                    }
                }).build();
        relay.start();
        var sent = new ArrayList<UUID>();
        for (int i = 0; i < 300; i++) {
            sent.addAll(sendMany(i % 2 == 0 ? 0 : 3, 1));
            Thread.sleep(5);
        }
        await(() -> delivered.containsAll(sent));

        assertThat(failures).as("the stranded shard's own failures, and only those").isNotEmpty()
                .noneMatch(failure -> failure.contains("before any query"));
    }

    /** {@code source} whose connections add to {@code atClose}, closed by a worker: whether they are in autocommit. */
    private static DataSource recordingAutocommit(DataSource source, Queue<Boolean> atClose) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (ds, method, args) -> {
                    var result = method.invoke(source, args);
                    return !method.getName().equals("getConnection") ? result : Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, (c, call, values) -> {
                                if (call.getName().equals("close")
                                        && Thread.currentThread().getName().startsWith("outbox-relay-")) {
                                    atClose.add(((Connection) result).getAutoCommit());
                                }
                                try {
                                    return call.invoke(result, values);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            });
                });
    }

    @Test
    void aShardThatAStepHoldsLeavesTheRunWithoutACheckpointAndAMidBatchOneKeepsItsBatch() throws Exception {
        int stranded = 1;
        int idle = 3;
        int midBatch = 5;
        var cursors = new Cursors(names, SHARDS);
        var ticks = new Ticks(names, SHARDS);
        var maintenance = writer.maintenance();
        long gone;
        try (var c = direct.getConnection()) {
            sendMany(stranded, 1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            gone = ticks.latest(c).id();
            cursors.append(c, new Cursor(stranded, gone, null, 0, null, 0, null, null));
            sendMany(stranded, 1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
        }
        assertThat(maintenance.runOnce()).isTrue();
        long oldTick = cursors(cursors, midBatch).tickId();
        var checkpoint = "select tick_id from " + names.checkpoint();
        long before;
        long target;
        long lastId;
        var idleRow = sendMany(idle, 1);
        var read = sendMany(midBatch, 1); // delivered before, in the batch in progress
        var unread = sendMany(midBatch, 1);
        try (var c = direct.getConnection()) {
            before = queryLong(c, checkpoint);
            execute(c, "delete from " + names.tick() + " where id = " + gone);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            target = ticks.latest(c).id();
            lastId = idOf(read.get(0));
            // written by a step that read its cursor before the checkpoint: below its effective tick
            cursors.append(c, new Cursor(midBatch, oldTick, target, lastId, null, 0, null, null));
        }

        try (var holder = direct.getConnection()) { // what a step of the idle shard holds
            holder.setAutoCommit(false);
            try (var ps = holder.prepareStatement("select pg_advisory_xact_lock(?, ?)")) {
                ps.setInt(1, names.internalNamespace());
                ps.setInt(2, TableNames.LOCK_SHARD_BASE + idle);
                ps.execute();
            }

            assertThat(maintenance.runOnce()).isTrue();

            try (var c = direct.getConnection()) {
                assertThat(queryLong(c, checkpoint)).as("no checkpoint this run").isEqualTo(before);
                assertThat(maintenance.strandedShards(c)).as("and no other shard stranded").containsOnlyKeys(stranded);
                var anchored = cursors.latest(c, midBatch).cursor();
                assertThat(anchored.tickId()).as("at its effective tick").isEqualTo(before);
                assertThat(anchored.targetTickId()).as("its batch goes on").isEqualTo(target);
                assertThat(anchored.lastId()).isEqualTo(lastId);
            }
            holder.rollback();
        }

        assertThat(maintenance.runOnce()).isTrue();

        try (var c = direct.getConnection()) {
            assertThat(queryLong(c, checkpoint)).as("the next run writes it").isGreaterThan(before);
            assertThat(maintenance.strandedShards(c)).containsOnlyKeys(stranded);
            assertThat(cursors.latest(c, idle).cursor().tickId()).as("anchored in its turn").isEqualTo(before);
        }
        startRelay();
        await(() -> delivered.containsAll(idleRow) && delivered.containsAll(unread));
        Thread.sleep(300);
        assertThat(times.get(idleRow.get(0))).isEqualTo(1);
        assertThat(times.get(unread.get(0))).isEqualTo(1);
        assertThat(delivered).as("past its last id").doesNotContainAnyElementsOf(read);
    }

    private Cursor cursors(Cursors cursors, int shard) throws Exception {
        try (var c = direct.getConnection()) {
            return cursors.latest(c, shard).cursor();
        }
    }

    @Test
    void refusesWhenNoShardIsStranded() throws Exception {
        startRelay();

        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> admin().recoverStrandedShards()).isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith("no stranded shards");
            assertThat(err.text()).doesNotContain("recoverStrandedShards");
        }
    }

    @Test
    void refusesDuringAnXidRegressionHalt() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var rows = sendMany(1, 2);
        makeATick();
        deleteTheirTick();
        startRelay();
        long id = idOf(rows.get(0));
        try (var c = direct.getConnection(); var ps = c.prepareStatement(XidRecovery.haltInsert(names))) {
            ps.setLong(1, 1);
            ps.executeUpdate();
        }

        assertThatThrownBy(() -> admin().recoverStrandedShards()).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recover the xid regression first");

        assertThat(idOf(rows.get(0))).as("nothing moved").isEqualTo(id);
    }

    /** A retry while the first call still runs (say after a gateway timeout) is told the holder may be that call. */
    @Test
    void aSecondCallWhileOneRunsSaysTheLockMayBeAnotherRecoveryCall() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var rows = sendMany(1, 5);
        makeATick();
        deleteTheirTick();
        startRelay();
        var inChunk = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        admin().afterChunk = c -> {
            inChunk.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        };
        var first = pool.submit(() -> admin().recoverStrandedShards());
        assertThat(inChunk.await(10, TimeUnit.SECONDS)).isTrue();

        admin().maintenanceWait = Duration.ofMillis(200);
        try {
            assertThatThrownBy(() -> admin().recoverStrandedShards()).isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith("maintenance or another outbox admin recovery call is running, retry");
        } finally {
            release.countDown();
        }

        assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(5);
        await(() -> delivered.containsAll(rows));
    }

    @Test
    void aHaltRecordedDuringTheCallLeavesDuplicatesAtMost() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        var rows = new ArrayList<UUID>(sendMany(1, 1500)); // two chunks at least: the regression comes between them
        rows.addAll(sendMany(2, 50));
        makeATick();
        deleteTheirTick();
        startRelay();
        var once = new AtomicBoolean();
        admin().afterChunk = c -> {
            if (once.compareAndSet(false, true)) {
                try (var other = direct.getConnection()) {
                    long xmax = queryLong(other, "select pg_snapshot_xmax(pg_current_snapshot())::text::bigint")
                            + 1_000_000_000L;
                    execute(other, "insert into " + names.tick() + " (snapshot, created_at) values ('" + xmax + ":"
                            + xmax + ":'::pg_snapshot, now() - interval '1 minute')");
                    try (var ps = other.prepareStatement(XidRecovery.haltInsert(names))) {
                        ps.setLong(1, xmax);
                        ps.executeUpdate();
                    }
                }
            }
        };

        admin().recoverStrandedShards(); // not re-checked: it goes on, and all its moved rows stay rows
        admin().afterChunk = null;
        admin().recoverXidRegression();

        await(() -> delivered.containsAll(rows));
    }

    @Test
    void logsItsResultInOneLine() throws Exception {
        leaveTheOtherShardsOnANewerTick();
        sendMany(1, 2);
        makeATick();
        deleteTheirTick();
        startRelay();

        try (var err = new CapturedStderr()) {
            admin().recoverStrandedShards();

            assertThat(err.text()).contains("outbox admin recoverStrandedShards() on " + names.base()
                    + " returned 2, the rows re-sent, for shards [1, 2]");
        }
    }

    /** The message id, topic, key and the md5 of the payload and headers of each row of {@code messageIds}. */
    private Set<String> contents(List<UUID> messageIds) throws Exception {
        var all = new HashSet<String>();
        try (var c = direct.getConnection(); var ps = c.prepareStatement("select concat_ws(' ', message_id, topic,"
                + " encode(key, 'hex'), md5(payload), md5(headers)) from " + names.base()
                + " where message_id = any(?)")) {
            ps.setArray(1, c.createArrayOf("uuid", messageIds.toArray()));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    all.add(rs.getString(1));
                }
            }
        }
        return all;
    }

    private long idOf(UUID messageId) throws Exception {
        try (var c = direct.getConnection()) {
            return queryLong(c, "select id from " + names.base() + " where message_id = '" + messageId + "'");
        }
    }

    private PgOutboxAdmin admin() {
        return (PgOutboxAdmin) relay.admin();
    }

    private void startRelay() {
        relay = OutboxRuntime.builder(config, transactions, this::deliver).build();
        relay.start();
    }

    private void deliver(OutboxRecord record) {
        times.merge(record.messageId(), 1, Integer::sum);
        delivered.add(record.messageId());
    }

    /** Every shard but {@link #STRANDED} moves off the bootstrap tick, before any of them has a row. */
    private void leaveTheOtherShardsOnANewerTick() throws Exception {
        try (var c = direct.getConnection()) {
            var ticks = new Ticks(names, SHARDS);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            long latest = ticks.latest(c).id();
            var cursors = new Cursors(names, SHARDS);
            for (int shard = 0; shard < SHARDS; shard++) {
                if (!STRANDED.contains(shard)) {
                    cursors.append(c, new Cursor(shard, latest, null, 0, null, 0, null, null));
                }
            }
        }
    }

    private void makeATick() throws Exception {
        try (var c = direct.getConnection()) {
            assertThat(new Ticks(names, SHARDS).make(c, Duration.ZERO)).isTrue();
        }
    }

    /** The tick the latest cursor rows of {@link #STRANDED} stand on, and the checkpoint. */
    private void deleteTheirTick() throws Exception {
        try (var c = direct.getConnection()) {
            long tick = new Cursors(names, SHARDS).latest(c, STRANDED.get(0)).cursor().tickId();
            makeATick();
            execute(c, "delete from " + names.tick() + " where id = " + tick);
            execute(c, "delete from " + names.checkpoint());
        }
    }

    private List<UUID> sendMany(int shard, int rows) {
        var ids = new ArrayList<UUID>();
        for (int sent = 0; sent < rows; sent += 500) { // as many key locks as a test's Postgres has room for
            var messages = new ArrayList<OutboxMessage>();
            for (int i = 0; i < Math.min(500, rows - sent); i++) {
                messages.add(OutboxMessage.of(TOPIC, keyOfShard(shard), "p".getBytes(UTF_8)));
            }
            ids.addAll(transactions.inTx(c -> writer.outbox().sendAll(messages)));
        }
        return ids;
    }

    private UUID send(String key) {
        return transactions.inTx(c -> writer.outbox().send(OutboxMessage.of(TOPIC, key, "p".getBytes(UTF_8))));
    }

    /** A new key hashing to {@code shard}. */
    private synchronized String keyOfShard(int shard) {
        int i = nextKey.getOrDefault(shard, 0);
        while (true) {
            var key = shard + "-" + i++;
            if (Hashing.shard(Hashing.keyHash(TOPIC, key.getBytes(UTF_8)), SHARDS) == shard) {
                nextKey.put(shard, i);
                return key;
            }
        }
    }

    /** A send on a thread of its own, its transaction open until {@link #commit()}. */
    private OpenSend openSend(String key) throws Exception {
        var sent = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var done = pool.submit(() -> transactions.inTx(c -> {
            var id = writer.outbox().send(OutboxMessage.of(TOPIC, key, "p".getBytes(UTF_8)));
            sent.countDown();
            try {
                assertThat(commit.await(30, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            return id;
        }));
        assertThat(sent.await(10, TimeUnit.SECONDS)).isTrue();
        return new OpenSend(commit, done);
    }

    private record OpenSend(CountDownLatch release, Future<UUID> done) {

        void commit() {
            release.countDown();
            id();
        }

        UUID id() {
            try {
                return done.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
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
