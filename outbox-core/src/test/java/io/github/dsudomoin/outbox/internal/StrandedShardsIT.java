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
import io.github.dsudomoin.outbox.internal.Cursors.Cursor;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    void refusesWhenNoShardIsStranded() throws Exception {
        startRelay();

        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> admin().recoverStrandedShards()).isInstanceOf(IllegalStateException.class)
                    .hasMessageStartingWith("no stranded shards");
            assertThat(err.text()).doesNotContain("recoverStrandedShards");
        }
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
