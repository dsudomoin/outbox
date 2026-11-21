package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.Handlers.eachRecord;
import static io.github.dsudomoin.outbox.testing.Intercepting.intercepting;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.AdminRows;
import io.github.dsudomoin.outbox.testing.Await;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * The relay's database stops answering: the network goes silent, connections staying open and nothing arriving, or a
 * lock holds its reads. With timeouts on the DataSource every wait ends in a failure the relay survives; without them,
 * and under the lock, the shutdown stays bounded all the same.
 */
class NetworkBlackholeIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final DataSource DIRECT = TestDataSources.newDataSource();

    private final Blackhole hole = new Blackhole();
    private final List<AutoCloseable> closing = new ArrayList<>();
    private final JdbcTransactions direct = new JdbcTransactions(DIRECT);

    NetworkBlackholeIT() throws IOException {}

    @AfterEach
    void release() throws Exception {
        hole.open();
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
        hole.close();
    }

    @Test
    void withTimeoutsTheRelayFailsWithABoundAndLosesNothing() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(500));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var failures = new AtomicInteger();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(throughTheHole(3)),
                record -> delivered.add(record.messageId())).telemetry(new OutboxTelemetry() {
                    @Override
                    public void relayFailed(Throwable error) {
                        failures.incrementAndGet();
                    }
                }).build();
        closing.add(relay);
        relay.start();
        var writer = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()).relayEnabled(false)
                .maintenanceInterval(Duration.ofMillis(500)), direct, record -> {}).build();
        closing.add(writer);
        writer.start();
        var sent = new HashSet<UUID>();
        sent.add(send(writer, direct, "before"));
        Await.until(TIMEOUT, () -> delivered.containsAll(sent));

        hole.shut();
        sent.add(send(writer, direct, "during"));
        int failed = failures.get();
        Await.until(TIMEOUT, () -> failures.get() > failed);
        assertThat(delivered.containsAll(sent)).as("what was sent during it waits").isFalse();

        hole.open();
        sent.add(send(writer, direct, "after"));
        Await.until(TIMEOUT, () -> delivered.containsAll(sent));
        assertThat(relay.relay().running()).isTrue();
        awaitBeat(relay);
    }

    @Test
    void aLockHeldOnTheCursorsHoldsTheRelayOnlyWhileItLasts() throws Exception {
        var config = new TestOutboxConfig();
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(DIRECT);
        var runtime = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(runtime);
        runtime.start();
        var sent = new HashSet<UUID>();
        sent.add(send(runtime, transactions, "before"));
        Await.until(TIMEOUT, () -> delivered.containsAll(sent));

        try (var locker = DIRECT.getConnection()) {
            locker.setAutoCommit(false);
            TestSql.execute(locker, "lock table " + runtime.schema().names().cursor() + " in access exclusive mode");
            sent.add(send(runtime, transactions, "during"));
            Thread.sleep(1000);
            assertThat(delivered.containsAll(sent)).as("what was sent during it waits").isFalse();
            locker.rollback();
        }

        Await.until(TIMEOUT, () -> delivered.containsAll(sent));
        assertThat(runtime.relay().running()).isTrue();
    }

    @Test
    void aLockHeldOnTheCursorsDoesNotHoldTheClose() throws Exception {
        var config = new TestOutboxConfig().shutdownTimeout(Duration.ofMillis(100));
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {}).build();
        runtime.start();

        try (var locker = DIRECT.getConnection()) {
            locker.setAutoCommit(false);
            TestSql.execute(locker, "lock table " + runtime.schema().names().cursor() + " in access exclusive mode");
            Thread.sleep(500); // the workers wait on the lock by now

            CompletableFuture.runAsync(runtime::close).get(60, TimeUnit.SECONDS);
            locker.rollback();
        }
    }

    @Test
    void withoutTimeoutsTheCloseStaysBounded() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(200))
                .idlePollInterval(Duration.ofMillis(20)).shutdownTimeout(Duration.ofMillis(100));
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(throughTheHole(0)), record -> {}).build();
        runtime.start();

        hole.shut();
        Thread.sleep(500); // the workers and the next beat are stuck on the silent sockets by now

        CompletableFuture.runAsync(runtime::close).get(60, TimeUnit.SECONDS);
    }

    @Test
    void aSilentDropHealsByItselfOnTheDefaultDataSource() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 0))),
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        var writer = writer(config.table());
        var sent = new HashSet<UUID>();
        sent.add(send(writer, direct, "before"));
        Await.until(TIMEOUT, () -> delivered.containsAll(sent));

        hole.forget(); // each worker's next statement, on its pooled connection, waits on a silent socket
        sent.add(send(writer, direct, "after"));

        Await.until(TIMEOUT, () -> delivered.containsAll(sent));
        assertThat(relay.relay().running()).isTrue();
    }

    @Test
    void aSocketTimeoutOfTheDataSourceWins() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofHours(1));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 1))),
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        var writer = writer(config.table());

        hole.forget();
        var after = send(writer, direct, "after");

        // within the await, so by the 1 s of the DataSource, not by the hour
        Await.until(TIMEOUT, () -> delivered.contains(after));
    }

    @Test
    void everyThreadOfTheLibraryGetsTheNetworkTimeout() throws Exception {
        var calls = new ConcurrentLinkedQueue<String>();
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(9))
                .maintenanceInterval(Duration.ofMillis(100));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(recordingTimeouts(DIRECT, calls));
        var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        assertThat(calls).as("the start's: its statements may run for their statement_timeout")
                .contains("Test worker:9000", "Test worker:65000");
        calls.removeIf(call -> call.startsWith("Test worker"));

        var sent = send(relay, transactions, "k");
        Await.until(TIMEOUT, () -> delivered.contains(sent));
        Thread.sleep(500); // some maintenance runs, dead-letter passes and beats

        assertThat(calls).allMatch(call -> call.endsWith(":9000"));
        assertThat(calls.stream().map(call -> call.replaceAll("-[0-9]+:.*", "").replaceAll(":.*", "")).toList())
                .contains("outbox-relay", "outbox-maintenance", "outbox-dead-letters", "outbox-heartbeat");
    }

    @Test
    void aZeroNetworkTimeoutSetsNone() throws Exception {
        var calls = new ConcurrentLinkedQueue<String>();
        var config = new TestOutboxConfig().networkTimeout(Duration.ZERO);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(recordingTimeouts(DIRECT, calls));
        var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();

        var sent = send(relay, transactions, "k");
        Await.until(TIMEOUT, () -> delivered.contains(sent));

        assertThat(calls).isEmpty();
    }

    @Test
    void aDrainOnASilentSocketFailsItsNetworkTimeoutLater() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).networkTimeout(Duration.ofSeconds(1));
        var transactions = new JdbcTransactions(intercepting(throughTheHole(3, 0), (c, call, args) -> {
            if (call.getName().equals("getAutoCommit") && Thread.currentThread().getName().equals("test-drain")) {
                hole.forget(); // the tick's first call on its connection
            }
        }));
        var relay = OutboxRuntime.builder(config, transactions, record -> {}).build();
        closing.add(relay);
        relay.start();

        assertThatThrownBy(() -> draining(relay).get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
    }

    @Test
    void maintenanceSurvivesASilentDrop() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 0))), record -> {}).build();
        closing.add(relay);
        relay.start();
        var names = relay.schema().names();
        Await.until(TIMEOUT, () -> lastSuccess(names.maintenance()) > 0);

        hole.forget();

        long seen = lastSuccess(names.maintenance());
        // a run cut off holds the maintenance lock till idle_in_transaction_session_timeout (60 s) ends its holder
        Await.until(Duration.ofSeconds(90), () -> lastSuccess(names.maintenance()) > seen);
    }

    @Test
    void maintenanceOnOwnerConnectionsSurvivesASilentDrop() throws Exception {
        var table = new TestOutboxConfig();
        var bootstrap = OutboxRuntime.builder(table, new JdbcTransactions(DIRECT), record -> {}).build();
        bootstrap.start();
        bootstrap.close();
        var config = new TestOutboxConfig().table(table.table()).schemaBootstrap(false)
                .networkTimeout(Duration.ofSeconds(1)).maintenanceInterval(Duration.ofMillis(100));
        var owner = pool(throughTheHole(3, 0));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {})
                .ownerConnections(owner::getConnection).build();
        closing.add(relay);
        relay.start();
        var names = relay.schema().names();
        Await.until(TIMEOUT, () -> lastSuccess(names.maintenance()) > 0);

        hole.forget();

        long seen = lastSuccess(names.maintenance());
        // a run cut off holds the maintenance lock till idle_in_transaction_session_timeout (60 s) ends its holder
        Await.until(Duration.ofSeconds(90), () -> lastSuccess(names.maintenance()) > seen);
    }

    @Test
    void inboxMaintenanceSurvivesASilentDrop() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var inbox = InboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 0)))).build();
        closing.add(inbox);
        inbox.start();
        Thread.sleep(150); // past a run, so it waits on its next statement

        hole.forget();

        var past = config.inbox().table() + "_t2020010100"; // a run drops it, far past retention and empty
        try (var c = DIRECT.getConnection()) {
            TestSql.execute(c, "create table " + past + " (like " + config.inbox().table() + " including defaults)");
        }
        // a run cut off holds the maintenance lock till idle_in_transaction_session_timeout (60 s) ends its holder
        Await.until(Duration.ofSeconds(90), () -> {
            try (var c = DIRECT.getConnection()) {
                return TestSql.queryString(c, "select to_regclass(?)::text", past) == null;
            }
        });
    }

    @Test
    void aStatementTheLibraryBoundsItselfOutlastsTheNetworkTimeout() throws Exception {
        // a recovery's wait for a lock, under its own timeouts, longer than the bound of the connection it runs on
        var config = new TestOutboxConfig().relayEnabled(false).networkTimeout(Duration.ofSeconds(1));
        var transactions = new JdbcTransactions(DIRECT);
        var runtime = OutboxRuntime.builder(config, transactions, record -> {}).build();
        closing.add(runtime);
        runtime.start();
        var names = runtime.schema().names();
        send(runtime, transactions, "k");
        new AdminRows(DIRECT::getConnection, names).forgeRegression();
        try (var holder = DIRECT.getConnection()) {
            holder.setAutoCommit(false);
            TestSql.execute(holder, "lock table " + names.cursor() + " in access exclusive mode");
            var release = rollbackAfter(holder, 2500);

            assertThat(runtime.admin().recoverXidRegression()).as("re-sent, not cut at 1 s").isEqualTo(1);

            release.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void anOrphanRestoreOutlastsTheNetworkTimeout() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).networkTimeout(Duration.ofSeconds(1));
        var runtime = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {}).build();
        closing.add(runtime);
        runtime.start();
        var names = runtime.schema().names();
        var orphan = names.base() + "_x1000";
        try (var c = DIRECT.getConnection()) {
            TestSql.execute(c, "create table " + orphan + " (like " + names.base() + " including defaults)");
            TestSql.execute(c, "insert into " + orphan + " (id, shard, xid, message_id, topic)"
                    + " values (1, 0, '1000', gen_random_uuid(), 't')");
        }
        try (var holder = DIRECT.getConnection()) {
            holder.setAutoCommit(false);
            TestSql.execute(holder, "lock table " + orphan + " in access exclusive mode");
            var release = rollbackAfter(holder, 2500);

            assertThat(runtime.admin().restoreOrphan(orphan, 0)).as("moved, not cut at 1 s").isEqualTo(1);

            release.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void theDeadLetterPassSurvivesASilentDrop() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        Set<UUID> handed = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(pool(throughTheHole(3, 0)));
        var relay = OutboxRuntime.builder(config, transactions,
                eachRecord(r -> Outcome.permanent(new IOException("never")))).deadLetterHandler(letters -> {
            letters.forEach(letter -> handed.add(letter.messageId()));
            return letters.stream().map(letter -> DeadLetterDecision.keep()).toList();
        }).build();
        closing.add(relay);
        relay.start();
        var writer = writer(config.table());
        var before = send(writer, direct, "before");
        Await.until(TIMEOUT, () -> handed.contains(before));

        Thread.sleep(150); // past its run, so it waits on its next statement
        hole.forget();
        var after = send(writer, direct, "after");

        Await.until(TIMEOUT, () -> handed.contains(after));
    }

    /** Cut off while the hook runs: the same letter comes back, once the server ends the abandoned claim. */
    @Test
    void aLetterCutOffInsideItsHookIsHandedAgain() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1)).deadLettersTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100)).retry(Duration.ofMillis(100), Duration.ofSeconds(1), 1);
        var calls = new AtomicInteger();
        var inHook = new CountDownLatch(1);
        var transactions = new JdbcTransactions(pool(throughTheHole(3, 0)));
        var relay = OutboxRuntime.builder(config, transactions,
                eachRecord(r -> Outcome.permanent(new IOException("never")))).deadLetterHandler(letters -> {
            if (calls.getAndIncrement() == 0) {
                inHook.countDown();
                try {
                    Thread.sleep(Long.MAX_VALUE); // a hanging call: past the hook's timeout
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return letters.stream().map(letter -> DeadLetterDecision.keep()).toList();
        }).build();
        closing.add(relay);
        relay.start();
        var writer = writer(config.table());
        send(writer, direct, "cut");
        assertThat(inHook.await(30, TimeUnit.SECONDS)).isTrue();

        hole.forget();

        var dead = relay.schema().names().dead();
        // the abandoned claim holds the letter for the hook's timeout + 30 s
        Await.until(Duration.ofSeconds(60), () -> {
            try (var c = DIRECT.getConnection()) {
                return "kept".equals(TestSql.queryString(c, "select status from " + dead));
            }
        });
    }

    @Test
    void theHeartbeatOnItsOwnPoolSurvivesASilentDrop() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var own = pool(throughTheHole(3, 0));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {})
                .heartbeatConnections(own::getConnection).build();
        closing.add(relay);
        relay.start();

        hole.forget();

        awaitBeat(relay);
    }

    @Test
    void theHeartbeatOnTheApplicationsPoolAsItsFallbackSurvivesASilentDrop() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(pool(throughTheHole(3, 0))), record -> {})
                .heartbeatConnections(() -> {
                    throw new SQLException("its own pool is down");
                }).build();
        closing.add(relay);
        relay.start();

        hole.forget();

        awaitBeat(relay);
    }

    @Test
    void aBeatOnASourceWithALongSocketTimeoutIsCutAtItsBound() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofSeconds(1)); // the beat's bound is 10 s
        var armed = new AtomicBoolean();
        var source = intercepting(pool(throughTheHole(3, 60)), (c, call, args) -> {
            if (call.getName().equals("prepareStatement") && Thread.currentThread().getName().equals("outbox-heartbeat")
                    && armed.compareAndSet(true, false)) {
                hole.forget(); // mid-beat, past the borrow's validation
            }
        });
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {})
                .heartbeatConnections(source::getConnection).build();
        closing.add(relay);
        relay.start();
        armed.set(true);
        Await.until(TIMEOUT, () -> !armed.get());

        awaitBeat(Duration.ofSeconds(20), relay); // not the 60 s socketTimeout
    }

    @Test
    void aBeatCutBeforeItsCommitDoesNotHoldTheInstancesRow() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var armed = new AtomicBoolean();
        var source = intercepting(pool(throughTheHole(3, 0)), (c, call, args) -> {
            if (call.getName().equals("commit") && Thread.currentThread().getName().equals("outbox-heartbeat")
                    && armed.compareAndSet(true, false)) {
                hole.forget(); // its commit goes nowhere: the backend sits in its transaction, the row locked
            }
        });
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(source), record -> {}).build();
        closing.add(relay);
        relay.start();
        armed.set(true);
        Await.until(TIMEOUT, () -> !armed.get());

        Thread.sleep(1500); // the client has given that beat up
        awaitBeat(relay);
    }

    @Test
    void anAnchorCutBeforeItsCommitDoesNotHoldItsShards() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100)).cursorAnchorInterval(Duration.ofMillis(100));
        var locking = new AtomicBoolean();
        var cut = new AtomicBoolean();
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var source = intercepting(pool(throughTheHole(3, 0)), (c, call, args) -> {
            if (!Thread.currentThread().getName().equals("outbox-maintenance")) {
                return;
            }
            if (call.getName().equals("prepareStatement") && args[0].toString().contains("pg_try_advisory_xact_lock")) {
                locking.set(true);
            } else if (call.getName().equals("commit") && locking.get() && cut.compareAndSet(false, true)) {
                hole.forget(); // the anchors' commit goes nowhere: the backend sits there, holding the shards' locks
            }
        });
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(source),
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        var writer = writer(config.table());
        Await.until(TIMEOUT, cut::get);

        var sent = new HashSet<UUID>();
        for (int i = 0; i < 64; i++) { // on every shard
            sent.add(send(writer, direct, "k" + i));
        }

        Await.until(TIMEOUT, () -> delivered.containsAll(sent));
    }

    @Test
    void aPoolWithoutASocketBoundGetsOneWarnAtStart() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(100));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(throughTheHole(3, 0));
        try (var err = new CapturedStderr()) {
            var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
            closing.add(relay);
            relay.start();
            var sent = send(relay, transactions, "k");
            Await.until(TIMEOUT, () -> delivered.contains(sent));
            Thread.sleep(300); // passes, runs and beats, each on a connection of that pool

            assertThat(err.text().lines().filter(line -> line.contains("WARN")
                    && line.contains("business transaction"))).hasSize(1);
        }
    }

    @Test
    void aPoolWithASocketBoundGetsNoWarn() throws Exception {
        try (var err = new CapturedStderr()) {
            var relay = OutboxRuntime.builder(new TestOutboxConfig(), new JdbcTransactions(throughTheHole(3, 30)),
                    record -> {}).build();
            closing.add(relay);
            relay.start();

            assertThat(err.text()).doesNotContain("business transaction");
        }
    }

    @Test
    void aConnectionThatTellsNoBoundGetsNoWarnAndStarts() throws Exception {
        try (var err = new CapturedStderr()) {
            var relay = OutboxRuntime.builder(new TestOutboxConfig(), new JdbcTransactions(intercepting(DIRECT,
                    (c, call, args) -> {
                        if (call.getName().equals("getNetworkTimeout")) {
                            throw new SQLFeatureNotSupportedException("no such thing here");
                        }
                    })), record -> {}).build();
            closing.add(relay);
            relay.start();

            assertThat(err.text()).doesNotContain("business transaction");
        }
    }

    @Test
    void anAdminCallOnASilentSocketFails() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).networkTimeout(Duration.ofSeconds(1));
        var transactions = new JdbcTransactions(intercepting(throughTheHole(3, 0), (c, call, args) -> {
            if (Thread.currentThread().getName().equals("test-admin")) {
                hole.forget();
            }
        }));
        var runtime = OutboxRuntime.builder(config, transactions, record -> {}).build();
        closing.add(runtime);
        runtime.start();
        // no read of its own 60 s statement timeout, which a status would be given
        var call = new FutureTask<>(() -> runtime.admin().abandonWaiting("t", null, "no such", false));
        var thread = new Thread(call, "test-admin");
        thread.setDaemon(true);
        thread.start();

        assertThatThrownBy(() -> call.get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
    }

    @Test
    void anIndexBuildLeftBehindByASilentDropIsLetGoOnceItsBackendStopped() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var bootstrap = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {}).build();
        bootstrap.start();
        bootstrap.close();
        var names = bootstrap.schema().names();
        var index = names.dead() + "_due";
        try (var c = DIRECT.getConnection(); var holder = DIRECT.getConnection()) {
            TestSql.execute(c, "drop index " + index);
            TestSql.execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                    + " values (1, 0, gen_random_uuid(), 't', now(), 1)"); // so maintenance builds it, not the start
            // a snapshot the concurrent build waits for, so it runs longer than any bound
            holder.setAutoCommit(false);
            holder.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            TestSql.queryLong(holder, "select 1");
            var relay = OutboxRuntime.builder(new TestOutboxConfig().table(config.table())
                    .networkTimeout(Duration.ofSeconds(1)).maintenanceInterval(Duration.ofMillis(100)),
                    new JdbcTransactions(pool(throughTheHole(3, 0))), record -> {}).build();
            closing.add(relay);
            relay.start();
            Await.until(TIMEOUT, () -> TestSql.queryLong(c, "select count(*) from pg_stat_activity where query like"
                    + " 'create index concurrently%" + index.substring(index.lastIndexOf('.') + 1) + "%'"
                    + " and state = 'active'") == 1);
            Thread.sleep(1500); // past the bound: a long statement, which it must not cut

            assertThat(TestSql.queryLong(c, "select count(*) from pg_stat_activity where query like"
                    + " 'create index concurrently%' and state = 'active'")).as("still building").isEqualTo(1);

            hole.forget();
            holder.rollback(); // the backend ends the build and its answer goes nowhere
        }

        Await.until(TIMEOUT, () -> Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.getName().equals("outbox-dead-index")));
        try (var c = DIRECT.getConnection()) {
            assertThat(TestSql.queryString(c, "select indisvalid::text from pg_index where indexrelid = to_regclass(?)",
                    index)).isEqualTo("true");
        }
    }

    @Test
    void anIndexBuildHoldsItsLockUnderAnIdleTimeoutAndLeavesNothingOnItsConnections() throws Exception {
        // a holder whose client the network lost idles holding the lock; the timeout frees it, as for maintenance's
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(100));
        var bootstrap = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {}).build();
        bootstrap.start();
        bootstrap.close();
        var names = bootstrap.schema().names();
        var index = names.dead() + "_due";
        var whileLocked = new AtomicReference<String>();
        var atClose = new ConcurrentLinkedQueue<String>();
        var source = intercepting(DIRECT, (c, call, args) -> {
            if (!Thread.currentThread().getName().equals("outbox-dead-index")) {
                return;
            }
            if (call.getName().equals("prepareStatement") && args[0].toString().contains("pg_try_advisory_xact_lock(?, ?)")) {
                whileLocked.set(TestSql.queryString(c, "select current_setting('idle_in_transaction_session_timeout')"));
            } else if (call.getName().equals("close") && !c.isClosed()) {
                atClose.add(TestSql.queryString(c, "select coalesce(string_agg(name, ','), 'none') from pg_settings"
                        + " where source = 'session' and name not in ('application_name', 'client_encoding', 'DateStyle',"
                        + " 'TimeZone', 'IntervalStyle', 'extra_float_digits')")
                        + "|" + TestSql.queryLong(c, "select count(*) from pg_locks where locktype = 'advisory'"
                        + " and pid = pg_backend_pid()"));
            }
        });
        try (var c = DIRECT.getConnection()) {
            TestSql.execute(c, "drop index " + index);
            TestSql.execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                    + " values (1, 0, gen_random_uuid(), 't', now(), 1)"); // so maintenance builds it, not the start
            var relay = OutboxRuntime.builder(new TestOutboxConfig().table(config.table())
                    .maintenanceInterval(Duration.ofMillis(100)), new JdbcTransactions(source), record -> {}).build();
            closing.add(relay);
            relay.start();

            Await.until(TIMEOUT, () -> atClose.size() >= 2); // the build's connection and its holder's
            assertThat(TestSql.queryString(c, "select indisvalid::text from pg_index where indexrelid = to_regclass(?)",
                    index)).isEqualTo("true");
        }
        assertThat(whileLocked.get()).as("while its holder holds the lock").isEqualTo("1min");
        assertThat(atClose).as("nothing left for the session").containsOnly("none|0");
    }

    @Test
    void aSocketTimeoutOfTheDataSourceDoesNotCutTheIndexBuild() throws Exception {
        var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(100));
        var bootstrap = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {}).build();
        bootstrap.start();
        bootstrap.close();
        var names = bootstrap.schema().names();
        var index = names.dead() + "_due";
        try (var c = DIRECT.getConnection(); var holder = DIRECT.getConnection(); var err = new CapturedStderr()) {
            TestSql.execute(c, "drop index " + index);
            TestSql.execute(c, "insert into " + names.dead() + " (id, shard, message_id, topic, created_at, attempts)"
                    + " values (1, 0, gen_random_uuid(), 't', now(), 1)");
            holder.setAutoCommit(false);
            holder.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            TestSql.queryLong(holder, "select 1");
            var relay = OutboxRuntime.builder(new TestOutboxConfig().table(config.table())
                    .maintenanceInterval(Duration.ofMillis(100)), new JdbcTransactions(throughTheHole(3, 1)),
                    record -> {}).build();
            closing.add(relay);
            relay.start();
            Await.until(TIMEOUT, () -> TestSql.queryLong(c, "select count(*) from pg_stat_activity where query like"
                    + " 'create index concurrently%' and state = 'active'") == 1);
            Thread.sleep(2000); // past the DataSource's 1 s

            holder.rollback();

            Await.until(TIMEOUT, () -> "true".equals(TestSql.queryString(c, "select indisvalid::text from pg_index"
                    + " where indexrelid = to_regclass(?)", index)));
            assertThat(err.text()).doesNotContain("could not build");
        }
    }

    @Test
    void aDrainOnASilentSocketFailsInItsHaltCheck() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).networkTimeout(Duration.ofSeconds(1));
        var transactions = new JdbcTransactions(intercepting(throughTheHole(3, 0), (c, call, args) -> {
            if (call.getName().equals("setAutoCommit") && Thread.currentThread().getName().equals("test-drain")) {
                hole.forget(); // the first call on the check's connection
            }
        }));
        var relay = OutboxRuntime.builder(config, transactions, record -> {}).build();
        closing.add(relay);
        relay.start();

        assertThatThrownBy(() -> draining(relay).get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
    }

    @Test
    void aDrainOnASilentSocketFailsInItsDeadLetterPass() throws Exception {
        var config = new TestOutboxConfig().relayEnabled(false).networkTimeout(Duration.ofSeconds(1));
        var deadTable = new AtomicReference<String>();
        var transactions = new JdbcTransactions(intercepting(throughTheHole(3, 0), (c, call, args) -> {
            if (call.getName().equals("prepareStatement") && Thread.currentThread().getName().equals("test-drain")
                    && deadTable.get() != null && args[0].toString().contains(deadTable.get())) {
                hole.forget(); // the pass's claim
            }
        }));
        var relay = OutboxRuntime.builder(config, transactions, record -> {})
                .deadLetterHandler(letters -> letters.stream().map(letter -> DeadLetterDecision.keep()).toList())
                .build();
        closing.add(relay);
        relay.start();
        deadTable.set(relay.schema().names().dead());

        assertThatThrownBy(() -> draining(relay).get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
    }

    /**
     * A batch larger than the socket buffers, its reader gone silent mid-stream: the backend is not idle but blocked
     * sending, which no idle or statement timeout ends, so only the step's TCP bound frees its shard's lock.
     */
    @Test
    void aBatchStreamedIntoASilentNetworkFreesItsShard() throws Exception {
        var config = new TestOutboxConfig().batchTimeout(Duration.ofSeconds(1)).batchSize(1000)
                .maintenanceInterval(Duration.ofMillis(500));
        var writer = writer(config.table());
        var payload = new byte[20_000];
        Arrays.fill(payload, (byte) 'p');
        var sent = new HashSet<UUID>();
        for (int t = 0; t < 15; t++) { // ~60 MB on the wire as hex: past any buffer between the backend and the relay
            sent.addAll(direct.inTx(c -> {
                var ids = new ArrayList<UUID>();
                for (int i = 0; i < 100; i++) {
                    ids.add(writer.outbox().send(OutboxMessage.of("t", "one-shard", payload)));
                }
                return ids;
            }));
        }
        var held = new AtomicBoolean();
        var source = intercepting(pool(throughTheHole(3, 3)), (c, call, args) -> {
            if (call.getName().equals("prepareStatement") && Thread.currentThread().getName().startsWith("outbox-relay")
                    && args[0].toString().contains("o.payload") && held.compareAndSet(false, true)) {
                hole.holdReplies(); // the batch is sent into a network that takes no more
            }
        });
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(source),
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        Await.until(TIMEOUT, () -> {
            try (var c = DIRECT.getConnection()) {
                return TestSql.queryLong(c, "select count(*) from pg_stat_activity where wait_event = 'ClientWrite'")
                        > 0;
            }
        });

        Await.until(Duration.ofSeconds(120), () -> delivered.containsAll(sent)); // batchTimeout + 30 s, then the rest
    }

    private static FutureTask<Integer> draining(OutboxRuntime relay) {
        var drain = new FutureTask<>(relay.relay()::drain);
        var thread = new Thread(drain, "test-drain");
        thread.setDaemon(true);
        thread.start();
        return drain;
    }

    /** When a maintenance run last completed, microseconds. */
    private static long lastSuccess(String maintenance) throws Exception {
        try (var c = DIRECT.getConnection()) {
            return TestSql.queryLong(c, "select (extract(epoch from succeeded_at) * 1000000)::bigint from "
                    + maintenance);
        }
    }

    /** The first statement on a connection, which {@code start} then waits on, cut off. */
    private DataSource forgottenAtItsFirstStatement(DataSource source) {
        return intercepting(source, (c, call, args) -> {
            if (call.getName().equals("createStatement") || call.getName().equals("prepareStatement")) {
                hole.forget();
            }
        });
    }

    private static void startFails(Runnable start) throws Exception {
        var started = CompletableFuture.runAsync(start);
        assertThatThrownBy(() -> started.get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
    }

    @Test
    void aStartOnASilentSocketFails() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(forgottenAtItsFirstStatement(
                throughTheHole(3, 0))), record -> {}).build();
        closing.add(relay);

        startFails(relay::start);
    }

    @Test
    void aStartOnASilentApplicationConnectionFails() throws Exception {
        var table = new TestOutboxConfig();
        var bootstrap = OutboxRuntime.builder(table, new JdbcTransactions(DIRECT), record -> {}).build();
        closing.add(bootstrap);
        bootstrap.start();
        var config = new TestOutboxConfig().table(table.table()).schemaBootstrap(false)
                .networkTimeout(Duration.ofSeconds(1));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(forgottenAtItsFirstStatement(
                throughTheHole(3, 0))), record -> {}).ownerConnections(DIRECT::getConnection).build();
        closing.add(relay);

        startFails(relay::start);
    }

    @Test
    void anInboxStartOnASilentSocketFails() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        var inbox = InboxRuntime.builder(config, new JdbcTransactions(forgottenAtItsFirstStatement(
                throughTheHole(3, 0)))).build();
        closing.add(inbox);

        startFails(inbox::start);
    }

    /** {@code source} cut off at the first statement after {@code lock} was taken. */
    private DataSource forgottenAfter(String lock, DataSource source) {
        var locked = new AtomicBoolean();
        return intercepting(source, (c, call, args) -> {
            if (call.getName().equals("prepareStatement") || call.getName().equals("createStatement")) {
                if (locked.get()) {
                    hole.forget();
                } else if (args != null && args.length > 0 && args[0].toString().contains(lock)) {
                    locked.set(true);
                }
            }
        });
    }

    /** A start cut off holding the schema lock: the next start, on a new instance, waits for it no longer than it may. */
    @Test
    void aStartCutOffHoldingTheSchemaLockLetsTheNextStartGoOn() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        var cut = OutboxRuntime.builder(config, new JdbcTransactions(forgottenAfter("advisory_xact_lock(?, ?)",
                throughTheHole(3, 0))), record -> {}).build();
        CompletableFuture.runAsync(cut::start); // cut off, as it should be; the hole's close ends its wait
        Await.until(TIMEOUT, () -> {
            try (var c = DIRECT.getConnection()) {
                return TestSql.queryLong(c, "select count(*) from pg_locks where locktype = 'advisory'"
                        + " and granted") > 0;
            }
        });
        var next = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()), new JdbcTransactions(DIRECT),
                record -> {}).build();
        closing.add(next);

        CompletableFuture.runAsync(next::start).get(60, TimeUnit.SECONDS);
    }

    /** The inbox's: later starts go on anyway, and its maintenance must not stay locked out for hours. */
    @Test
    void anInboxStartCutOffHoldingItsLockLeavesMaintenanceGoingOn() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        var made = InboxRuntime.builder(config, new JdbcTransactions(DIRECT)).build();
        made.start();
        made.close();
        var cut = InboxRuntime.builder(config, new JdbcTransactions(forgottenAfter("advisory_xact_lock(?, ?)",
                throughTheHole(3, 0)))).build();
        CompletableFuture.runAsync(cut::start); // cut off, as it should be; the hole's close ends its wait
        Await.until(TIMEOUT, () -> {
            try (var c = DIRECT.getConnection()) {
                return TestSql.queryLong(c, "select count(*) from pg_locks where locktype = 'advisory' and granted") > 0;
            }
        });
        var next = InboxRuntime.builder(new TestOutboxConfig().inboxTable(config.inbox().table()).maintenanceInterval(
                Duration.ofMillis(200)), new JdbcTransactions(DIRECT)).build();
        closing.add(next);

        CompletableFuture.runAsync(next::start).get(60, TimeUnit.SECONDS);
        var past = config.inbox().table() + "_t2020010100"; // a run drops it, far past retention and empty
        try (var c = DIRECT.getConnection()) {
            TestSql.execute(c, "create table " + past + " (like " + config.inbox().table() + " including defaults)");
        }

        Await.until(Duration.ofSeconds(60), () -> {
            try (var c = DIRECT.getConnection()) {
                return TestSql.queryString(c, "select to_regclass(?)::text", past) == null;
            }
        });
    }

    /** A lock wait the start's own lock_timeout ends is no silence, however short the bound. */
    @Test
    void aStartWaitingOnALockOutlastsTheNetworkTimeout() throws Exception {
        var table = new TestOutboxConfig();
        var first = OutboxRuntime.builder(table, new JdbcTransactions(DIRECT), record -> {}).build();
        closing.add(first);
        first.start();
        var maintenance = first.schema().names().maintenance();
        var config = new TestOutboxConfig().table(table.table()).networkTimeout(Duration.ofSeconds(1));
        var second = OutboxRuntime.builder(config, new JdbcTransactions(throughTheHole(3, 0)), record -> {}).build();
        closing.add(second);
        try (var locker = DIRECT.getConnection()) {
            locker.setAutoCommit(false);
            TestSql.execute(locker, "lock table " + maintenance + " in access exclusive mode");
            var started = CompletableFuture.runAsync(second::start);
            Thread.sleep(3000);
            locker.rollback();
            started.get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void aHeartbeatSourceSilentAtStartLeavesTheStartGoing() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1));
        var own = forgottenAtItsFirstStatement(throughTheHole(3, 0));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {})
                .heartbeatConnections(own::getConnection).build();
        closing.add(relay);

        CompletableFuture.runAsync(relay::start).get(20, TimeUnit.SECONDS);
    }

    /** Its first connection after a start that could not reach it: the check of it goes silent, and the beats go on. */
    @Test
    void aHeartbeatSourceSilentAtItsFirstCheckFallsBack() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(1))
                .maintenanceInterval(Duration.ofMillis(100));
        var reachable = new AtomicBoolean();
        var own = forgottenAtItsFirstStatement(throughTheHole(3, 0));
        var relay = OutboxRuntime.builder(config, new JdbcTransactions(DIRECT), record -> {})
                .heartbeatConnections(() -> {
                    if (!reachable.get()) {
                        throw new SQLException("unreachable");
                    }
                    return own.getConnection();
                }).build();
        closing.add(relay);
        relay.start();

        reachable.set(true);

        Thread.sleep(500); // on the beat that takes it
        awaitBeat(relay);
    }

    @Test
    void aConnectionGoesBackWithTheTimeoutItCameWith() throws Exception {
        var atClose = new ConcurrentLinkedQueue<Integer>();
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(9));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(intercepting(DIRECT, (c, call, args) -> {
            if (call.getName().equals("close") && Thread.currentThread().getName().startsWith("outbox-relay-")) {
                atClose.add(c.getNetworkTimeout());
            }
        }));
        var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();

        var sent = send(relay, transactions, "k");
        Await.until(TIMEOUT, () -> delivered.contains(sent));
        Await.until(TIMEOUT, () -> !atClose.isEmpty());

        assertThat(atClose).as("a pool that restores nothing passes on what it gets").containsOnly(0);
    }

    @Test
    void aConnectionThatTellsNoTimeoutStillDeliversWithOneWarn() throws Exception {
        var config = new TestOutboxConfig().networkTimeout(Duration.ofSeconds(9));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var transactions = new JdbcTransactions(intercepting(DIRECT, (c, call, args) -> {
            if (call.getName().equals("getNetworkTimeout")) {
                throw new SQLFeatureNotSupportedException("no such thing here");
            }
        }));
        try (var err = new CapturedStderr()) {
            var relay = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId()))
                    .build();
            closing.add(relay);
            relay.start();

            var sent = send(relay, transactions, "k");
            Await.until(TIMEOUT, () -> delivered.contains(sent));
            Thread.sleep(200); // more passes

            assertThat(err.text().lines().filter(line -> line.contains("could not be set")).count())
                    .as("once per JVM, however many passes and components; a test before it may have had it")
                    .isLessThanOrEqualTo(1);
        }
    }

    @Test
    void aSilentCursorReadOnOneConnectionHoldsNoOtherWorker() throws Exception {
        var armed = new AtomicBoolean();
        var once = new AtomicBoolean();
        var silent = new CountDownLatch(1);
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        // one relay connection goes silent in its cursor read until its networkTimeout, the others stay healthy
        var transactions = new JdbcTransactions(intercepting(DIRECT, (c, call, args) -> {
            if (armed.get() && call.getName().equals("prepareStatement")
                    && ((String) args[0]).startsWith("with c0 as (select c.* from generate_series")
                    && Thread.currentThread().getName().startsWith("outbox-relay-") && once.compareAndSet(false, true)) {
                silent.countDown();
                Thread.sleep(30_000);
            }
        }));
        var relay = OutboxRuntime.builder(new TestOutboxConfig().workers(4).shards(16), transactions,
                record -> delivered.add(record.messageId())).build();
        closing.add(relay);
        relay.start();
        var sent = new HashSet<UUID>();
        sent.add(send(relay, transactions, "before"));
        Await.until(TIMEOUT, () -> delivered.containsAll(sent));

        armed.set(true);
        assertThat(silent.await(10, TimeUnit.SECONDS)).isTrue();
        for (int i = 0; i < 10; i++) {
            sent.add(send(relay, transactions, "k" + i));
        }

        Await.until(Duration.ofSeconds(3), () -> delivered.containsAll(sent));
    }

    private static UUID send(OutboxRuntime runtime, JdbcTransactions transactions, String key) {
        return transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", key, "p".getBytes(UTF_8))));
    }

    /** Rolls {@code holder} back, releasing its locks, {@code millis} from now. */
    private static CompletableFuture<Void> rollbackAfter(Connection holder, long millis) {
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(millis);
                holder.rollback();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /** A writer-only instance on {@link #DIRECT} for {@code table}, started; closed after the test. */
    private OutboxRuntime writer(String table) {
        var writer = OutboxRuntime.builder(new TestOutboxConfig().table(table).relayEnabled(false), direct,
                record -> {}).build();
        closing.add(writer);
        writer.start();
        return writer;
    }

    /** Until {@code relay}'s heartbeat beats again, within 30 s. */
    private static void awaitBeat(OutboxRuntime relay) throws Exception {
        awaitBeat(Duration.ofSeconds(30), relay);
    }

    private static void awaitBeat(Duration bound, OutboxRuntime relay) throws Exception {
        var instances = relay.schema().names().instance();
        long seen = lastSeen(instances, relay);
        Await.until(bound, () -> lastSeen(instances, relay) > seen);
    }

    private static long lastSeen(String instances, OutboxRuntime runtime) throws Exception {
        try (var c = DIRECT.getConnection()) {
            return TestSql.queryLong(c, "select (extract(epoch from last_seen) * 1000000)::bigint from " + instances
                    + " where instance_id = '" + runtime.schema().instanceId() + "'");
        }
    }

    /** Connections through {@link #hole}; {@code seconds}: connect, login and socket timeouts, 0 for none. */
    private DataSource throughTheHole(int seconds) {
        return throughTheHole(seconds, seconds);
    }

    /** {@code connectSeconds}: connect and login timeouts; {@code socketSeconds}: socket timeout; 0 for none. */
    private DataSource throughTheHole(int connectSeconds, int socketSeconds) {
        var ds = new PGSimpleDataSource();
        ds.setUrl("jdbc:postgresql://127.0.0.1:" + hole.port() + "/" + TARGET.getPath().substring(1));
        ds.setUser(TestDataSources.user());
        ds.setPassword(TestDataSources.password());
        ds.setConnectTimeout(connectSeconds);
        ds.setLoginTimeout(connectSeconds);
        ds.setSocketTimeout(socketSeconds);
        return ds;
    }

    /** {@code source} behind a pool, so a worker keeps its connection from pass to pass, as in a service. */
    private DataSource pool(DataSource source) {
        var config = new HikariConfig();
        config.setDataSource(source);
        config.setMaximumPoolSize(8);
        config.setValidationTimeout(250); // sheds the idle connections it finds dead fast: 5 s each by default
        // also its login timeout: its one adder thread on a login the hole forgot holds every new connection that long
        config.setConnectionTimeout(2000);
        var pool = new HikariDataSource(config);
        closing.add(pool);
        return pool;
    }

    /** {@code source} whose connections add to {@code calls}, per {@code setNetworkTimeout}: thread name:milliseconds. */
    private static DataSource recordingTimeouts(DataSource source, Queue<String> calls) {
        return intercepting(source, (c, call, args) -> {
            if (call.getName().equals("setNetworkTimeout") && !args[1].equals(0)) {
                calls.add(Thread.currentThread().getName() + ":" + args[1]);
            }
        });
    }

    private static final URI TARGET = URI.create(TestDataSources.jdbcUrl().substring("jdbc:".length()));

    /**
     * A TCP forwarder to the test's Postgres that, shut, keeps every connection open and forwards nothing; a connection
     * it forgot stays so for good, as a firewall that lost track of it would keep it, while new ones go through.
     */
    private static final class Blackhole implements AutoCloseable {

        private final ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final Set<Socket> forgotten = ConcurrentHashMap.newKeySet();
        private final Set<Socket> targets = ConcurrentHashMap.newKeySet();
        private final Set<Socket> held = ConcurrentHashMap.newKeySet();
        private volatile boolean shut;

        Blackhole() throws IOException {
            daemon(this::accept);
        }

        int port() {
            return server.getLocalPort();
        }

        void shut() {
            shut = true;
        }

        void open() {
            shut = false;
        }

        /** Every connection stays silent for good, and the hole lets new ones through. */
        void forget() {
            forgotten.addAll(sockets);
            shut = false;
        }

        /**
         * Every connection's server side is read no more and stays open, its client gone or not, as a network that
         * lost the client keeps the server's sends unacknowledged; requests still go through, and new connections too.
         */
        void holdReplies() {
            held.addAll(targets);
        }

        @Override
        public void close() throws IOException {
            server.close();
            for (var socket : sockets) {
                socket.close();
            }
        }

        private void accept() {
            try {
                while (true) {
                    var client = server.accept();
                    var target = new Socket(TARGET.getHost(), TARGET.getPort());
                    sockets.add(client);
                    sockets.add(target);
                    targets.add(target);
                    daemon(() -> pump(client, target));
                    daemon(() -> pump(target, client));
                }
            } catch (IOException e) { // closed
            }
        }

        private void pump(Socket from, Socket to) {
            var buffer = new byte[8192];
            try (from) {
                var in = from.getInputStream();
                var out = to.getOutputStream();
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    while (shut || forgotten.contains(from) || held.contains(from)) {
                        Thread.sleep(10);
                    }
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException | InterruptedException e) { // one side went away, and the other goes with it
            } finally {
                if (!held.contains(to)) {
                    try {
                        to.close();
                    } catch (IOException e) { // closed already
                    }
                }
            }
        }

        private static void daemon(Runnable work) {
            var thread = new Thread(work, "test-blackhole");
            thread.setDaemon(true);
            thread.start();
        }
    }
}
