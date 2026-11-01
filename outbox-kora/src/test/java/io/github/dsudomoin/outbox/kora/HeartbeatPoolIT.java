package io.github.dsudomoin.outbox.kora;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.internal.TableNames;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.application.graph.All;
import ru.tinkoff.kora.application.graph.LifecycleWrapper;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetryFactory;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;

/**
 * {@link OutboxModule#outboxRuntime}'s heartbeat: on a pool of one connection derived from the app's by default, so
 * beats go on while business load holds every app connection; on the app's connections with the flag off or when the
 * pool cannot be derived; on a tagged factory whatever the flag says. The derived pool closes after the runtime has
 * deleted its row.
 */
class HeartbeatPoolIT {

    private static final Duration INTERVAL = Duration.ofSeconds(1);
    /** 3 intervals: older than that, the row has expired. */
    private static final long STALE_MILLIS = 3 * INTERVAL.toMillis();

    private final TestDatabase database = TestDb.newDatabase(2);
    private final TestOutboxConfig config = new TestOutboxConfig().maintenanceInterval(INTERVAL).relayEnabled(false);

    @AfterEach
    void releaseDatabase() {
        database.release();
    }

    @Test
    void beatsContinueWhileTheAppPoolIsHeld() throws Exception {
        var run = hold(runtime(null, true));

        assertThat(run.second()).as("beats while the app pool is held").isGreaterThan(run.first());
        assertThat(run.age()).isLessThan(STALE_MILLIS);
    }

    @Test
    void withTheFlagOffBeatsStall() throws Exception {
        var run = hold(runtime(null, false));

        assertThat(run.age()).isGreaterThan(STALE_MILLIS);
    }

    @Test
    void aTaggedFactoryWinsOverTheFlagOff() throws Exception {
        taggedFactoryServesTheBeats(false);
    }

    @Test
    void aTaggedFactoryWinsOverTheFlagOn() throws Exception {
        taggedFactoryServesTheBeats(true);
    }

    @Test
    void aFailedDerivationSharesTheAppConnections() throws Exception {
        var taken = new AtomicInteger();
        var app = new JdbcDatabase(TestDb.config(2), DataBaseTelemetryFactory.EMPTY) {
            @Override
            public DataSource value() {
                return new HikariDataSource(); // not started and without a URL: a pool derived from it fails to build
            }

            @Override
            public Connection newConnection() {
                taken.incrementAndGet();
                return super.newConnection();
            }
        };
        try (var log = new CapturedStderr()) {
            var wrapper = new OutboxModule() {}.outboxRuntime(config, new KoraOutboxTransactions(app),
                    new RecordingHandler(), All.of(), null, new OutboxTelemetry() {}, null, null, flag(true));
            try {
                wrapper.init();
                var names = wrapper.value().schema().names();
                int before = taken.get();
                awaitBeat(names);
                awaitBeat(names);

                assertThat(taken.get()).as("app connections across two beats").isGreaterThan(before);
            } finally {
                wrapper.release();
            }
            assertThat(log.text()).containsOnlyOnce("could not derive a pool of its own")
                    .containsOnlyOnce("outbox heartbeat shares the application's connections");
        } finally {
            app.release();
        }
    }

    @Test
    void releaseDeletesTheRowBeforeClosingThePool() throws Exception {
        var wrapper = runtime(null, true);
        wrapper.init();
        var names = wrapper.value().schema().names();
        assertThat(poolRuns()).as("the derived pool, before the release").isTrue();
        try (var log = new CapturedStderr()) {
            wrapper.release();

            assertThat(log.text()).doesNotContain("this beat uses the application's connections")
                    .doesNotContain("could not delete its row");
        }
        try (var c = TestDb.connect()) {
            assertThat(TestSql.queryLong(c, "select count(*) from " + names.instance())).isZero();
        }
        await(() -> !poolRuns());
    }

    @Test
    void aLockedRowDoesNotHoldTheReleaseAndThePoolStillCloses() throws Exception {
        var wrapper = runtime(null, true);
        wrapper.init();
        var names = wrapper.value().schema().names();
        var releasing = new CompletableFuture<Void>();
        try (var holder = TestDb.connect()) {
            holder.setAutoCommit(false);
            TestSql.execute(holder, "select 1 from " + names.instance() + " for update");
            var thread = new Thread(() -> {
                try {
                    wrapper.release();
                    releasing.complete(null);
                } catch (Throwable t) {
                    releasing.completeExceptionally(t);
                }
            });
            thread.setDaemon(true);
            thread.start();

            releasing.get(20, TimeUnit.SECONDS);
            holder.rollback();
        }
        await(() -> !poolRuns());
    }

    @Test
    void aFailedBuildOrStartClosesThePool() throws Exception {
        assertThatThrownBy(() -> new OutboxModule() {}.outboxRuntime(config, new KoraOutboxTransactions(database), null,
                All.of(), null, new OutboxTelemetry() {}, null, null, flag(true)))
                .as("no handler, no route").isInstanceOf(IllegalArgumentException.class);
        await(() -> !poolRuns());

        config.table("s" + TestSql.uniqueTable() + ".outbox"); // in a schema that does not exist
        var wrapper = runtime(null, true);
        assertThat(poolRuns()).isTrue();
        assertThatThrownBy(wrapper::init).hasStackTraceContaining("does not exist");
        await(() -> !poolRuns());
    }

    @Test
    void withNothingToCopyOneInfoNamesTheTag() {
        try (var log = new CapturedStderr()) {
            new OutboxModule() {}.outboxRuntime(config, database, new RecordingHandler(), All.of(), null,
                    new OutboxTelemetry() {}, null, null, flag(true));

            assertThat(log.text()).as("TestDatabase is no KoraOutboxTransactions")
                    .containsOnlyOnce("@Tag(OutboxHeartbeat.class)");
        }
        assertThat(poolRuns()).isFalse();
    }

    /** Beats on a tagged factory, with no pool derived whatever {@code dedicated} says. */
    private void taggedFactoryServesTheBeats(boolean dedicated) throws Exception {
        var other = TestDb.newDatabase(1);
        var taken = new AtomicInteger();
        var wrapper = runtime(counting(other, taken), dedicated);
        try {
            wrapper.init();
            var names = wrapper.value().schema().names();
            int before = taken.get();
            awaitBeat(names);
            awaitBeat(names);

            assertThat(taken.get()).as("connections the tagged factory gave across two beats").isGreaterThan(before);
            assertThat(poolRuns()).as("a pool derived from the app's").isFalse();
        } finally {
            wrapper.release();
            other.release();
        }
    }

    /** {@code last_seen} sampled twice, 2 s apart, while both app connections are held, and its age as they go back. */
    private record Run(long first, long second, long age) {}

    /** Starts the runtime, waits for a beat, then holds both app connections for 5 s, measuring; then releases. */
    private Run hold(LifecycleWrapper<OutboxRuntime> wrapper) throws Exception {
        wrapper.init();
        try {
            var names = wrapper.value().schema().names();
            awaitBeat(names);
            var app = database.value();
            try (var a = app.getConnection(); var b = app.getConnection()) {
                Thread.sleep(1000);
                long first = millis("last_seen", names);
                Thread.sleep(2000);
                long second = millis("last_seen", names);
                Thread.sleep(2000);
                return new Run(first, second, millis("now() - last_seen", names));
            }
        } finally {
            wrapper.release();
        }
    }

    private LifecycleWrapper<OutboxRuntime> runtime(@Nullable JdbcConnectionFactory tagged, boolean dedicated) {
        return new OutboxModule() {}.outboxRuntime(config, new KoraOutboxTransactions(database), new RecordingHandler(),
                All.<OutboxTopicHandler>of(), null, new OutboxTelemetry() {}, null, tagged, flag(dedicated));
    }

    private static OutboxHeartbeatConfig flag(boolean dedicated) {
        return new OutboxHeartbeatConfig() {
            @Override
            public boolean dedicatedConnection() {
                return dedicated;
            }
        };
    }

    /** Whether the pool derived from {@link #database} runs: Hikari names its housekeeper thread after the pool. */
    private boolean poolRuns() {
        var name = ((HikariDataSource) database.value()).getPoolName() + "-outbox-heartbeat:housekeeper";
        return Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals(name));
    }

    /** {@code db}, counting the connections it gives. */
    private static JdbcConnectionFactory counting(JdbcDatabase db, AtomicInteger taken) {
        return new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                return db.withConnection(callback);
            }

            @Nullable
            @Override
            public Connection currentConnection() {
                return db.currentConnection();
            }

            @Override
            public Connection newConnection() {
                taken.incrementAndGet();
                return db.newConnection();
            }

            @Override
            public DataBaseTelemetry telemetry() {
                return db.telemetry();
            }
        };
    }

    /** Waits until {@code last_seen} moves past its value now. */
    private static void awaitBeat(TableNames names) throws Exception {
        long seen = millis("last_seen", names);
        await(() -> millis("last_seen", names) > seen);
    }

    /** {@code expression} of the instance's row, in milliseconds since the epoch or as a duration. */
    private static long millis(String expression, TableNames names) throws SQLException {
        try (var c = TestDb.connect()) {
            return TestSql.queryLong(c, "select (extract(epoch from " + expression + ") * 1000)::bigint from "
                    + names.instance());
        }
    }

    private static void await(Check condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within 10 s");
            }
            Thread.sleep(20);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }
}
