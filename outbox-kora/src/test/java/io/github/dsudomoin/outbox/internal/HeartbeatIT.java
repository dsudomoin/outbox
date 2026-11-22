package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.kora.KoraOutboxTransactions;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.Await;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Each instance's row in {@code <base>_instance} ({@link Heartbeat}): written from the bootstrap on, refreshed by the
 * beats, gone after shutdown; and the shard count of an existing outbox, which every instance takes from its cursors.
 */
class HeartbeatIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final OutboxDeadLetterHandler HOOK = letters -> List.of();

    private final TestDatabase db = TestDb.newDatabase(20);
    private final String table = TestSql.uniqueTable();
    private final TableNames names = TableNames.of(table);
    private final RecordingHandler recording = new RecordingHandler();
    private final List<OutboxRelay> relays = new ArrayList<>();

    private record Node(OutboxSchema schema, OutboxRelay relay) {}

    @AfterEach
    void release() {
        relays.forEach(OutboxRelay::close);
        db.release();
    }

    @Test
    void anExistingOutboxKeepsTheShardCountOfItsCursors() throws Exception {
        start(config(32));
        long ticks = queryLong("select count(*) from " + names.tick());

        var node = start(config(4));
        assertThat(OutboxSchema.shards(node.schema().config())).isEqualTo(32);
        assertThat(cursorShards()).isEqualTo(32);
        assertThat(queryLong("select count(*) from " + names.tick())).as("cursors left alone").isEqualTo(ticks);

        var fresh = new OutboxSchema(db, new TestOutboxConfig().shards(16));
        fresh.start();
        assertThat(OutboxSchema.shards(fresh.config())).as("a new outbox").isEqualTo(16);
    }

    @Test
    void hookActiveOnlyWhereTheRelayRuns() throws Exception {
        var writer = start(config(4).relayEnabled(false), HOOK);
        writer.relay().start();
        var relaying = start(config(4), HOOK);
        relaying.relay().start();

        Await.until(TIMEOUT, () -> queryLong("select count(*) from " + names.instance()
                + " where last_seen > now() - interval '10 seconds' and hook_active is not null") == 2);
        Await.until(TIMEOUT, () -> queryString("select string_agg(hook_active::text, ',' order by hook_active) from "
                + names.instance()).equals("false,true"));
        assertThat(queryString("select hook_active::text from " + names.instance() + " where instance_id = '"
                + writer.schema().instanceId() + "'")).as("a writer-only pod runs no dead-letter pass")
                .isEqualTo("false");
    }

    @Test
    void heartbeatRowIsGoneAfterShutdown() throws Exception {
        var node = start(config(4).relayEnabled(false));
        node.relay().start();

        Await.until(TIMEOUT, () -> queryLong("select count(*) from " + names.instance()) == 1);
        assertThat(queryString("select instance_id from " + names.instance()))
                .isEqualTo(node.schema().instanceId())
                .matches("[^/]+/" + ProcessHandle.current().pid() + "/[0-9a-f]{8}");
        assertThat(queryString("select shards || ' ' || hook_active || ' ' || (expires_at > now()) from "
                + names.instance())).isEqualTo("4 false true");

        node.relay().close();
        assertThat(queryLong("select count(*) from " + names.instance())).isZero();
    }

    @Test
    void aBeatAfterStopDoesNotBringTheRowBack() throws Exception {
        var node = start(config(4));
        var heartbeat = new Heartbeat(db, node.schema(), node.schema().config(), false);

        heartbeat.stop();
        heartbeat.beat();

        assertThat(queryLong("select count(*) from " + names.instance())).isZero();
    }

    @Test
    void aFailedBeatKeepsTheScheduleGoing() throws Exception {
        var node = start(config(4).maintenanceInterval(Duration.ofMillis(50)));
        execute("delete from " + names.instance());
        var failed = new AtomicBoolean();
        var failingOnce = TestDb.opening(db, () -> {
            if (failed.compareAndSet(false, true)) {
                throw new AssertionError("not an Exception: would cancel a schedule that catches only those");
            }
            return db.newConnection();
        });
        var heartbeat = new Heartbeat(
                new KoraOutboxTransactions(failingOnce), node.schema(), node.schema().config(), false);
        try {
            heartbeat.start();
            Await.until(TIMEOUT, () -> queryLong("select count(*) from " + names.instance()) == 1);
            assertThat(failed).isTrue();
        } finally {
            heartbeat.stop();
        }
    }


    private TestOutboxConfig config(int shards) {
        return new TestOutboxConfig().table(table).shards(shards);
    }

    private Node start(TestOutboxConfig config) throws SQLException {
        return start(config, null);
    }

    /** Bootstrap with {@code config}, and a relay (not started) over it. */
    private Node start(TestOutboxConfig config, OutboxDeadLetterHandler hook) throws SQLException {
        var schema = new OutboxSchema(db, config);
        schema.start();
        var resolved = schema.config();
        var maintenance = new Maintenance(db, schema.names(), resolved, new OutboxMetrics(null));
        var relay = new OutboxRelay(db, resolved, schema, recording, TestTelemetry.kora(), maintenance,
                new DeadLetterPass(db, schema.names(), resolved, hook, new OutboxMetrics(null)));
        relays.add(relay);
        return new Node(schema, relay);
    }

    private long cursorShards() throws SQLException {
        return queryLong("select count(distinct shard) from " + names.cursor());
    }

    private static void execute(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    private static long queryLong(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            return TestSql.queryLong(c, sql);
        }
    }

    private static String queryString(String sql) throws SQLException {
        try (var c = TestDb.connect()) {
            return TestSql.queryString(c, sql);
        }
    }
}
