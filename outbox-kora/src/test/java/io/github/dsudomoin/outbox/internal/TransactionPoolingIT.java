package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxSql;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestPooler;
import io.github.dsudomoin.outbox.testing.TestTelemetry;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/**
 * The whole library with pgbouncer in transaction mode as its only way to the database: start, send, relay,
 * maintenance, inbox and the admin's repairs, on a pool of four server sessions handed out round robin, so the
 * statements of one connection land in different server sessions. After each test no server session holds anything a
 * client left: no session advisory lock, no setting set for the session.
 */
class TransactionPoolingIT {

    private static TestPooler pooler;

    private final List<TestDatabase> pools = new ArrayList<>();
    private final TestDatabase direct = TestDb.newDatabase(4);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final List<AutoCloseable> closing = new ArrayList<>();

    @BeforeAll
    static void startPooler() throws SQLException {
        pooler = TestPooler.start(4, 0);
        pooler.warm();
    }

    @AfterAll
    static void stopPooler() {
        pooler.close();
    }

    @AfterEach
    void nothingLeftInTheServerSessions() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
        pools.forEach(TestDatabase::release);
        direct.release();
        assertThat(pooler.leftovers()).isEmpty();
    }

    @Test
    void startsSendsAndRelaysThroughThePoolerAlone() throws Exception {
        var db = pooled(10);
        var handler = new RecordingHandler();
        var runtime = OutboxRuntime.builder(config, db, handler).build();
        closing.add(runtime);

        runtime.start();
        var sent = new ArrayList<UUID>();
        for (int i = 0; i < 30; i++) {
            var message = msg("t", "k" + i % 3, "m" + i);
            sent.add(db.inTx((SqlFunction0<UUID>) () -> runtime.outbox().send(message)));
        }

        await(() -> handler.delivered().size() >= sent.size(), "all 30 delivered");
        assertThat(handler.delivered()).extracting(OutboxRecord::messageId).containsExactlyInAnyOrderElementsOf(sent);
        assertThat(handler.delivered()).filteredOn(r -> new String(r.key(), UTF_8).equals("k1"))
                .extracting(r -> new String(r.payload(), UTF_8)).as("in the order of their commits")
                .containsExactly("m1", "m4", "m7", "m10", "m13", "m16", "m19", "m22", "m25", "m28");
    }

    @Test
    void startsWithTheSchemaFromMigrations() throws Exception {
        try (var c = TestDb.connect()) {
            execute(c, OutboxSql.outbox(config, null));
        }
        config.schemaBootstrap(false);
        var handler = new RecordingHandler();
        var runtime = OutboxRuntime.builder(config, pooled(10), handler).build();
        closing.add(runtime);

        runtime.start();

        assertThat(runtime.admin().status()).isNotNull();
    }

    @Test
    void maintenanceExtendsAndDropsPartitionsThroughThePooler() throws Exception {
        config.relayEnabled(false).xidsPerPartition(1000).premake(10);
        var schema = new OutboxSchema(direct, config);
        schema.start();
        var names = schema.names();
        var relay = new OutboxRelay(direct, config, schema, record -> {}, TestTelemetry.kora(),
                new Maintenance(direct, names, config, new OutboxMetrics(null)),
                new DeadLetterPass(direct, names, config, null, new OutboxMetrics(null)));
        closing.add(relay::close);
        var outbox = new PgOutbox(direct, schema, config.shards(), () -> {}, TestTelemetry.kora());
        direct.inTx((SqlFunction0<List<UUID>>) () -> outbox.sendAll(List.of(msg("t", "k", "a"))));
        TestDb.burnXids(7000); // past half of the premade ranges: the extension is due
        assertThat(relay.drain()).isOne();
        var maintenance = new Maintenance(pooled(4), names, config, new OutboxMetrics(null));

        assertThat(maintenance.runOnce()).as("ran, not skipped").isTrue();

        try (var c = TestDb.connect()) {
            var partitions = XidPartitions.forBootstrap(names, config.partitions());
            assertThat(partitions.premadeAhead(c)).as("extended").isGreaterThan(config.premake() / 2);
            long xmin = new Ticks(names, config.shards()).latest(c).snapshot().xmin();
            assertThat(PartitionDdl.listRanges(c, names.base())).as("processed ranges dropped")
                    .noneMatch(b -> b.to() != null && Long.parseLong(b.to()) <= xmin - 2000);
        }
    }

    @Test
    void inboxStartsAcceptsAndMaintainsThroughThePooler() throws Exception {
        var db = pooled(4);
        var inbox = new PgInbox(db, config, new OutboxMetrics(null));
        closing.add(inbox);

        inbox.start();
        var id = UuidV7.next();

        assertThat(db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isTrue();
        assertThat(db.inTx((SqlFunction0<Boolean>) () -> inbox.tryAccept(id))).isFalse();
        inbox.runMaintenance();
    }

    @Test
    void adminRepairsRunThroughThePooler() throws Exception {
        config.relayEnabled(false);
        var schema = new OutboxSchema(direct, config);
        schema.start();
        var names = schema.names();
        var db = pooled(4);
        var admin = new PgOutboxAdmin(db, names, config, new Maintenance(db, names, config, new OutboxMetrics(null)),
                () -> {});
        var orphan = names.partitionPrefix() + "1000";
        try (var c = TestDb.connect()) {
            PartitionDdl.create(c, names.base(), orphan, Duration.ofSeconds(5));
            execute(c, "insert into " + orphan + " (id, shard, xid, message_id, topic, key) overriding system value"
                    + " values (1, 0, '20', gen_random_uuid(), 't', convert_to('k', 'UTF8'))");
        }

        assertThat(admin.restoreOrphan(orphan, 10)).isOne();
        assertThatThrownBy(admin::recoverXidRegression).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no xid regression to recover");
        assertThatThrownBy(admin::recoverStrandedShards).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no stranded shards");
    }

    @Test
    void startRefusesConnectionsThatLosePreparedStatementsAndSaysWhatToSet() throws Exception {
        // prepareThreshold 5
        var db = TestDb.newDatabase(pooler.jdbcUrl(), TestDataSources.user(), TestDataSources.password(), 4);
        pools.add(db);
        var runtime = OutboxRuntime.builder(config, db, new RecordingHandler()).build();
        closing.add(runtime);

        assertThatThrownBy(runtime::start).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("prepareThreshold=0").hasMessageContaining("max_prepared_statements");
    }

    @Test
    void runsWithTheDriversPreparedStatementsBehindAPoolerThatKeepsThem() throws Exception {
        try (var keeping = TestPooler.start(4, 200)) {
            keeping.warm();
            // threshold 5
            var db = TestDb.newDatabase(keeping.jdbcUrl(), TestDataSources.user(), TestDataSources.password(), 10);
            pools.add(db);
            var handler = new RecordingHandler();
            var runtime = OutboxRuntime.builder(config, db, handler).build();
            closing.add(runtime);

            runtime.start();
            for (int i = 0; i < 30; i++) {
                var message = msg("t", "k" + i % 3, "m" + i);
                db.inTx((SqlFunction0<UUID>) () -> runtime.outbox().send(message));
            }
            await(() -> handler.delivered().size() >= 30, "all 30 delivered");
            assertThat(runtime.maintenance().runOnce()).isTrue();

            runtime.close();
            assertThat(keeping.leftovers()).isEmpty();
        }
    }

    private TestDatabase pooled(int size) {
        var db = TestDb.newDatabase(pooler, size);
        pools.add(db);
        return db;
    }

    private static void await(BooleanSupplier done, String what) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!done.getAsBoolean()) {
            assertThat(System.nanoTime()).as(what).isLessThan(deadline);
            Thread.sleep(20);
        }
    }

    private static OutboxMessage msg(String topic, String key, String payload) {
        return OutboxMessage.of(topic, key, payload.getBytes(UTF_8));
    }
}
