package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class PartitionDdlIT {

    /** Postgres 17, the first with {@code transaction_timeout}; started by the first test that needs it. */
    private static final class Pg17 {
        static final PostgreSQLContainer INSTANCE =
                new PostgreSQLContainer("postgres:17-alpine").withCommand("postgres", "-c", "fsync=off");

        static {
            INSTANCE.start();
        }

        static Connection connect() throws SQLException {
            return DriverManager.getConnection(INSTANCE.getJdbcUrl(), INSTANCE.getUsername(), INSTANCE.getPassword());
        }
    }

    /**
     * A snapshot taken before another session dropped a range still lists it in {@code pg_inherits}, while
     * {@code pg_get_expr} finds no relation for it any more: as {@code admin.status()} during maintenance's drops.
     */
    @Test
    void aRangeDroppedAfterTheSnapshotIsLeftOut() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var reader = TestDataSources.newDataSource().getConnection();
                var other = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(other, "create table " + parent + " (xid xid8 not null) partition by range (xid)");
            TestSql.execute(other, "create table " + parent + "_a partition of " + parent
                    + " for values from ('0') to ('100')");
            TestSql.execute(other, "create table " + parent + "_b partition of " + parent
                    + " for values from ('100') to ('200')");
            reader.setAutoCommit(false);
            reader.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            TestSql.queryLong(reader, "select 1"); // the snapshot
            TestSql.execute(other, "drop table " + parent + "_a");

            assertThat(PartitionDdl.listRanges(reader, parent))
                    .containsExactly(new PartitionDdl.Bound(parent + "_b", "100", "200"));
            reader.rollback();
        }
    }

    /** The same for a standalone table: {@code ::regclass} prints one dropped since the snapshot as its bare oid. */
    @Test
    void aStandaloneTableDroppedAfterTheSnapshotIsLeftOut() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var reader = TestDataSources.newDataSource().getConnection();
                var other = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(other, "create table " + parent + " (xid xid8 not null) partition by range (xid)");
            TestSql.execute(other, "create table " + parent + "_x1 (like " + parent + ")");
            TestSql.execute(other, "create table " + parent + "_x2 (like " + parent + ")");
            reader.setAutoCommit(false);
            reader.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            TestSql.queryLong(reader, "select 1"); // the snapshot
            TestSql.execute(other, "drop table " + parent + "_x1");

            assertThat(PartitionDdl.standalone(reader, parent, "_x[0-9]+")).containsExactly(parent + "_x2");
            reader.rollback();
        }
    }

    /**
     * A drop of a detached range waits for a reader's lock for 3 s, under a session whose {@code transaction_timeout}
     * (as a role or database default sets it) is 1 s: the drop waits its own lock timeout, the session is not ended.
     */
    @Test
    void aDropWaitsForItsLockPastTheSessionTransactionTimeout() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var c = Pg17.connect(); var reader = Pg17.connect()) {
            TestSql.execute(c, "create table " + parent + " (xid xid8 not null) partition by range (xid)");
            TestSql.execute(c, "create table " + parent + "_a (like " + parent + ")");
            TestSql.execute(c, "create table " + parent + "_b (like " + parent + ")");
            TestSql.execute(c, "set transaction_timeout = '1s'");

            var held = holdFor3s(reader, parent + "_a");
            assertThat(PartitionDdl.dropIfEmptyElseAttach(c, parent, parent + "_a", "0", "100",
                    Duration.ofSeconds(5), Duration.ofSeconds(10))).isTrue();
            held.get(30, TimeUnit.SECONDS);
            held = holdFor3s(reader, parent + "_b");
            assertThat(PartitionDdl.dropUnlessFound(c, parent + "_b", "select 1 from {table} limit 1", null,
                    Duration.ofSeconds(5))).isTrue();
            held.get(30, TimeUnit.SECONDS);
            assertThat(TestSql.queryLong(c, "select count(*) from pg_class where relname like '" + parent + "\\_%'"))
                    .as("both dropped").isZero();
        }
    }

    /** A CHECK and a generated column the owner's migration put on the parent: an attach requires both of a range. */
    @Test
    void aRangeTakesTheParentsChecksAndGeneratedColumns() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create table " + parent + " (xid xid8 not null, payload bytea not null)"
                    + " partition by range (xid)");
            TestSql.execute(c, "alter table " + parent + " add constraint payload_small"
                    + " check (octet_length(payload) < 1000000)");
            TestSql.execute(c, "alter table " + parent + " add column plen int"
                    + " generated always as (octet_length(payload)) stored");

            PartitionDdl.createAndAttach(c, parent, parent + "_a", "0", "100", Duration.ofSeconds(5));
            PartitionDdl.create(c, parent, parent + "_b", Duration.ofSeconds(5), true);
            PartitionDdl.attach(c, parent, parent + "_b", "100", "200", Duration.ofSeconds(5));

            assertThat(PartitionDdl.listRanges(c, parent)).hasSize(2);
        }
    }

    /**
     * An empty range made before the parent gained a CHECK, a generated column or a NOT NULL would fail every attach:
     * {@code create} makes it again.
     */
    @Test
    void anEmptyRangeMadeBeforeTheParentChangedIsMadeAgain() throws Exception {
        var parent = TestSql.uniqueTable();
        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create table " + parent + " (xid xid8 not null, payload bytea, plen int)"
                    + " partition by range (xid)");
            for (var change : new String[] {
                    "add constraint payload_small check (octet_length(payload) < 1000000)",
                    "alter column payload set not null",
                    "drop column plen, add column plen int generated always as (octet_length(payload)) stored"}) {
                var range = parent + "_" + TestSql.uniqueTable();
                PartitionDdl.create(c, parent, range, Duration.ofSeconds(5), true);
                TestSql.execute(c, "alter table " + parent + " " + change);

                PartitionDdl.create(c, parent, range, Duration.ofSeconds(5), true);
                PartitionDdl.attach(c, parent, range, "0", "100", Duration.ofSeconds(5));
                PartitionDdl.detachConcurrentlyAndDrop(c, parent, range, Duration.ofSeconds(5));
            }
        }
    }

    /** Takes a lock on {@code table} in a transaction of {@code reader} and ends it 3 s later. */
    private static CompletableFuture<Void> holdFor3s(Connection reader, String table) throws Exception {
        reader.setAutoCommit(false);
        TestSql.execute(reader, "lock table " + table + " in access share mode");
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(3000);
                reader.commit();
                reader.setAutoCommit(true);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
