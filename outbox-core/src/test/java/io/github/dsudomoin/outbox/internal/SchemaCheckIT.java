package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxSql;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class SchemaCheckIT {

    @Test
    void newerLibraryOnOlderSchemaFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());
        var tables = new ArrayList<>(SchemaObjects.outbox(names).tables());
        var tick = tables.get(1);
        var columns = new ArrayList<>(tick.columns());
        columns.add(new SchemaObjects.Column("lineage", "bigint", false, null, false, 2));
        tables.set(1, new SchemaObjects.Table(tick.name(), tick.since(), tick.partitionKey(), columns,
                tick.constraints(), tick.checks(), tick.indexes()));

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));

            assertThatThrownBy(() -> SchemaCheck.run(c, c, new SchemaObjects(tables), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("column " + names.tick() + ".lineage is missing (schema version 2: apply"
                            + " OutboxSql.outbox)");
        }
    }

    @Test
    void missingWaitingSinceFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "alter table " + names.retry() + " drop column waiting_since");

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(names), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("column " + names.retry() + ".waiting_since is missing (schema version 1:"
                            + " apply OutboxSql.outbox)");
        }
    }

    @Test
    void missingRoutesFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "alter table " + names.instance() + " drop column routes");

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(names), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("column " + names.instance() + ".routes is missing (schema version 1:"
                            + " apply OutboxSql.outbox)");
        }
    }

    @Test
    void missingWaitingIndexFails() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var names = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "drop index " + names.retry() + "_waiting");

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(names), config.table(),
                    "OutboxSql.outbox"))
                    .hasMessageContaining("index " + names.retry() + "_waiting is missing (schema version 1: apply"
                            + " OutboxSql.outbox)");
        }
    }

    @Test
    void oneDatabaseUnderTwoTimeZonesIsOneDatabase() throws Exception {
        var source = TestDataSources.newDataSource();
        var table = "t" + TestSql.uniqueTable();
        try (var utc = source.getConnection(); var moscow = source.getConnection()) {
            TestSql.execute(utc, "set time zone 'UTC'");
            TestSql.execute(moscow, "set time zone 'Europe/Moscow'");

            SchemaCheck.requireSameDatabase(utc, moscow, table);
        }
    }

    @Test
    void aNameResolvedToAnotherTableIsStillRefusedAcrossTimeZones() throws Exception {
        var source = TestDataSources.newDataSource();
        var table = "t" + TestSql.uniqueTable();
        var schema = "s" + TestSql.uniqueTable();
        try (var utc = source.getConnection(); var moscow = source.getConnection()) {
            TestSql.execute(utc, "create schema " + schema);
            TestSql.execute(utc, "create table public." + table + " (id int)");
            TestSql.execute(utc, "create table " + schema + "." + table + " (id int)");
            TestSql.execute(utc, "set time zone 'UTC'");
            TestSql.execute(moscow, "set time zone 'Europe/Moscow'; set search_path = " + schema);

            assertThatThrownBy(() -> SchemaCheck.requireSameDatabase(utc, moscow, table))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reaches another database than the application's");
        }
    }

    @Test
    void anIndexOnOtherColumnsFails() throws Exception {
        assertSchemaProblem((c, n) -> {
            TestSql.execute(c, "drop index " + n.retry() + "_key");
            TestSql.execute(c, "create index " + unqualified(n.retry()) + "_key on " + n.retry() + " (shard, id)");
        }, n -> "index " + n.retry() + "_key is (shard, id), not (shard, topic, key, id)");
    }

    @Test
    void anIndexWithoutItsPredicateFails() throws Exception {
        assertSchemaProblem((c, n) -> {
            TestSql.execute(c, "drop index " + n.retry() + "_waiting");
            TestSql.execute(c, "create index " + unqualified(n.retry()) + "_waiting on " + n.retry()
                    + " (waiting_since)");
        }, n -> "index " + n.retry() + "_waiting is (waiting_since), not (waiting_since) where waiting_since is not"
                + " null");
    }

    @Test
    void anIndexOnAnotherTableFails() throws Exception {
        assertSchemaProblem((c, n) -> {
            TestSql.execute(c, "drop index " + n.parked() + "_due");
            TestSql.execute(c, "create index " + unqualified(n.parked()) + "_due on " + n.retry() + " (shard, id)");
        }, n -> "index " + n.parked() + "_due is on " + n.retry() + ", not on " + n.parked());
    }

    @Test
    void aUniqueIndexFails() throws Exception {
        assertSchemaProblem((c, n) -> {
            TestSql.execute(c, "drop index " + n.retry() + "_key");
            TestSql.execute(c, "create unique index " + unqualified(n.retry()) + "_key on " + n.retry()
                    + " (shard, topic, key, id)");
        }, n -> "index " + n.retry() + "_key is unique");
    }

    @Test
    void aPrimaryKeyOnOtherColumnsFails() throws Exception {
        assertSchemaProblem((c, n) -> {
            TestSql.execute(c, "alter table " + n.instance() + " drop constraint " + unqualified(n.instance())
                    + "_pkey");
            TestSql.execute(c, "alter table " + n.instance() + " add primary key (instance_id, shards)");
        }, n -> n.instance() + " has primary key (instance_id, shards), not (instance_id)");
    }

    @Test
    void noPrimaryKeyFails() throws Exception {
        assertSchemaProblem((c, n) -> TestSql.execute(c, "alter table " + n.redriven() + " drop constraint "
                + unqualified(n.redriven()) + "_pkey"), n -> n.redriven() + " has no primary key");
    }

    @Test
    void storageOptionsAndAQuotedNameAreTheMigrationsToChoose() throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var n = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            TestSql.execute(c, "drop index " + n.retry() + "_key");
            TestSql.execute(c, "create index " + unqualified(n.retry()) + "_key on " + n.retry()
                    + " (\"shard\", \"topic\", \"key\", \"id\") with (fillfactor = 70)");

            SchemaCheck.run(c, c, SchemaObjects.outbox(n), config.table(), "OutboxSql.outbox");
        }
    }

    @FunctionalInterface
    private interface Damage {
        void on(Connection c, TableNames names) throws SQLException;
    }

    /** A schema as {@link OutboxSql#outbox} renders it, passing the check, then with {@code damage} done failing it. */
    private static void assertSchemaProblem(Damage damage, Function<TableNames, String> problem) throws Exception {
        var schema = "s" + TestSql.uniqueTable();
        var config = new TestOutboxConfig().table(schema + ".outbox");
        var n = TableNames.of(config.table());

        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, "create schema " + schema);
            TestSql.execute(c, OutboxSql.outbox(config, null));
            SchemaCheck.run(c, c, SchemaObjects.outbox(n), config.table(), "OutboxSql.outbox");
            damage.on(c, n);

            assertThatThrownBy(() -> SchemaCheck.run(c, c, SchemaObjects.outbox(n), config.table(),
                    "OutboxSql.outbox")).hasMessageContaining(problem.apply(n));
        }
    }

    private static String unqualified(String name) {
        return name.substring(name.indexOf('.') + 1);
    }
}
