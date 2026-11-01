package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.queryLong;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.send;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.OutboxSql;
import io.github.dsudomoin.outbox.spring.OutboxAutoConfigurationIT.WithInboxUser;
import io.github.dsudomoin.outbox.spring.OutboxAutoConfigurationIT.WithOwner;
import io.github.dsudomoin.outbox.spring.TestApps.OrdersRoute;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The schema from the application's Flyway or Liquibase migrations, run as an owner role, with Boot's own DataSource
 * logged in as a DML-only role and an {@link OutboxOwner} DataSource for the partitions. Every migration is what
 * {@link OutboxSql} renders; the one exception changes one default on purpose.
 */
class MigrationsIT {

    private static final Duration WAIT = Duration.ofSeconds(10);
    /** With JPA in the app Flyway runs in time even without the outbox's declared order: it would hide its loss. */
    private static final FilteredClassLoader NO_JPA = new FilteredClassLoader("jakarta.persistence",
        "org.springframework.data.jpa");

    private final String schema = "s" + TestSql.uniqueTable();
    private final String owner = "o" + TestSql.uniqueTable();
    private final String app = "a" + TestSql.uniqueTable();
    private final TestOutboxConfig config = new TestOutboxConfig().table(schema + ".outbox")
        .inboxTable(schema + ".inbox");

    @TempDir
    private Path migrations;

    MigrationsIT() throws SQLException {
        TestApps.execute("create role " + owner + " login password 'pw'");
        TestApps.execute("create role " + app + " login password 'pw'");
        TestApps.execute("grant pg_read_all_stats to " + owner); // as README recommends
        TestApps.execute("create schema " + schema + " authorization " + owner);
        TestApps.execute("grant usage on schema " + schema + " to " + app);
        WithOwner.owner = owner;
    }

    @Test
    void flywayMigrationThenStart() throws IOException {
        flyway().run(this::startedOnTheMigratedSchema);
    }

    @Test
    void liquibaseMigrationThenStart() throws IOException {
        Files.writeString(migrations.resolve("outbox.sql"), OutboxSql.outbox(config, app));
        Files.writeString(migrations.resolve("changelog.yaml"), """
            databaseChangeLog:
              - changeSet:
                  id: outbox
                  author: app
                  changes:
                    - sqlFile:
                        path: outbox.sql
                        relativeToChangelogFile: true
            """);

        app("spring.liquibase.enabled=true", "spring.liquibase.user=" + owner, "spring.liquibase.password=pw",
            "spring.liquibase.change-log=" + migrations.resolve("changelog.yaml").toUri(),
            "spring.liquibase.default-schema=" + schema, "spring.liquibase.analytics-enabled=false")
            .run(context -> {
                startedOnTheMigratedSchema(context);
                assertThat(queryLong("select count(to_regclass('" + schema + ".databasechangelog'))"))
                    .as("Liquibase ran").isOne();
            });
    }

    private void startedOnTheMigratedSchema(AssertableApplicationContext context) throws SQLException {
        assertThat(context).hasNotFailed();
        assertThat(new JdbcTemplate(context.getBean(DataSource.class))
            .queryForObject("select current_user", String.class)).as("Boot's DataSource").isEqualTo(app);
        assertThat(queryLong("select count(*) from pg_class where oid = to_regclass('" + config.table()
            + "') and pg_get_userbyid(relowner) = '" + owner + "'")).as("base table owned by the owner").isOne();
        assertThat(queryLong(partitions(""))).as("ranges").isPositive();
        assertThat(queryLong(partitions(" and pg_get_userbyid(r.relowner) <> '" + owner + "'")))
            .as("ranges made by another role than the owner").isZero();

        var id = send(context, "orders");

        await().atMost(WAIT).until(() -> context.getBean(OrdersRoute.class).delivered().contains(id));
    }

    private String partitions(String and) {
        return "select count(*) from pg_class r join pg_namespace n on n.oid = r.relnamespace where n.nspname = '"
            + schema + "' and r.relispartition" + and;
    }

    @Test
    void withoutMigrationStartFails() {
        app().run(context -> assertThat(context).hasFailed().getFailure().rootCause()
            .hasMessageContaining("outbox schema check of " + config.table() + " failed")
            .hasMessageContaining("table " + config.table() + " is missing (schema version 1: apply OutboxSql.outbox)")
            .hasMessageContaining("table " + config.table() + "_tick is missing"));
    }

    @Test
    void ownerWithBootstrapTrueFails() {
        app("outbox.schema.bootstrap=true").run(context -> assertThat(context).hasFailed().getFailure().rootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("outbox.schema.bootstrap must be false when an owner connection source is set: objects it"
                + " created would leave the application role without rights on them"));
    }

    /**
     * The same app and schema first without the migration, where the check fails, then with it: had the check run
     * before Flyway, the second start would fail as the first did.
     */
    @Test
    void migrationRunsBeforeRuntimeCheck() throws IOException {
        var flyway = flyway().withClassLoader(NO_JPA);

        flyway.withPropertyValues("spring.flyway.enabled=false").run(context -> assertThat(context).hasFailed()
            .getFailure().rootCause().hasMessageContaining("schema check of " + config.table() + " failed"));

        flyway.run(this::startedOnTheMigratedSchema);
    }

    /** As above for the inbox, which checks as soon as a bean injects it, long before the lifecycle starts. */
    @Test
    void migrationRunsBeforeInboxCheck() throws IOException {
        Files.writeString(migrations.resolve("V2__inbox.sql"), OutboxSql.inbox(config, app));
        var flyway = flyway().withClassLoader(NO_JPA).withUserConfiguration(WithInboxUser.class);

        flyway.withPropertyValues("spring.flyway.enabled=false").run(context -> assertThat(context).hasFailed()
            .getFailure().rootCause().hasMessageContaining("schema check of " + config.inbox().table() + " failed"));

        flyway.run(this::startedOnTheMigratedSchema);
    }

    /** Under global lazy init nothing but the lifecycle bean's declared dependency makes Flyway run in time. */
    @Test
    void migrationRunsBeforeRuntimeCheckUnderLazyInit() throws IOException {
        flyway().withClassLoader(NO_JPA).withBean(LazyInitializationBeanFactoryPostProcessor.class)
            .run(this::startedOnTheMigratedSchema);
    }

    @Test
    void nonCanonicalDefaultFailsStart() throws IOException {
        var canonical = OutboxSql.outbox(config, app);
        var changed = canonical.replaceFirst("(failed_at [^,\\n]*) default now\\(\\)", "$1 default CURRENT_TIMESTAMP");
        assertThat(changed).isNotEqualTo(canonical);
        Files.writeString(migrations.resolve("V1__outbox.sql"), changed);

        flywayApp().run(context -> assertThat(context).hasFailed().getFailure().rootCause()
            .hasMessageContaining("column " + config.table() + "_dead.failed_at has default CURRENT_TIMESTAMP,"
                + " not now()"));
    }

    private ApplicationContextRunner flyway() throws IOException {
        Files.writeString(migrations.resolve("V1__outbox.sql"), OutboxSql.outbox(config, app));
        return flywayApp();
    }

    /** Flyway's history table goes to the owner's schema: the role may not create in {@code public}. */
    private ApplicationContextRunner flywayApp() {
        return app("spring.flyway.enabled=true", "spring.flyway.user=" + owner, "spring.flyway.password=pw",
            "spring.flyway.locations=filesystem:" + migrations, "spring.flyway.default-schema=" + schema);
    }

    /** The application role's Boot DataSource, the owner's beside it, no bootstrap, no migration tool yet. */
    private ApplicationContextRunner app(String... more) {
        return runner(Stream.concat(Stream.of("spring.datasource.username=" + app, "spring.datasource.password=pw",
            "outbox.table=" + config.table(), "outbox.inbox.table=" + config.inbox().table(),
            "outbox.schema.bootstrap=false"), Stream.of(more)).toArray(String[]::new))
            .withUserConfiguration(WithRoute.class, WithOwner.class);
    }
}
