package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.queryLong;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.send;
import static io.github.dsudomoin.outbox.spring.TestApps.table;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.OutboxSql;
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.spring.TestApps.OrdersRoute;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** What the auto-configuration wires, and when it refuses to start. */
class OutboxAutoConfigurationIT {

    private static final Duration WAIT = Duration.ofSeconds(10);

    private static OrdersRoute route(ApplicationContext context) {
        return context.getBean(OrdersRoute.class);
    }

    @Test
    void beansPresent() {
        runner().withUserConfiguration(WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(Outbox.class).hasSingleBean(OutboxAdmin.class)
                .hasSingleBean(OutboxLifecycle.class).hasSingleBean(SpringOutboxTransactions.class);
            assertThat(context.getBean(OutboxRelay.class))
                .isSameAs(context.getBean(OutboxLifecycle.class).runtime().relay());
            assertThat(context.getBean(OutboxAdmin.class).status()).isNotNull();

            var id = send(context, "orders");

            await().atMost(WAIT).until(() -> route(context).delivered().contains(id));
        });
        runner("outbox.relay.enabled=false").withUserConfiguration(WithRoute.class).run(context -> {
            var id = send(context, "orders");

            assertThat(context.getBean(OutboxRelay.class).drain()).isOne();
            assertThat(route(context).delivered()).containsExactly(id);
        });
    }

    @Test
    void noHandlerFailsWithMessage() {
        runner().run(context -> assertThat(context).hasFailed().getFailure().rootCause()
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outbox-kafka-spring-boot"));
    }

    @Configuration(proxyBeanMethods = false)
    static class AppConfig {

        @Bean
        TestOutboxConfig appOutboxConfig() {
            return new TestOutboxConfig();
        }
    }

    @Test
    void appOutboxConfigBeanReplacesProperties() {
        runner().withUserConfiguration(WithRoute.class, AppConfig.class).run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(OutboxConfig.class)
                .doesNotHaveBean(OutboxProperties.class);
            var table = context.getBean(TestOutboxConfig.class).table();

            var id = send(context, "orders");

            assertThat(queryLong("select count(*) from " + table + " where message_id = '" + id + "'")).isOne();
        });
    }

    @Test
    void enabledFalseKeepsInbox() {
        runner("outbox.enabled=false").run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(Outbox.class).doesNotHaveBean(OutboxLifecycle.class)
                .doesNotHaveBean(OutboxAdmin.class);
            var inbox = context.getBean(Inbox.class);
            var tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            var id = UuidV7.next();

            Boolean first = tx.execute(status -> inbox.tryAccept(id));
            Boolean again = tx.execute(status -> inbox.tryAccept(id));

            assertThat(first).isTrue();
            assertThat(again).isFalse();
        });
    }

    /** A bean that uses the inbox. */
    record InboxUser(Inbox inbox) {}

    @Configuration(proxyBeanMethods = false)
    static class WithInboxUser {

        @Bean
        InboxUser inboxUser(Inbox inbox) {
            return new InboxUser(inbox);
        }
    }

    @Test
    void inboxTableOnlyWhenInjected() {
        runner().withUserConfiguration(WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(regclass(context.getBean(OutboxConfig.class).inbox().table())).isZero();
        });
        runner().withUserConfiguration(WithRoute.class, WithInboxUser.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(regclass(context.getBean(OutboxConfig.class).inbox().table())).isOne();
        });
    }

    private static long regclass(String table) throws SQLException {
        return queryLong("select count(to_regclass('" + table + "'))");
    }

    /** The owner role's DataSource, beside Boot's (the application role's), for the outbox's DDL. */
    @Configuration(proxyBeanMethods = false)
    static class WithOwner {

        static String owner;

        @Bean(defaultCandidate = false)
        @OutboxOwner
        DataSource ownerDataSource() {
            return as(owner);
        }
    }

    private static PGSimpleDataSource as(String role) {
        var ds = (PGSimpleDataSource) TestDataSources.newDataSource();
        ds.setUser(role);
        ds.setPassword("pw");
        return ds;
    }

    @Test
    void ownerDataSourceBySelfQualifier() throws SQLException {
        var schema = "s" + TestSql.uniqueTable();
        var owner = "o" + TestSql.uniqueTable();
        var app = "a" + TestSql.uniqueTable();
        TestApps.execute("create role " + owner + " login password 'pw'");
        TestApps.execute("create role " + app + " login password 'pw'");
        TestApps.execute("grant pg_read_all_stats to " + owner); // as README recommends
        TestApps.execute("create schema " + schema + " authorization " + owner);
        TestApps.execute("grant usage on schema " + schema + " to " + app);
        var config = new TestOutboxConfig().table(schema + ".outbox").inboxTable(schema + ".inbox");
        try (var c = as(owner).getConnection()) {
            TestSql.execute(c, OutboxSql.outbox(config, app));
            TestSql.execute(c, OutboxSql.inbox(config, app));
        }
        WithOwner.owner = owner;

        runner("spring.datasource.username=" + app, "spring.datasource.password=pw",
            "outbox.table=" + config.table(), "outbox.inbox.table=" + config.inbox().table(),
            "outbox.schema.bootstrap=false")
            .withUserConfiguration(WithRoute.class, WithOwner.class, WithInboxUser.class).run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBeansOfType(DataSource.class)).hasSize(2);
                assertThat(new JdbcTemplate(context.getBean(DataSource.class))
                    .queryForObject("select current_user", String.class)).as("Boot's DataSource").isEqualTo(app);
                assertThat(queryLong("select count(*) from pg_class r join pg_namespace n on n.oid = r.relnamespace"
                    + " where n.nspname = '" + schema + "' and r.relispartition")).as("ranges").isPositive();
                assertThat(queryLong("select count(*) from pg_class r join pg_namespace n on n.oid = r.relnamespace"
                    + " where n.nspname = '" + schema + "' and r.relispartition and pg_get_userbyid(r.relowner) <> '"
                    + owner + "'")).as("ranges made by another role than the owner").isZero();

                var id = send(context, "orders");

                await().atMost(WAIT).until(() -> route(context).delivered().contains(id));
            });
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoDataSources {

        @Bean
        @Primary
        DataSource first() {
            return TestDataSources.newDataSource();
        }

        /** Nothing listens there: the outbox must not use it. */
        @Bean
        DataSource second() {
            return new DriverManagerDataSource("jdbc:postgresql://127.0.0.1:1/none");
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TwoDataSourcesWithoutPrimary {

        @Bean
        DataSource first() {
            return TestDataSources.newDataSource();
        }

        @Bean
        DataSource second() {
            return TestDataSources.newDataSource();
        }
    }

    @Test
    void twoDataSourcesUsePrimary() {
        runner().withUserConfiguration(WithRoute.class, TwoDataSources.class).run(context -> {
            assertThat(context).hasNotFailed();
            var id = send(context, "orders");

            await().atMost(WAIT).until(() -> route(context).delivered().contains(id));
            assertThat(queryLong("select count(*) from " + table(context))).isOne();
        });
    }

    @Test
    void twoDataSourcesWithoutPrimaryFail() {
        runner().withUserConfiguration(WithRoute.class, TwoDataSourcesWithoutPrimary.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasMessageContaining("outboxTransactions")
                .rootCause().isInstanceOf(NoUniqueBeanDefinitionException.class)
                .hasMessageContaining("first").hasMessageContaining("second");
        });
    }

    @Test
    void globalLazyInitStillStarts() {
        runner().withUserConfiguration(WithRoute.class)
            .withBean(LazyInitializationBeanFactoryPostProcessor.class).run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(OutboxLifecycle.class).isRunning()).isTrue();

                var id = send(context, "orders");

                await().atMost(WAIT).until(() -> route(context).delivered().contains(id));
            });
    }
}
