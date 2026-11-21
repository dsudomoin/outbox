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
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.spring.TestApps.OrdersRoute;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
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

    @Test
    void appOutboxConfigBeanReplacesProperties() {
        runner().withUserConfiguration(WithRoute.class)
            .withBean(TestOutboxConfig.class, TestOutboxConfig::new).run(context -> {
                assertThat(context).hasNotFailed().hasSingleBean(OutboxConfig.class)
                    .doesNotHaveBean(OutboxProperties.class);
                var table = table(context);

                var id = send(context, "orders");

                assertThat(queryLong("select count(*) from " + table + " where message_id = '" + id + "'")).isOne();
            });
    }

    /** The switch decides as the binder binds it, whatever the spelling. */
    @ParameterizedTest
    @ValueSource(strings = {"off", "FALSE"})
    void enabledOffInAnySpellingKeepsInbox(String off) {
        runner("outbox.enabled=" + off).run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(Outbox.class).doesNotHaveBean(OutboxLifecycle.class)
                .doesNotHaveBean(OutboxAdmin.class).hasSingleBean(Inbox.class);
            assertThat(context.getBean(OutboxProperties.class).enabled()).isFalse();
            var inbox = context.getBean(Inbox.class);
            var tx = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            var id = UuidV7.next();

            Boolean first = tx.execute(status -> inbox.tryAccept(id));
            Boolean again = tx.execute(status -> inbox.tryAccept(id));

            assertThat(first).isTrue();
            assertThat(again).isFalse();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"on", "TRUE"})
    void enabledOnInAnySpellingStartsTheRuntime(String on) {
        runner("outbox.enabled=" + on).withUserConfiguration(WithRoute.class).run(context ->
            assertThat(context).hasNotFailed().hasSingleBean(Outbox.class).hasSingleBean(OutboxLifecycle.class));
    }

    @Test
    void enabledUnparseableFailsTheStart() {
        runner("outbox.enabled=flase").withUserConfiguration(WithRoute.class).run(context ->
            assertThat(context).hasFailed().getFailure().rootCause().hasMessageContaining("flase"));
    }

    /** A bean that uses the inbox. */
    record InboxUser(Inbox inbox) {}

    @Test
    void inboxTableOnlyWhenInjected() {
        runner().withUserConfiguration(WithRoute.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(regclass(context.getBean(OutboxConfig.class).inbox().table())).isZero();
        });
        runner().withUserConfiguration(WithRoute.class).withBean(InboxUser.class).run(context -> {
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
            var ds = TestDataSources.newDataSource();
            ds.setUser(owner);
            ds.setPassword("pw");
            return ds;
        }
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
